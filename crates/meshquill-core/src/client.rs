use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use chrono::Utc;
use tokio::sync::broadcast;
use tokio::time::timeout;
use zeroize::Zeroizing;

use crate::domain::{
    Ack, AdvertPath, AutoAddConfig, BatteryInfo, BinaryResponse, ChannelInfo, CommandTracking,
    Contact, ContactRoute, ContactSnapshot, ContactUri, CustomVariables, DefaultFloodScope,
    DeviceInfo, DeviceStats, Event, FloodScope, FrequencyRange, LoginSession, Message, Path,
    PathDiscovery, PrivateKeyMaterial, RadioParams, RemoteStatus, SelfInfo, Signature, StatsType,
    TelemetryResponse, TuningParams,
};
use crate::error::{CoreError, TransportError};
use crate::protocol::{Command, MAX_INNER_PAYLOAD, Packet};
use crate::transport::{ReadyRead, ReconnectableTransport, Transport};

/// Default request timeout used when waiting for companion replies.
pub const CLIENT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
/// Default outbound event buffer size.
pub const CLIENT_EVENT_CAPACITY: usize = 256;
/// Largest caller-controlled operation timeout accepted by the core client (24 hours).
pub const MAX_OPERATION_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);

const CLIENT_COMPLETED_ACK_CAPACITY: usize = 256;
const CLIENT_PENDING_ACK_CAPACITY: usize = 256;
const CLIENT_SYNC_READY_DRAIN_CAPACITY: usize = 256;
const MAX_REMOTE_RESPONSE_TIMEOUT: Duration = Duration::from_secs(60);
const SIGNING_CHUNK_BYTES: usize = 120;

enum PacketWait<T> {
    Ready(T),
    Continue(Packet),
    Fail(CoreError),
}

enum ReadySync {
    Message(Message),
    NoMoreMessages,
    Error,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SyncState {
    Idle,
    AwaitingResponse,
}

struct ContactCollection {
    contacts: Vec<Contact>,
    lastmod: Option<u32>,
}

/// Core client state machine for companion operations.
pub struct Client<T: Transport> {
    transport: T,
    event_tx: broadcast::Sender<Event>,
    request_timeout: Duration,
    connected: bool,
    pending_acks: HashMap<[u8; 4], CommandTracking>,
    pending_ack_order: VecDeque<[u8; 4]>,
    completed_acks: HashMap<[u8; 4], Ack>,
    completed_ack_order: VecDeque<[u8; 4]>,
    next_message_observation_id: u64,
    sync_state: SyncState,
    deferred_sync: Option<ReadySync>,
}

impl<T> Client<T>
where
    T: Transport + Send,
{
    /// Constructs a client with default timeouts and capacity.
    pub fn new(transport: T) -> Self {
        Self::with_valid_timeout(transport, CLIENT_REQUEST_TIMEOUT)
    }

    /// Constructs a client with a custom request timeout.
    ///
    /// # Errors
    /// Returns [`CoreError::InvalidArgument`] when the timeout is zero or exceeds 24 hours.
    pub fn with_timeout(transport: T, request_timeout: Duration) -> Result<Self, CoreError> {
        validate_operation_timeout(request_timeout)?;
        Ok(Self::with_valid_timeout(transport, request_timeout))
    }

    fn with_valid_timeout(transport: T, request_timeout: Duration) -> Self {
        let (event_tx, _) = broadcast::channel(CLIENT_EVENT_CAPACITY);
        Self {
            transport,
            event_tx,
            request_timeout,
            connected: false,
            pending_acks: HashMap::new(),
            pending_ack_order: VecDeque::new(),
            completed_acks: HashMap::new(),
            completed_ack_order: VecDeque::new(),
            next_message_observation_id: 1,
            sync_state: SyncState::Idle,
            deferred_sync: None,
        }
    }

    /// Returns a bounded event receiver.
    ///
    /// A slow receiver gets [`broadcast::error::RecvError::Lagged`] when the fixed-capacity event
    /// buffer overwrites unseen events. After reporting the lag, the receiver resumes at the
    /// oldest event still retained.
    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.event_tx.subscribe()
    }

    /// Returns whether transport is currently connected.
    pub fn is_connected(&self) -> bool {
        self.connected
    }

    /// Returns the list of currently tracked ACK codes.
    ///
    /// Tracking is FIFO-bounded. If more than 256 sends remain unacknowledged, the oldest tracker
    /// is evicted; this bounds memory without retrying or replaying any command.
    pub fn pending_ack_codes(&self) -> Vec<[u8; 4]> {
        self.pending_ack_order.iter().copied().collect()
    }

    /// Opens the transport, initializes the companion session, and returns self metadata.
    ///
    /// # Errors
    /// Returns `CoreError::Transport` if transport connection fails, `CoreError::ProtocolInvariant`
    /// if `APP_START` does not complete, or `CoreError::Disconnected` if transport closes
    /// unexpectedly.
    pub async fn connect(&mut self) -> Result<SelfInfo, CoreError> {
        self.clear_tracking();
        self.clear_sync_tracking();
        self.transport.connect().await?;
        self.connected = true;
        match self.init().await {
            Ok(info) => {
                let _ = self.event_tx.send(Event::Connected);
                Ok(info)
            }
            Err(error) => {
                let _ = self.transport.disconnect().await;
                self.mark_disconnected();
                Err(error)
            }
        }
    }

    /// Sends `APP_START` and consumes its `SELF_INFO` response.
    ///
    /// # Errors
    /// Returns `CoreError::Disconnected` when not connected, and `CoreError::ProtocolInvariant` for
    /// session initialization failures.
    pub async fn init(&mut self) -> Result<SelfInfo, CoreError> {
        self.ensure_connected()?;
        self.send_only(Command::app_start()).await?;
        loop {
            let packet = self.read_next_packet_with_timeout().await?;
            match packet {
                Packet::SelfInfo(info) => {
                    self.publish_event(&Packet::SelfInfo(info.clone()));
                    return Ok(info);
                }
                Packet::Error(_) => {
                    return Err(CoreError::ProtocolInvariant("APP_START returned an error"));
                }
                other => {
                    self.publish_event(&other);
                    self.update_tracking(&other);
                }
            }
        }
    }

    /// Closes the active transport.
    ///
    /// # Errors
    /// Returns `CoreError::Transport` if transport shutdown fails.
    pub async fn disconnect(&mut self) -> Result<(), CoreError> {
        let result = self.transport.disconnect().await;
        self.mark_disconnected();
        result.map_err(CoreError::Transport)
    }

    /// Attempts a reconnect for reconnectable transports.
    ///
    /// # Errors
    /// Returns `CoreError::Transport` when reconnect fails and `CoreError::ProtocolInvariant` when
    /// session re-initialization fails.
    pub async fn reconnect(&mut self) -> Result<SelfInfo, CoreError>
    where
        T: ReconnectableTransport,
    {
        self.mark_disconnected();
        self.transport.reconnect().await?;
        self.connected = true;
        match self.init().await {
            Ok(info) => {
                let _ = self.event_tx.send(Event::Connected);
                Ok(info)
            }
            Err(error) => {
                let _ = self.transport.disconnect().await;
                self.mark_disconnected();
                Err(error)
            }
        }
    }

    /// Reads and publishes one unsolicited companion packet while no request is active.
    ///
    /// The packet is also applied to ACK tracking. Packets without a public event representation
    /// return `Ok(None)` after being consumed.
    ///
    /// # Cancel safety
    ///
    /// This method is cancellation safe. If its future is dropped before completion, no logical
    /// packet is consumed, as required by the [`Transport::read`] contract. Once a read completes,
    /// parsing, state updates, and publication happen without another suspension point.
    ///
    /// # Errors
    ///
    /// Returns `CoreError::Disconnected` after a clean peer close, `CoreError::Transport` for
    /// transport failures (including idle timeouts), or `CoreError::Parse` for malformed packets.
    /// Terminal connection I/O errors transition the client to disconnected state; recoverable
    /// errors such as `InvalidData` leave it connected so framed transports can resynchronize.
    pub async fn next_event(&mut self) -> Result<Option<Event>, CoreError> {
        self.ensure_connected()?;
        let packet = self.read_next_packet().await?;
        self.retain_sync_response(&packet);
        self.update_tracking(&packet);
        let Some(event) = packet.into_event() else {
            return Ok(None);
        };
        self.publish_domain_event(&event);
        Ok(Some(event))
    }

    /// Lists contacts from firmware.
    ///
    /// This compatibility helper discards the response-level last-modified
    /// marker. Call [`Self::list_contacts_snapshot`] when that marker is needed.
    ///
    /// # Errors
    /// Returns `CoreError::Disconnected` when transport is unavailable, `CoreError::Timeout` when no
    /// complete response arrives in time, `CoreError::ProtocolInvariant` for firmware error packets, or
    /// `CoreError::Parse` when packet decoding fails.
    pub async fn list_contacts(&mut self, lastmod: Option<u32>) -> Result<Vec<Contact>, CoreError> {
        Ok(self.collect_contacts(lastmod).await?.contacts)
    }

    /// Lists contacts and preserves the firmware snapshot sequence marker.
    ///
    /// # Errors
    /// Returns `CoreError::Disconnected` when transport is unavailable, `CoreError::Timeout` when no
    /// complete response arrives in time, `CoreError::ProtocolInvariant` when the firmware does not
    /// terminate the response with a marker or returns an error packet, or `CoreError::Parse` when
    /// packet decoding fails.
    pub async fn list_contacts_snapshot(
        &mut self,
        lastmod: Option<u32>,
    ) -> Result<ContactSnapshot, CoreError> {
        let collection = self.collect_contacts(lastmod).await?;
        let Some(lastmod) = collection.lastmod else {
            return Err(CoreError::ProtocolInvariant(
                "contact snapshot ended without a last-modified marker",
            ));
        };
        Ok(ContactSnapshot {
            contacts: collection.contacts,
            lastmod,
        })
    }

    async fn collect_contacts(
        &mut self,
        lastmod: Option<u32>,
    ) -> Result<ContactCollection, CoreError> {
        self.ensure_connected()?;
        self.send_only(Command::get_contacts(lastmod)).await?;

        let mut contacts = Vec::new();
        loop {
            let packet = self.read_next_packet_with_timeout().await?;
            match packet {
                Packet::Contact(contact) => contacts.push(contact),
                Packet::ContactEnd { lastmod } => {
                    return Ok(ContactCollection {
                        contacts,
                        lastmod: Some(lastmod),
                    });
                }
                Packet::NoMoreMsgs => {
                    return Ok(ContactCollection {
                        contacts,
                        lastmod: None,
                    });
                }
                Packet::Error(_code) => {
                    return Err(CoreError::ProtocolInvariant(
                        "contact request returned an error",
                    ));
                }
                other => {
                    self.publish_event(&other);
                    self.update_tracking(&other);
                }
            }
        }
    }

    /// Queries self/device metadata.
    ///
    /// # Errors
    /// Returns `CoreError::Disconnected` when transport is unavailable, `CoreError::Timeout` when no
    /// response arrives in time, `CoreError::ProtocolInvariant` for firmware error packets, or
    /// `CoreError::Parse` when packet decoding fails.
    pub async fn query_device_info(&mut self) -> Result<DeviceInfo, CoreError> {
        self.ensure_connected()?;
        self.send_only(Command::device_query()).await?;

        loop {
            let packet = self.read_next_packet_with_timeout().await?;
            match packet {
                Packet::DeviceInfo(info) => return Ok(info),
                Packet::Error(_code) => {
                    return Err(CoreError::ProtocolInvariant(
                        "device query returned an error",
                    ));
                }
                other => {
                    self.publish_event(&other);
                    self.update_tracking(&other);
                }
            }
        }
    }

    /// Queries the companion clock.
    ///
    /// # Errors
    ///
    /// Returns transport, timeout, parse, or protocol errors from the companion operation.
    pub async fn get_time(&mut self) -> Result<u32, CoreError> {
        self.send_command_expect(Command::get_time(), "time query", |packet| match packet {
            Packet::CurrentTime(value) => Ok(value),
            other => Err(other),
        })
        .await
    }

    /// Updates the companion clock without retry or replay.
    ///
    /// # Errors
    ///
    /// Returns transport, timeout, parse, or protocol errors from the companion operation.
    pub async fn set_time(&mut self, value: u32) -> Result<(), CoreError> {
        self.send_command_expect_ok(Command::set_time(value)).await
    }

    /// Queries battery voltage and optional storage metrics.
    ///
    /// # Errors
    ///
    /// Returns transport, timeout, parse, or protocol errors from the companion operation.
    pub async fn get_battery(&mut self) -> Result<BatteryInfo, CoreError> {
        self.send_command_expect(
            Command::get_battery(),
            "battery query",
            |packet| match packet {
                Packet::Battery(info) => Ok(info),
                other => Err(other),
            },
        )
        .await
    }

    /// Sends this device's advert, optionally as a flood.
    ///
    /// # Errors
    ///
    /// Returns transport, timeout, parse, or protocol errors from the companion operation.
    pub async fn send_self_advert(&mut self, flood: bool) -> Result<(), CoreError> {
        self.send_command_expect_ok(Command::send_self_advert(flood))
            .await
    }

    /// Updates the advertised device name without retry or replay.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or protocol errors.
    pub async fn set_advert_name(&mut self, name: &str) -> Result<(), CoreError> {
        self.send_command_expect_ok(Command::set_advert_name(name)?)
            .await
    }

    /// Updates advertised coordinates without retry or replay.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or protocol errors.
    pub async fn set_coordinates(
        &mut self,
        latitude: f64,
        longitude: f64,
    ) -> Result<(), CoreError> {
        self.send_command_expect_ok(Command::set_coordinates(latitude, longitude)?)
            .await
    }

    /// Updates radio transmit power without retry or replay.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or protocol errors.
    pub async fn set_tx_power(&mut self, power_dbm: i8) -> Result<(), CoreError> {
        self.send_command_expect_ok(Command::set_tx_power(power_dbm)?)
            .await
    }

    /// Updates radio parameters without retry or replay.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or protocol errors.
    pub async fn set_radio_params(&mut self, params: &RadioParams) -> Result<(), CoreError> {
        self.send_command_expect_ok(Command::set_radio_params(params)?)
            .await
    }

    /// Updates packet timing parameters without retry or replay.
    ///
    /// # Errors
    ///
    /// Returns transport, timeout, parse, or protocol errors from the companion operation.
    pub async fn set_tuning(&mut self, params: TuningParams) -> Result<(), CoreError> {
        self.send_command_expect_ok(Command::set_tuning(params))
            .await
    }

    /// Queries packet timing parameters.
    ///
    /// # Errors
    ///
    /// Returns transport, timeout, parse, or protocol errors from the companion operation.
    pub async fn get_tuning(&mut self) -> Result<TuningParams, CoreError> {
        self.send_command_expect(
            Command::get_tuning(),
            "tuning query",
            |packet| match packet {
                Packet::TuningParams(params) => Ok(params),
                other => Err(other),
            },
        )
        .await
    }

    /// Resets the stored route for one exact public key.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or protocol errors.
    pub async fn reset_path(&mut self, public_key: &[u8]) -> Result<(), CoreError> {
        self.send_command_expect_ok(Command::reset_path(public_key)?)
            .await
    }

    /// Replaces one contact's mutable metadata from a freshly queried full record.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or firmware rejection errors.
    pub async fn update_contact(&mut self, contact: &Contact) -> Result<(), CoreError> {
        self.send_command_expect_ok(Command::update_contact(contact)?)
            .await
    }

    /// Shares one contact over zero hop.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or protocol errors.
    pub async fn share_contact(&mut self, public_key: &[u8]) -> Result<(), CoreError> {
        self.send_command_expect_ok(Command::share_contact(public_key)?)
            .await
    }

    /// Exports this device's contact card or a stored contact's card.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or protocol errors.
    pub async fn export_contact(
        &mut self,
        public_key: Option<&[u8]>,
    ) -> Result<ContactUri, CoreError> {
        self.send_command_expect(
            Command::export_contact(public_key)?,
            "contact export",
            |packet| match packet {
                Packet::ContactUri(uri) => Ok(uri),
                other => Err(other),
            },
        )
        .await
    }

    /// Imports a validated contact card without retry or replay.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or protocol errors.
    pub async fn import_contact(&mut self, card: &[u8]) -> Result<(), CoreError> {
        self.send_command_expect_ok(Command::import_contact(card)?)
            .await
    }

    /// Queries one contact by its exact public key.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or protocol errors.
    pub async fn get_contact(&mut self, public_key: &[u8]) -> Result<Contact, CoreError> {
        self.send_command_expect(
            Command::get_contact(public_key)?,
            "contact query",
            |packet| match packet {
                Packet::Contact(contact) => Ok(contact),
                other => Err(other),
            },
        )
        .await
    }

    /// Queries the latest advert path for one exact public key.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or protocol errors.
    pub async fn get_advert_path(&mut self, public_key: &[u8]) -> Result<AdvertPath, CoreError> {
        self.send_command_expect(
            Command::get_advert_path(public_key)?,
            "advert path query",
            |packet| match packet {
                Packet::AdvertPath(path) => Ok(path),
                other => Err(other),
            },
        )
        .await
    }

    /// Updates automatic contact-addition configuration without retry or replay.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or protocol errors.
    pub async fn set_auto_add_config(&mut self, config: AutoAddConfig) -> Result<(), CoreError> {
        self.send_command_expect_ok(Command::set_auto_add_config(config)?)
            .await
    }

    /// Queries automatic contact-addition configuration.
    ///
    /// # Errors
    ///
    /// Returns transport, timeout, parse, or protocol errors from the companion operation.
    pub async fn get_auto_add_config(&mut self) -> Result<AutoAddConfig, CoreError> {
        self.send_command_expect(
            Command::get_auto_add_config(),
            "auto-add config query",
            |packet| match packet {
                Packet::AutoAddConfig(config) => Ok(config),
                other => Err(other),
            },
        )
        .await
    }

    /// Queries custom device variables.
    ///
    /// # Errors
    ///
    /// Returns transport, timeout, parse, or protocol errors from the companion operation.
    pub async fn get_custom_vars(&mut self) -> Result<CustomVariables, CoreError> {
        self.send_command_expect(
            Command::get_custom_vars(),
            "custom-variable query",
            |packet| match packet {
                Packet::CustomVariables(vars) => Ok(vars),
                other => Err(other),
            },
        )
        .await
    }

    /// Updates one strict custom variable without retry or replay.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or protocol errors.
    pub async fn set_custom_var(&mut self, key: &str, value: &str) -> Result<(), CoreError> {
        self.send_command_expect_ok(Command::set_custom_var(key, value)?)
            .await
    }

    /// Queries one known statistics family.
    ///
    /// # Errors
    ///
    /// Returns transport, timeout, parse, or protocol errors from the companion operation.
    pub async fn get_stats(&mut self, stats_type: StatsType) -> Result<DeviceStats, CoreError> {
        self.send_command_expect(
            Command::get_stats(stats_type),
            "statistics query",
            |packet| match packet {
                Packet::DeviceStats(stats) => Ok(stats),
                other => Err(other),
            },
        )
        .await
    }

    /// Queries allowed client-repeat frequency ranges.
    ///
    /// # Errors
    ///
    /// Returns transport, timeout, parse, or protocol errors from the companion operation.
    pub async fn get_allowed_repeat_frequencies(
        &mut self,
    ) -> Result<Vec<FrequencyRange>, CoreError> {
        self.send_command_expect(
            Command::get_allowed_repeat_frequencies(),
            "allowed repeat-frequency query",
            |packet| match packet {
                Packet::AllowedRepeatFrequencies(ranges) => Ok(ranges),
                other => Err(other),
            },
        )
        .await
    }

    /// Updates the path-hash mode without retry or replay.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or protocol errors.
    pub async fn set_path_hash_mode(&mut self, mode: u8) -> Result<(), CoreError> {
        self.send_command_expect_ok(Command::set_path_hash_mode(mode)?)
            .await
    }

    /// Queries the path-hash mode through device metadata.
    ///
    /// # Errors
    ///
    /// Returns transport, timeout, parse, or protocol errors, or a protocol invariant error when
    /// older firmware omits the field.
    pub async fn get_path_hash_mode(&mut self) -> Result<u8, CoreError> {
        self.query_device_info()
            .await?
            .path_hash_mode
            .ok_or(CoreError::ProtocolInvariant(
                "device info does not include path hash mode",
            ))
    }

    /// Selects the default, unscoped, or exact-key flood scope without retry or replay.
    ///
    /// # Errors
    ///
    /// Returns transport, timeout, parse, or protocol errors from the companion operation.
    pub async fn set_flood_scope(&mut self, scope: &FloodScope) -> Result<(), CoreError> {
        self.send_command_expect_ok(Command::set_flood_scope(scope))
            .await
    }

    /// Configures the named default flood scope with the caller's exact key bytes.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or protocol errors.
    pub async fn set_default_flood_scope(
        &mut self,
        name: &str,
        key: [u8; 16],
    ) -> Result<(), CoreError> {
        self.send_command_expect_ok(Command::set_default_flood_scope(name, key)?)
            .await
    }

    /// Clears the configured default flood scope without retry or replay.
    ///
    /// # Errors
    ///
    /// Returns transport, timeout, parse, or protocol errors from the companion operation.
    pub async fn clear_default_flood_scope(&mut self) -> Result<(), CoreError> {
        self.send_command_expect_ok(Command::clear_default_flood_scope())
            .await
    }

    /// Queries the configured default flood scope.
    ///
    /// # Errors
    ///
    /// Returns transport, timeout, parse, or protocol errors from the companion operation.
    pub async fn get_default_flood_scope(&mut self) -> Result<DefaultFloodScope, CoreError> {
        self.send_command_expect(
            Command::get_default_flood_scope(),
            "default flood-scope query",
            |packet| match packet {
                Packet::DefaultFloodScope(scope) => Ok(scope),
                other => Err(other),
            },
        )
        .await
    }

    /// Queries one channel slot, retaining its zeroizing secret only for explicit callers.
    ///
    /// # Errors
    ///
    /// Returns transport, timeout, parse, or firmware rejection errors.
    pub async fn get_channel(&mut self, idx: u8) -> Result<ChannelInfo, CoreError> {
        self.send_command_expect(
            Command::get_channel(idx),
            "channel query",
            |packet| match packet {
                Packet::ChannelInfo(info) => Ok(info),
                other => Err(other),
            },
        )
        .await
    }

    /// Updates one channel slot without retry or replay.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or firmware rejection errors.
    pub async fn set_channel(
        &mut self,
        idx: u8,
        name: &str,
        secret: [u8; 16],
    ) -> Result<(), CoreError> {
        self.send_command_expect_ok(Command::set_channel(idx, name, secret)?)
            .await
    }

    /// Clears one channel slot without retry or replay.
    ///
    /// # Errors
    ///
    /// Returns transport, timeout, parse, or firmware rejection errors.
    pub async fn clear_channel(&mut self, idx: u8) -> Result<(), CoreError> {
        self.send_command_expect_ok(Command::clear_channel(idx))
            .await
    }

    /// Removes one exact contact without retry or replay.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or firmware rejection errors.
    pub async fn remove_contact(&mut self, public_key: &[u8]) -> Result<(), CoreError> {
        self.send_command_expect_ok(Command::remove_contact(public_key)?)
            .await
    }

    /// Authenticates to one exact remote contact and waits for the matching login result.
    ///
    /// The password is never included in an event or diagnostic. The command is not retried after
    /// any transport ambiguity.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, device-rejection, or authentication errors.
    pub async fn login(
        &mut self,
        public_key: &[u8],
        password: &str,
    ) -> Result<LoginSession, CoreError> {
        let expected_prefix = key_prefix(public_key)?;
        let tracking = self
            .send_command_expect_msg_sent_untracked(Command::send_login(public_key, password)?)
            .await?;
        let response_timeout = self.remote_response_timeout(tracking.timeout_ms);
        self.wait_for_packet(response_timeout, "remote login", |packet| match packet {
            Packet::LoginSuccess(session) if session.pubkey_prefix == expected_prefix => {
                PacketWait::Ready(session)
            }
            Packet::LoginFailed { pubkey_prefix } if pubkey_prefix == expected_prefix => {
                PacketWait::Fail(CoreError::AuthenticationFailed)
            }
            other => PacketWait::Continue(other),
        })
        .await
    }

    /// Ends an authenticated remote session.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or firmware rejection errors.
    pub async fn logout(&mut self, public_key: &[u8]) -> Result<(), CoreError> {
        self.send_command_expect_ok(Command::logout(public_key)?)
            .await
    }

    /// Checks whether the companion currently holds a session for one remote key.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or non-not-found firmware errors.
    pub async fn has_connection(&mut self, public_key: &[u8]) -> Result<bool, CoreError> {
        self.ensure_connected()?;
        self.send_raw(Command::has_connection(public_key)?.into_encoded())
            .await?;
        loop {
            let packet = self.read_next_packet_with_timeout().await?;
            match packet {
                Packet::Ok(_) => return Ok(true),
                Packet::Error(Some(2)) => return Ok(false),
                Packet::Error(code) => {
                    return Err(CoreError::DeviceRejected {
                        operation: "remote connection query",
                        code,
                    });
                }
                other => {
                    self.publish_event(&other);
                    self.update_tracking(&other);
                }
            }
        }
    }

    /// Requests and waits for a matching remote status response.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or firmware rejection errors.
    pub async fn remote_status(&mut self, public_key: &[u8]) -> Result<RemoteStatus, CoreError> {
        let expected_prefix = key_prefix(public_key)?;
        let tracking = self
            .send_command_expect_msg_sent_untracked(Command::send_status_request(public_key)?)
            .await?;
        let response_timeout = self.remote_response_timeout(tracking.timeout_ms);
        self.wait_for_packet(response_timeout, "remote status", |packet| match packet {
            Packet::RemoteStatus(status) if status.pubkey_prefix == expected_prefix => {
                PacketWait::Ready(status)
            }
            other => PacketWait::Continue(other),
        })
        .await
    }

    /// Queries local telemetry bytes.
    ///
    /// # Errors
    ///
    /// Returns transport, timeout, parse, or firmware rejection errors.
    pub async fn get_self_telemetry(&mut self) -> Result<TelemetryResponse, CoreError> {
        self.send_command_expect(
            Command::get_self_telemetry(),
            "self telemetry query",
            |packet| match packet {
                Packet::TelemetryResponse(response) => Ok(response),
                other => Err(other),
            },
        )
        .await
    }

    /// Sends one typed remote binary request and waits for its exact correlation tag.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or firmware rejection errors.
    pub async fn binary_request(
        &mut self,
        public_key: &[u8],
        request_type: u8,
        data: &[u8],
    ) -> Result<BinaryResponse, CoreError> {
        let tracking = self
            .send_command_expect_msg_sent_untracked(Command::send_binary_request(
                public_key,
                request_type,
                data,
            )?)
            .await?;
        let expected_tag = tracking.ack_code;
        let response_timeout = self.remote_response_timeout(tracking.timeout_ms);
        self.wait_for_packet(response_timeout, "binary request", |packet| match packet {
            Packet::BinaryResponse(response) if response.tag == expected_tag => {
                PacketWait::Ready(response)
            }
            other => PacketWait::Continue(other),
        })
        .await
    }

    /// Sends one anonymous metadata request and waits for its exact correlation tag.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or firmware rejection errors.
    pub async fn anonymous_request(
        &mut self,
        public_key: &[u8],
        request_type: u8,
        reply_route: ContactRoute,
        reply_path: &Path,
    ) -> Result<BinaryResponse, CoreError> {
        let tracking = self
            .send_command_expect_msg_sent_untracked(Command::send_anonymous_request(
                public_key,
                request_type,
                reply_route,
                reply_path,
            )?)
            .await?;
        let expected_tag = tracking.ack_code;
        let response_timeout = self.remote_response_timeout(tracking.timeout_ms);
        self.wait_for_packet(
            response_timeout,
            "anonymous request",
            |packet| match packet {
                Packet::BinaryResponse(response) if response.tag == expected_tag => {
                    PacketWait::Ready(response)
                }
                other => PacketWait::Continue(other),
            },
        )
        .await
    }

    /// Sends one correlated node-discovery request and returns after the companion accepts it.
    ///
    /// Matching discovery responses arrive later as [`Event::ControlData`] events. Use
    /// [`crate::domain::ControlData::node_discovery_response`] to decode and correlate them by
    /// `tag`.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or firmware rejection errors.
    pub async fn send_node_discovery(
        &mut self,
        filter: u8,
        prefix_only: bool,
        tag: u32,
        since: Option<u32>,
    ) -> Result<(), CoreError> {
        self.send_command_expect_ok(Command::send_node_discovery(
            filter,
            prefix_only,
            tag,
            since,
        )?)
        .await
    }

    /// Floods a path-discovery request and waits for the matching contact prefix.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or firmware rejection errors.
    pub async fn discover_path(&mut self, public_key: &[u8]) -> Result<PathDiscovery, CoreError> {
        let expected_prefix = key_prefix(public_key)?;
        let tracking = self
            .send_command_expect_msg_sent_untracked(Command::discover_path(public_key)?)
            .await?;
        let response_timeout = self.remote_response_timeout(tracking.timeout_ms);
        self.wait_for_packet(response_timeout, "path discovery", |packet| match packet {
            Packet::PathDiscovery(path) if path.pubkey_prefix == expected_prefix => {
                PacketWait::Ready(path)
            }
            other => PacketWait::Continue(other),
        })
        .await
    }

    /// Signs bounded bytes using the companion identity without exporting its private key.
    ///
    /// Chunks are never retried after a timeout or transport ambiguity.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, state, or firmware rejection errors.
    pub async fn sign(&mut self, data: &[u8]) -> Result<Signature, CoreError> {
        let max_data_bytes = self
            .send_command_expect(
                Command::sign_start(),
                "signing start",
                |packet| match packet {
                    Packet::SignStart { max_data_bytes } => Ok(max_data_bytes),
                    other => Err(other),
                },
            )
            .await?;
        if data.len() > usize::try_from(max_data_bytes).unwrap_or(usize::MAX) {
            return Err(CoreError::InvalidArgument {
                field: "data",
                message: format!(
                    "{} bytes exceeds device signing limit {max_data_bytes}",
                    data.len()
                ),
            });
        }
        for chunk in data.chunks(SIGNING_CHUNK_BYTES) {
            self.send_command_expect_ok(Command::sign_data(chunk)?)
                .await?;
        }
        self.send_command_expect(
            Command::sign_finish(),
            "signing finish",
            |packet| match packet {
                Packet::Signature(signature) => Ok(signature),
                other => Err(other),
            },
        )
        .await
    }

    /// Exports zeroizing private-key material when firmware explicitly enables the feature.
    ///
    /// # Errors
    ///
    /// Returns transport, timeout, parse, disabled-feature, or firmware rejection errors.
    pub async fn export_private_key(&mut self) -> Result<PrivateKeyMaterial, CoreError> {
        self.ensure_connected()?;
        self.send_raw(Command::export_private_key().into_encoded())
            .await?;
        loop {
            let packet = self.read_next_packet_with_timeout().await?;
            match packet {
                Packet::PrivateKey(key) => return Ok(key),
                Packet::Disabled => {
                    return Err(CoreError::FeatureDisabled {
                        feature: "private-key export",
                    });
                }
                Packet::Error(code) => {
                    return Err(CoreError::DeviceRejected {
                        operation: "private-key export",
                        code,
                    });
                }
                other => {
                    self.publish_event(&other);
                    self.update_tracking(&other);
                }
            }
        }
    }

    /// Imports exact zeroizing private-key material when firmware explicitly enables the feature.
    ///
    /// # Errors
    ///
    /// Returns transport, timeout, parse, disabled-feature, or firmware rejection errors.
    pub async fn import_private_key(&mut self, key: &PrivateKeyMaterial) -> Result<(), CoreError> {
        self.ensure_connected()?;
        self.send_raw(Command::import_private_key(key).into_encoded())
            .await?;
        loop {
            let packet = self.read_next_packet_with_timeout().await?;
            match packet {
                Packet::Ok(_) => return Ok(()),
                Packet::Disabled => {
                    return Err(CoreError::FeatureDisabled {
                        feature: "private-key import",
                    });
                }
                Packet::Error(code) => {
                    return Err(CoreError::DeviceRejected {
                        operation: "private-key import",
                        code,
                    });
                }
                other => {
                    self.publish_event(&other);
                    self.update_tracking(&other);
                }
            }
        }
    }

    /// Updates the device pairing PIN without retaining it in events or diagnostics.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or firmware rejection errors.
    pub async fn set_device_pin(&mut self, pin: u32) -> Result<(), CoreError> {
        self.send_command_expect_ok(Command::set_device_pin(pin)?)
            .await
    }

    /// Requests a local reboot, then invalidates the current session without replaying the write.
    ///
    /// # Errors
    ///
    /// Returns a transport error only when the one attempted write fails.
    pub async fn reboot(&mut self) -> Result<(), CoreError> {
        self.send_disconnect_command(Command::reboot()).await
    }

    /// Requests a factory reset, then invalidates the current session without replaying the write.
    ///
    /// # Errors
    ///
    /// Returns a transport error only when the one attempted write fails.
    pub async fn factory_reset(&mut self) -> Result<(), CoreError> {
        self.send_disconnect_command(Command::factory_reset()).await
    }

    /// Sends a direct text command and returns the expected firmware ACK code.
    ///
    /// # Errors
    /// Returns `CoreError::ProtocolInvariant` for invalid input validation, `CoreError::Timeout` while
    /// waiting for firmware confirmation, `CoreError::Disconnected` when transport is closed, or
    /// `CoreError::Parse` if decoding the ACK packet fails.
    pub async fn send_direct_text(
        &mut self,
        destination_prefix: &[u8],
        attempt: u8,
        text: &str,
    ) -> Result<CommandTracking, CoreError> {
        validate_destination_prefix(destination_prefix)?;
        validate_text(text, MAX_INNER_PAYLOAD - 13, "text")?;
        let timestamp = u32::try_from(Utc::now().timestamp()).unwrap_or(0);
        let command = Command::send_direct_text(destination_prefix, timestamp, attempt, text);
        self.send_command_expect_msg_sent(command).await
    }

    /// Sends a direct command payload and returns the expected firmware ACK code.
    ///
    /// # Errors
    /// Returns `CoreError::ProtocolInvariant` for invalid input validation, `CoreError::Timeout` while
    /// waiting for firmware confirmation, `CoreError::Disconnected` when transport is closed, or
    /// `CoreError::Parse` if decoding the ACK packet fails.
    pub async fn send_direct_command(
        &mut self,
        destination_prefix: &[u8],
        attempt: u8,
        command_text: &str,
    ) -> Result<CommandTracking, CoreError> {
        validate_destination_prefix(destination_prefix)?;
        validate_text(command_text, MAX_INNER_PAYLOAD - 13, "command")?;
        let timestamp = u32::try_from(Utc::now().timestamp()).unwrap_or(0);
        let command =
            Command::send_direct_command(destination_prefix, timestamp, attempt, command_text);
        self.send_command_expect_msg_sent(command).await
    }

    /// Sends a channel message and waits for the firmware `OK` response.
    ///
    /// # Errors
    /// Returns `CoreError::ProtocolInvariant` when validation or firmware ACK checks fail, `CoreError::Timeout`
    /// when no confirmation is received, `CoreError::Disconnected` when transport closes, or
    /// `CoreError::Parse` if packet decoding fails.
    pub async fn send_channel_message(
        &mut self,
        channel: u8,
        txt_type: u8,
        text: &str,
    ) -> Result<(), CoreError> {
        validate_text(text, MAX_INNER_PAYLOAD - 7, "text")?;
        let timestamp = u32::try_from(Utc::now().timestamp()).unwrap_or(0);
        let command = Command::send_channel(channel, txt_type, timestamp, text);
        self.send_command_expect_ok(command).await
    }

    /// Requests one queued message from firmware.
    ///
    /// # Errors
    /// Returns `CoreError::Disconnected` when transport is unavailable, `CoreError::Timeout` when no
    /// message arrives in time, `CoreError::ProtocolInvariant` if firmware reports an error, or
    /// `CoreError::Parse` when packet decoding fails.
    pub async fn sync_next_message(&mut self) -> Result<Option<Message>, CoreError> {
        self.ensure_connected()?;
        if let Some(ready) = self.deferred_sync.take() {
            return Self::ready_sync_result(ready);
        }
        if let Some(ready) = self.drain_ready_packets_before_sync()? {
            return Self::ready_sync_result(ready);
        }
        if self.sync_state == SyncState::AwaitingResponse {
            return self.wait_for_sync_response().await;
        }

        // Set the state before awaiting the transport write. Dropping this future after the write
        // may otherwise leave an untracked response that a later command could misattribute.
        self.sync_state = SyncState::AwaitingResponse;
        self.write_raw(Command::sync_next_message().into_encoded())
            .await?;
        self.wait_for_sync_response().await
    }

    async fn wait_for_sync_response(&mut self) -> Result<Option<Message>, CoreError> {
        loop {
            let packet = self.read_next_packet_with_timeout().await?;
            match packet {
                Packet::ContactMsg(message) => {
                    self.sync_state = SyncState::Idle;
                    self.publish_event(&Packet::ContactMsg(message.clone()));
                    return Ok(Some(message));
                }
                Packet::ChannelMsg(message) => {
                    self.sync_state = SyncState::Idle;
                    self.publish_event(&Packet::ChannelMsg(message.clone()));
                    return Ok(Some(message));
                }
                Packet::NoMoreMsgs => {
                    self.sync_state = SyncState::Idle;
                    return Ok(None);
                }
                Packet::Error(_code) => {
                    self.sync_state = SyncState::Idle;
                    return Err(CoreError::ProtocolInvariant(
                        "sync next message returned an error",
                    ));
                }
                other => {
                    self.publish_event(&other);
                    self.update_tracking(&other);
                }
            }
        }
    }

    /// Waits for a firmware ACK that matches a tracked ACK code.
    ///
    /// # Errors
    /// Returns `CoreError::Disconnected` when transport is unavailable, `CoreError::Timeout` on timeout,
    /// `CoreError::ProtocolInvariant` when firmware reports an error, or `CoreError::Parse` when packet
    /// decoding fails.
    pub async fn wait_for_ack(
        &mut self,
        ack_code: [u8; 4],
        request_timeout: Option<Duration>,
    ) -> Result<Ack, CoreError> {
        if let Some(request_timeout) = request_timeout {
            validate_operation_timeout(request_timeout)?;
        }
        self.ensure_connected()?;
        if let Some(ack) = self.take_completed_ack(ack_code) {
            return Ok(ack);
        }
        let timeout_at = request_timeout.unwrap_or(self.request_timeout);
        let deadline = timeout(timeout_at, async {
            loop {
                let packet = self.read_next_packet().await?;
                match packet {
                    Packet::Ack(ack) => {
                        if ack.code == ack_code {
                            self.discard_pending_ack(ack_code);
                            self.publish_event(&Packet::Ack(ack.clone()));
                            return Ok(ack);
                        }
                        let packet = Packet::Ack(ack);
                        self.publish_event(&packet);
                        self.update_tracking(&packet);
                    }
                    Packet::Error(_code) => {
                        return Err(CoreError::ProtocolInvariant(
                            "ack request returned an error",
                        ));
                    }
                    other => {
                        self.publish_event(&other);
                    }
                }
            }
        });

        let result = match deadline.await {
            Ok(result) => result,
            Err(_) => Err(CoreError::Timeout),
        };
        if result.is_err() {
            self.discard_pending_ack(ack_code);
            self.discard_completed_ack(ack_code);
        }
        result
    }

    async fn send_only(&mut self, command: Command) -> Result<(), CoreError> {
        self.ensure_connected()?;
        self.send_raw(command.into_encoded()).await
    }

    async fn send_command_expect_msg_sent(
        &mut self,
        command: Command,
    ) -> Result<CommandTracking, CoreError> {
        self.send_command_expect_msg_sent_inner(command, true).await
    }

    async fn send_command_expect_msg_sent_untracked(
        &mut self,
        command: Command,
    ) -> Result<CommandTracking, CoreError> {
        self.send_command_expect_msg_sent_inner(command, false)
            .await
    }

    async fn send_command_expect_msg_sent_inner(
        &mut self,
        command: Command,
        track_ack: bool,
    ) -> Result<CommandTracking, CoreError> {
        self.ensure_connected()?;

        self.send_raw(command.into_encoded()).await?;
        loop {
            let packet = self.read_next_packet_with_timeout().await?;
            match packet {
                Packet::MsgSent {
                    destination_type: _,
                    expected_ack,
                    suggested_timeout_ms,
                } => {
                    let tracking = CommandTracking {
                        ack_code: expected_ack,
                        timeout_ms: suggested_timeout_ms,
                    };
                    if track_ack {
                        self.discard_completed_ack(expected_ack);
                        self.remember_pending_ack(tracking.clone());
                    }
                    return Ok(tracking);
                }
                Packet::Error(code) => {
                    return Err(CoreError::DeviceRejected {
                        operation: "message send",
                        code,
                    });
                }
                other => {
                    self.publish_event(&other);
                    self.update_tracking(&other);
                }
            }
        }
    }

    async fn send_command_expect_ok(&mut self, command: Command) -> Result<(), CoreError> {
        self.ensure_connected()?;
        self.send_raw(command.into_encoded()).await?;
        loop {
            let packet = self.read_next_packet_with_timeout().await?;
            match packet {
                Packet::Ok(_) => return Ok(()),
                Packet::Disabled => {
                    return Err(CoreError::FeatureDisabled {
                        feature: "requested operation",
                    });
                }
                Packet::Error(code) => {
                    return Err(CoreError::DeviceRejected {
                        operation: "command",
                        code,
                    });
                }
                other => {
                    self.publish_event(&other);
                    self.update_tracking(&other);
                }
            }
        }
    }

    async fn send_command_expect<R, F>(
        &mut self,
        command: Command,
        error_context: &'static str,
        mut extract: F,
    ) -> Result<R, CoreError>
    where
        F: FnMut(Packet) -> Result<R, Packet>,
    {
        self.ensure_connected()?;
        self.send_raw(command.into_encoded()).await?;
        loop {
            let packet = self.read_next_packet_with_timeout().await?;
            match extract(packet) {
                Ok(value) => return Ok(value),
                Err(Packet::Disabled) => {
                    return Err(CoreError::FeatureDisabled {
                        feature: error_context,
                    });
                }
                Err(Packet::Error(code)) => {
                    return Err(CoreError::DeviceRejected {
                        operation: error_context,
                        code,
                    });
                }
                Err(other) => {
                    self.publish_event(&other);
                    self.update_tracking(&other);
                }
            }
        }
    }

    async fn send_raw(&mut self, payload: Vec<u8>) -> Result<(), CoreError> {
        if self.sync_state == SyncState::AwaitingResponse {
            return Err(CoreError::ProtocolInvariant(
                "a prior inbox synchronization response must be reconciled before another command",
            ));
        }
        self.write_raw(payload).await
    }

    async fn write_raw(&mut self, payload: Vec<u8>) -> Result<(), CoreError> {
        let payload = Zeroizing::new(payload);
        if payload.is_empty() {
            return Err(CoreError::ProtocolInvariant(
                "cannot send an empty companion packet",
            ));
        }
        if payload.len() > MAX_INNER_PAYLOAD {
            return Err(crate::error::ParseError::OversizedPacketPayload {
                actual: payload.len(),
                maximum: MAX_INNER_PAYLOAD,
            }
            .into());
        }
        match self.transport.write(payload.as_slice()).await {
            Ok(()) => Ok(()),
            Err(error) => {
                if is_terminal_write_error(&error) {
                    self.mark_disconnected();
                }
                Err(CoreError::Transport(error))
            }
        }
    }

    async fn send_disconnect_command(&mut self, command: Command) -> Result<(), CoreError> {
        self.ensure_connected()?;
        self.send_raw(command.into_encoded()).await?;
        let _disconnect_result = self.transport.disconnect().await;
        self.mark_disconnected();
        Ok(())
    }

    fn remote_response_timeout(&self, suggested_timeout_ms: u32) -> Duration {
        let suggested = Duration::from_millis(u64::from(suggested_timeout_ms))
            .saturating_add(Duration::from_secs(1));
        self.request_timeout
            .max(suggested)
            .min(MAX_REMOTE_RESPONSE_TIMEOUT)
    }

    async fn wait_for_packet<R, F>(
        &mut self,
        response_timeout: Duration,
        operation: &'static str,
        mut extract: F,
    ) -> Result<R, CoreError>
    where
        F: FnMut(Packet) -> PacketWait<R>,
    {
        timeout(response_timeout, async {
            loop {
                let packet = self.read_next_packet().await?;
                match extract(packet) {
                    PacketWait::Ready(value) => return Ok(value),
                    PacketWait::Fail(error) => return Err(error),
                    PacketWait::Continue(Packet::Error(code)) => {
                        return Err(CoreError::DeviceRejected { operation, code });
                    }
                    PacketWait::Continue(Packet::Disabled) => {
                        return Err(CoreError::FeatureDisabled { feature: operation });
                    }
                    PacketWait::Continue(other) => {
                        self.publish_event(&other);
                        self.update_tracking(&other);
                    }
                }
            }
        })
        .await
        .map_err(|_| CoreError::Timeout)?
    }

    async fn read_next_packet(&mut self) -> Result<Packet, CoreError> {
        let raw = match self.transport.read().await {
            Ok(Some(raw)) => raw,
            Ok(None) => {
                self.mark_disconnected();
                return Err(CoreError::Disconnected);
            }
            Err(error) => {
                if is_disconnect_error(&error) {
                    self.mark_disconnected();
                }
                return Err(CoreError::Transport(error));
            }
        };

        self.parse_packet(&raw)
    }

    fn drain_ready_packets_before_sync(&mut self) -> Result<Option<ReadySync>, CoreError> {
        // Valid asynchronous PUSH_CODE packets are published before a fresh command. A ready
        // message or terminal packet is conservatively treated as a late response to an earlier
        // cancelled or timed-out sync, because response packets have no request tag.
        for index in 0..=CLIENT_SYNC_READY_DRAIN_CAPACITY {
            let raw = match self.transport.try_read() {
                Ok(ReadyRead::Pending) => return Ok(None),
                Ok(ReadyRead::Closed) => {
                    self.mark_disconnected();
                    return Err(CoreError::Disconnected);
                }
                Ok(ReadyRead::Packet(raw)) => raw,
                Err(error) => {
                    if is_disconnect_error(&error) {
                        self.mark_disconnected();
                    }
                    return Err(CoreError::Transport(error));
                }
            };
            let packet = self.parse_packet(&raw)?;
            match packet {
                Packet::ContactMsg(message) => {
                    self.sync_state = SyncState::Idle;
                    self.publish_event(&Packet::ContactMsg(message.clone()));
                    return Ok(Some(ReadySync::Message(message)));
                }
                Packet::ChannelMsg(message) => {
                    self.sync_state = SyncState::Idle;
                    self.publish_event(&Packet::ChannelMsg(message.clone()));
                    return Ok(Some(ReadySync::Message(message)));
                }
                Packet::NoMoreMsgs => {
                    self.sync_state = SyncState::Idle;
                    return Ok(Some(ReadySync::NoMoreMessages));
                }
                Packet::Error(_code) => {
                    self.sync_state = SyncState::Idle;
                    return Ok(Some(ReadySync::Error));
                }
                other => {
                    self.publish_event(&other);
                    self.update_tracking(&other);
                }
            }
            if index == CLIENT_SYNC_READY_DRAIN_CAPACITY {
                return Err(CoreError::ProtocolInvariant(
                    "too many buffered packets before inbox synchronization",
                ));
            }
        }
        Ok(None)
    }

    fn retain_sync_response(&mut self, packet: &Packet) {
        if self.sync_state != SyncState::AwaitingResponse {
            return;
        }
        let ready = match packet {
            Packet::ContactMsg(message) | Packet::ChannelMsg(message) => {
                Some(ReadySync::Message(message.clone()))
            }
            Packet::NoMoreMsgs => Some(ReadySync::NoMoreMessages),
            Packet::Error(_) => Some(ReadySync::Error),
            _ => None,
        };
        if let Some(ready) = ready {
            self.deferred_sync = Some(ready);
            self.sync_state = SyncState::Idle;
        }
    }

    fn ready_sync_result(ready: ReadySync) -> Result<Option<Message>, CoreError> {
        match ready {
            ReadySync::Message(message) => Ok(Some(message)),
            ReadySync::NoMoreMessages => Ok(None),
            ReadySync::Error => Err(CoreError::ProtocolInvariant(
                "sync next message returned an error",
            )),
        }
    }

    fn parse_packet(&mut self, raw: &[u8]) -> Result<Packet, CoreError> {
        let mut packet = Packet::parse(raw).map_err(CoreError::Parse)?;
        if let Packet::ContactMsg(message) | Packet::ChannelMsg(message) = &mut packet {
            message.observation_id = Some(self.next_message_observation_id);
            self.next_message_observation_id = self.next_message_observation_id.wrapping_add(1);
            if self.next_message_observation_id == 0 {
                self.next_message_observation_id = 1;
            }
        }
        Ok(packet)
    }

    async fn read_next_packet_with_timeout(&mut self) -> Result<Packet, CoreError> {
        timeout(self.request_timeout, self.read_next_packet())
            .await
            .map_err(|_| CoreError::Timeout)?
    }

    fn publish_event(&mut self, packet: &Packet) {
        self.retain_sync_response(packet);
        if let Some(event) = packet.clone().into_event() {
            self.publish_domain_event(&event);
        }
    }

    fn publish_domain_event(&self, event: &Event) {
        let _ = self.event_tx.send(event.clone());
    }

    fn update_tracking(&mut self, packet: &Packet) {
        match packet {
            Packet::MsgSent {
                expected_ack,
                suggested_timeout_ms,
                ..
            } => {
                self.discard_completed_ack(*expected_ack);
                self.remember_pending_ack(CommandTracking {
                    ack_code: *expected_ack,
                    timeout_ms: *suggested_timeout_ms,
                });
            }
            Packet::Ack(ack) if self.take_pending_ack(ack.code).is_some() => {
                self.remember_completed_ack(ack.clone());
            }
            _ => {}
        }
    }

    fn mark_disconnected(&mut self) {
        self.clear_tracking();
        self.clear_sync_tracking();
        if std::mem::replace(&mut self.connected, false) {
            let _ = self.event_tx.send(Event::Disconnected);
        }
    }

    fn clear_tracking(&mut self) {
        self.pending_acks.clear();
        self.pending_ack_order.clear();
        self.completed_acks.clear();
        self.completed_ack_order.clear();
    }

    fn clear_sync_tracking(&mut self) {
        self.sync_state = SyncState::Idle;
        self.deferred_sync = None;
    }

    fn remember_pending_ack(&mut self, tracking: CommandTracking) {
        self.discard_pending_ack(tracking.ack_code);
        while self.pending_ack_order.len() >= CLIENT_PENDING_ACK_CAPACITY {
            if let Some(oldest) = self.pending_ack_order.pop_front() {
                self.pending_acks.remove(&oldest);
            }
        }
        self.pending_ack_order.push_back(tracking.ack_code);
        self.pending_acks.insert(tracking.ack_code, tracking);
    }

    fn discard_pending_ack(&mut self, ack_code: [u8; 4]) {
        self.pending_acks.remove(&ack_code);
        self.pending_ack_order.retain(|code| *code != ack_code);
    }

    fn take_pending_ack(&mut self, ack_code: [u8; 4]) -> Option<CommandTracking> {
        let tracking = self.pending_acks.remove(&ack_code);
        if tracking.is_some() {
            self.pending_ack_order.retain(|code| *code != ack_code);
        }
        tracking
    }

    fn remember_completed_ack(&mut self, ack: Ack) {
        self.discard_completed_ack(ack.code);
        while self.completed_ack_order.len() >= CLIENT_COMPLETED_ACK_CAPACITY {
            if let Some(oldest) = self.completed_ack_order.pop_front() {
                self.completed_acks.remove(&oldest);
            }
        }
        self.completed_ack_order.push_back(ack.code);
        self.completed_acks.insert(ack.code, ack);
    }

    fn discard_completed_ack(&mut self, ack_code: [u8; 4]) {
        self.completed_acks.remove(&ack_code);
        self.completed_ack_order.retain(|code| *code != ack_code);
    }

    fn take_completed_ack(&mut self, ack_code: [u8; 4]) -> Option<Ack> {
        let ack = self.completed_acks.remove(&ack_code);
        if ack.is_some() {
            self.completed_ack_order.retain(|code| *code != ack_code);
        }
        ack
    }

    pub(crate) fn event_sender(&self) -> broadcast::Sender<Event> {
        self.event_tx.clone()
    }

    pub(crate) const fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    fn ensure_connected(&self) -> Result<(), CoreError> {
        if self.connected {
            Ok(())
        } else {
            Err(CoreError::Disconnected)
        }
    }
}

fn validate_operation_timeout(timeout: Duration) -> Result<(), CoreError> {
    if timeout.is_zero() || timeout > MAX_OPERATION_TIMEOUT {
        return Err(CoreError::InvalidArgument {
            field: "timeout",
            message: "must be greater than zero and at most 24 hours".to_owned(),
        });
    }
    Ok(())
}

fn is_disconnect_error(error: &TransportError) -> bool {
    match error {
        TransportError::NotConnected | TransportError::Closed => true,
        TransportError::Io(error) => matches!(
            error.kind(),
            std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::NotConnected
                | std::io::ErrorKind::UnexpectedEof
        ),
        _ => false,
    }
}

fn is_terminal_write_error(error: &TransportError) -> bool {
    matches!(
        error,
        TransportError::NotConnected
            | TransportError::Closed
            | TransportError::Timeout
            | TransportError::ReconnectFailed { .. }
            | TransportError::Io(_)
    )
}

fn key_prefix(public_key: &[u8]) -> Result<[u8; 6], CoreError> {
    if public_key.len() != 32 {
        return Err(CoreError::InvalidArgument {
            field: "public_key",
            message: format!("expected exactly 32 bytes, got {}", public_key.len()),
        });
    }
    let mut prefix = [0_u8; 6];
    prefix.copy_from_slice(&public_key[..6]);
    Ok(prefix)
}

fn validate_destination_prefix(prefix: &[u8]) -> Result<(), CoreError> {
    if prefix.len() != 6 {
        return Err(CoreError::InvalidArgument {
            field: "destination_prefix",
            message: format!("expected exactly 6 bytes, got {}", prefix.len()),
        });
    }
    Ok(())
}

fn validate_text(text: &str, maximum_bytes: usize, field: &'static str) -> Result<(), CoreError> {
    if text.is_empty() {
        return Err(CoreError::InvalidArgument {
            field,
            message: "must not be empty".to_owned(),
        });
    }
    if text.len() > maximum_bytes {
        return Err(CoreError::InvalidArgument {
            field,
            message: format!(
                "UTF-8 payload is {} bytes; maximum is {maximum_bytes}",
                text.len()
            ),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::io;

    use async_trait::async_trait;

    use super::*;
    use crate::protocol::PacketCode;
    use crate::transport::{ScriptedTransport, TransportKind};

    fn message_sent_packet(ack_code: [u8; 4], timeout_ms: u32) -> Vec<u8> {
        let mut raw = vec![PacketCode::MsgSent.to_u8(), 0];
        raw.extend_from_slice(&ack_code);
        raw.extend_from_slice(&timeout_ms.to_le_bytes());
        raw
    }

    fn login_success_packet(prefix: [u8; 6], permissions: u8) -> Vec<u8> {
        let mut raw = vec![PacketCode::LoginSuccess.to_u8(), permissions];
        raw.extend_from_slice(&prefix);
        raw
    }

    fn binary_response_packet(tag: [u8; 4], payload: &[u8]) -> Vec<u8> {
        let mut raw = vec![PacketCode::BinaryResponse.to_u8(), 0];
        raw.extend_from_slice(&tag);
        raw.extend_from_slice(payload);
        raw
    }

    fn contact_message_packet(route: u8, timestamp: u32, text: &str) -> Vec<u8> {
        let mut raw = vec![PacketCode::ContactMsgRecv.to_u8()];
        raw.extend_from_slice(&[0x42; 6]);
        raw.push(route);
        raw.push(0);
        raw.extend_from_slice(&timestamp.to_le_bytes());
        raw.extend_from_slice(text.as_bytes());
        raw
    }

    fn contact_end_packet(lastmod: u32) -> Vec<u8> {
        let mut raw = vec![PacketCode::ContactEnd.to_u8()];
        raw.extend_from_slice(&lastmod.to_le_bytes());
        raw
    }

    struct ReadErrorTransport {
        kind: io::ErrorKind,
    }

    struct BlockingReadTransport;

    struct SyncResponseTransport {
        inbound: VecDeque<Vec<u8>>,
        writes: usize,
        preexisting_notification: bool,
        respond_on_write: bool,
    }

    impl SyncResponseTransport {
        fn new() -> Self {
            Self {
                inbound: VecDeque::new(),
                writes: 0,
                preexisting_notification: false,
                respond_on_write: true,
            }
        }

        fn with_preexisting_notification() -> Self {
            Self {
                inbound: VecDeque::from([vec![PacketCode::MessagesWaiting.to_u8()]]),
                writes: 0,
                preexisting_notification: true,
                respond_on_write: true,
            }
        }

        fn delayed() -> Self {
            Self {
                inbound: VecDeque::new(),
                writes: 0,
                preexisting_notification: false,
                respond_on_write: false,
            }
        }
    }

    #[async_trait]
    impl Transport for ReadErrorTransport {
        fn kind(&self) -> TransportKind {
            TransportKind::Scripted
        }

        async fn connect(&mut self) -> Result<(), TransportError> {
            Ok(())
        }

        async fn disconnect(&mut self) -> Result<(), TransportError> {
            Ok(())
        }

        async fn write(&mut self, _payload: &[u8]) -> Result<(), TransportError> {
            Ok(())
        }

        async fn read(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
            Err(TransportError::Io(io::Error::from(self.kind)))
        }
    }

    #[async_trait]
    impl Transport for BlockingReadTransport {
        fn kind(&self) -> TransportKind {
            TransportKind::Scripted
        }

        async fn connect(&mut self) -> Result<(), TransportError> {
            Ok(())
        }

        async fn disconnect(&mut self) -> Result<(), TransportError> {
            Ok(())
        }

        async fn write(&mut self, _payload: &[u8]) -> Result<(), TransportError> {
            Ok(())
        }

        async fn read(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
            pending().await
        }
    }

    #[async_trait]
    impl Transport for SyncResponseTransport {
        fn kind(&self) -> TransportKind {
            TransportKind::Scripted
        }

        async fn connect(&mut self) -> Result<(), TransportError> {
            Ok(())
        }

        async fn disconnect(&mut self) -> Result<(), TransportError> {
            Ok(())
        }

        async fn write(&mut self, payload: &[u8]) -> Result<(), TransportError> {
            assert_eq!(payload, Command::sync_next_message().encode());
            self.writes = self.writes.saturating_add(1);
            if self.respond_on_write {
                if self.writes == 1 {
                    if !self.preexisting_notification {
                        self.inbound
                            .push_back(vec![PacketCode::MessagesWaiting.to_u8()]);
                    }
                    self.inbound
                        .push_back(contact_message_packet(u8::MAX, 42, "queued response"));
                } else {
                    self.inbound.push_back(vec![PacketCode::NoMoreMsgs.to_u8()]);
                }
            }
            Ok(())
        }

        async fn read(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
            match self.inbound.pop_front() {
                Some(packet) => Ok(Some(packet)),
                None => pending().await,
            }
        }

        fn try_read(&mut self) -> Result<ReadyRead, TransportError> {
            Ok(self
                .inbound
                .pop_front()
                .map_or(ReadyRead::Pending, ReadyRead::Packet))
        }
    }

    #[test]
    fn destination_prefix_is_exactly_six_bytes() {
        assert!(validate_destination_prefix(&[0_u8; 6]).is_ok());
        assert!(validate_destination_prefix(&[0_u8; 5]).is_err());
        assert!(validate_destination_prefix(&[0_u8; 32]).is_err());
    }

    #[test]
    fn text_limits_count_utf8_bytes() {
        assert!(validate_text("ok", 2, "text").is_ok());
        assert!(validate_text("", 2, "text").is_err());
        assert!(validate_text("é", 1, "text").is_err());
    }

    #[test]
    fn request_timeout_bounds_are_strict() {
        assert!(Client::with_timeout(ScriptedTransport::new(), MAX_OPERATION_TIMEOUT).is_ok());
        assert!(Client::with_timeout(ScriptedTransport::new(), Duration::ZERO).is_err());
        assert!(
            Client::with_timeout(
                ScriptedTransport::new(),
                MAX_OPERATION_TIMEOUT.saturating_add(Duration::from_nanos(1)),
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn acknowledgement_timeout_is_validated_before_io() {
        let mut client = Client::new(ScriptedTransport::new());
        assert!(matches!(
            client
                .wait_for_ack(
                    [1, 2, 3, 4],
                    Some(MAX_OPERATION_TIMEOUT.saturating_add(Duration::from_nanos(1))),
                )
                .await,
            Err(CoreError::InvalidArgument {
                field: "timeout",
                ..
            })
        ));
    }

    #[tokio::test]
    async fn contact_snapshot_preserves_terminating_lastmod() {
        let expected_lastmod = 0x7856_3412;
        let mut client = Client::new(ScriptedTransport::with_inbound_frames([
            contact_end_packet(expected_lastmod),
        ]));
        client.connected = true;

        let snapshot = client
            .list_contacts_snapshot(None)
            .await
            .unwrap_or_else(|error| panic!("contact snapshot failed: {error}"));
        assert!(snapshot.contacts.is_empty());
        assert_eq!(snapshot.lastmod, expected_lastmod);
        assert_eq!(
            client.transport.outbound_frames(),
            vec![Command::get_contacts(None).encode()]
        );
    }

    #[tokio::test]
    async fn contact_snapshot_does_not_invent_a_marker() {
        let mut client = Client::new(ScriptedTransport::with_inbound_frames([vec![
            PacketCode::NoMoreMsgs.to_u8(),
        ]]));
        client.connected = true;

        assert!(matches!(
            client.list_contacts_snapshot(None).await,
            Err(CoreError::ProtocolInvariant(
                "contact snapshot ended without a last-modified marker"
            ))
        ));
    }

    #[tokio::test]
    async fn legacy_contact_list_accepts_no_more_messages_without_a_marker() {
        let mut client = Client::new(ScriptedTransport::with_inbound_frames([vec![
            PacketCode::NoMoreMsgs.to_u8(),
        ]]));
        client.connected = true;

        let contacts = client
            .list_contacts(None)
            .await
            .unwrap_or_else(|error| panic!("legacy contact list failed: {error}"));
        assert!(contacts.is_empty());
    }

    #[tokio::test]
    async fn repeated_logical_messages_are_never_silently_dropped() {
        let frames = [
            contact_message_packet(u8::MAX, 42, "same payload"),
            contact_message_packet(1, 42, "same payload"),
        ];
        let mut client = Client::new(ScriptedTransport::with_inbound_frames(frames));
        client.connected = true;
        let mut events = client.subscribe();

        assert!(matches!(
            client.next_event().await,
            Ok(Some(Event::Message(Message {
                sender_timestamp: 42,
                ..
            })))
        ));
        assert!(matches!(
            client.next_event().await,
            Ok(Some(Event::Message(Message {
                sender_timestamp: 42,
                ..
            })))
        ));
        assert!(matches!(events.try_recv(), Ok(Event::Message(_))));
        assert!(matches!(events.try_recv(), Ok(Event::Message(_))));
    }

    #[tokio::test]
    async fn sync_uses_exactly_one_command_for_each_queue_response() {
        let frames = [
            contact_message_packet(u8::MAX, 42, "same payload"),
            contact_message_packet(1, 42, "same payload"),
            vec![PacketCode::NoMoreMsgs.to_u8()],
        ];
        let mut client = Client::new(ScriptedTransport::with_inbound_frames(frames));
        client.connected = true;
        let mut events = client.subscribe();

        assert!(matches!(
            client.sync_next_message().await,
            Ok(Some(Message {
                sender_timestamp: 42,
                ..
            }))
        ));
        assert_eq!(client.transport.outbound_frames().len(), 1);
        assert!(matches!(
            client.sync_next_message().await,
            Ok(Some(Message {
                sender_timestamp: 42,
                ..
            }))
        ));
        assert_eq!(client.transport.outbound_frames().len(), 2);
        assert!(matches!(client.sync_next_message().await, Ok(None)));
        assert_eq!(client.transport.outbound_frames().len(), 3);
        assert!(matches!(events.try_recv(), Ok(Event::Message(_))));
        assert!(matches!(events.try_recv(), Ok(Event::Message(_))));
    }

    #[tokio::test]
    async fn prior_message_waiting_notification_does_not_satisfy_sync() {
        let frames = [
            vec![PacketCode::MessagesWaiting.to_u8()],
            contact_message_packet(u8::MAX, 42, "same payload"),
        ];
        let mut client = Client::new(ScriptedTransport::with_inbound_frames(frames));
        client.connected = true;

        assert!(matches!(
            client.next_event().await,
            Ok(Some(Event::MessagesWaiting))
        ));
        assert!(matches!(client.sync_next_message().await, Ok(Some(_))));
        assert_eq!(client.transport.outbound_frames().len(), 1);
    }

    #[tokio::test]
    async fn valid_push_during_sync_write_is_published_before_the_single_response() {
        let mut client = Client::new(SyncResponseTransport::new());
        client.connected = true;
        let mut events = client.subscribe();

        let returned = client
            .sync_next_message()
            .await
            .expect("sync")
            .expect("queued response");
        assert_eq!(returned.text, "queued response");
        assert_eq!(client.transport.writes, 1);

        assert!(matches!(client.sync_next_message().await, Ok(None)));
        assert_eq!(client.transport.writes, 2);
        assert!(matches!(events.try_recv(), Ok(Event::MessagesWaiting)));
        let Event::Message(event) = events.try_recv().expect("sync response event") else {
            panic!("sync response was not published as a message");
        };
        assert_eq!(event.observation_id, returned.observation_id);
    }

    #[tokio::test]
    async fn preexisting_valid_push_is_published_before_one_fresh_sync_response() {
        let mut client = Client::new(SyncResponseTransport::with_preexisting_notification());
        client.connected = true;
        let mut events = client.subscribe();

        let returned = client
            .sync_next_message()
            .await
            .expect("sync")
            .expect("queued response");
        assert_eq!(returned.text, "queued response");
        assert_eq!(client.transport.writes, 1);

        assert!(matches!(events.try_recv(), Ok(Event::MessagesWaiting)));
        let Event::Message(queued) = events.try_recv().expect("sync response event") else {
            panic!("sync response was not published as a message");
        };
        assert_eq!(queued.observation_id, returned.observation_id);
    }

    #[tokio::test]
    async fn cancelled_sync_response_is_reconciled_before_any_second_command() {
        let mut client = Client::new(SyncResponseTransport::delayed());
        client.connected = true;

        assert!(
            tokio::time::timeout(Duration::from_millis(10), client.sync_next_message())
                .await
                .is_err(),
            "outer timeout must cancel the in-flight sync"
        );
        assert_eq!(client.transport.writes, 1);
        assert!(matches!(
            client.query_device_info().await,
            Err(CoreError::ProtocolInvariant(
                "a prior inbox synchronization response must be reconciled before another command"
            ))
        ));
        assert_eq!(client.transport.writes, 1);

        client
            .transport
            .inbound
            .push_back(contact_message_packet(u8::MAX, 43, "late response"));
        let reconciled = client
            .sync_next_message()
            .await
            .expect("reconcile sync")
            .expect("late response");
        assert_eq!(reconciled.text, "late response");
        assert_eq!(client.transport.writes, 1);
    }

    #[tokio::test]
    async fn login_correlates_prefix_without_tracking_or_leaking_password() {
        let key = [0x42; 32];
        let expected_prefix = [0x42; 6];
        let other_prefix = [0x24; 6];
        let frames = [
            message_sent_packet([1, 2, 3, 4], 10),
            login_success_packet(other_prefix, 1),
            login_success_packet(expected_prefix, 3),
        ];
        let mut client = Client::new(ScriptedTransport::with_inbound_frames(frames));
        client.connected = true;
        let mut events = client.subscribe();

        let session = match client.login(&key, "PASSWORD_SENTINEL").await {
            Ok(session) => session,
            Err(error) => panic!("login should succeed: {error}"),
        };
        assert_eq!(session.pubkey_prefix, expected_prefix);
        assert_eq!(session.permissions, 3);
        assert!(client.pending_ack_codes().is_empty());
        assert!(matches!(
            events.try_recv(),
            Ok(Event::LoginSucceeded(LoginSession { pubkey_prefix, .. }))
                if pubkey_prefix == other_prefix
        ));

        let outbound = client.transport.drain_outbound();
        assert_eq!(outbound.len(), 1);
        assert_eq!(outbound[0], built_login(&key, "PASSWORD_SENTINEL"));
        assert!(!format!("{session:?}").contains("424242"));
    }

    #[tokio::test]
    async fn matching_login_failure_is_typed_and_not_retried() {
        let key = [0x33; 32];
        let mut failed = vec![PacketCode::LoginFailed.to_u8(), 0];
        failed.extend_from_slice(&key[..6]);
        let frames = [message_sent_packet([9, 8, 7, 6], 10), failed];
        let mut client = Client::new(ScriptedTransport::with_inbound_frames(frames));
        client.connected = true;

        assert!(matches!(
            client.login(&key, "incorrect").await,
            Err(CoreError::AuthenticationFailed)
        ));
        assert_eq!(client.transport.outbound_frames().len(), 1);
        assert!(client.pending_ack_codes().is_empty());
    }

    #[tokio::test]
    async fn binary_request_waits_for_exact_tag_and_publishes_unmatched_response() {
        let key = [0x55; 32];
        let expected_tag = [1, 3, 3, 7];
        let frames = [
            message_sent_packet(expected_tag, 10),
            binary_response_packet([9, 9, 9, 9], b"other"),
            binary_response_packet(expected_tag, b"matched"),
        ];
        let mut client = Client::new(ScriptedTransport::with_inbound_frames(frames));
        client.connected = true;
        let mut events = client.subscribe();

        let response = match client.binary_request(&key, 7, b"request").await {
            Ok(response) => response,
            Err(error) => panic!("binary request should succeed: {error}"),
        };
        assert_eq!(response.tag, expected_tag);
        assert_eq!(response.payload, b"matched");
        assert!(matches!(
            events.try_recv(),
            Ok(Event::BinaryResponse(BinaryResponse { tag, payload }))
                if tag == [9, 9, 9, 9] && payload == b"other"
        ));

        let outbound = client.transport.drain_outbound();
        assert_eq!(outbound.len(), 1);
        assert_eq!(
            outbound[0],
            Command::send_binary_request(&key, 7, b"request")
                .expect("test request should build")
                .encode()
        );
    }

    #[tokio::test]
    async fn node_discovery_returns_after_ok_and_correlates_control_event() {
        let tag = 0x1234_5678_u32;
        let mut control = vec![
            PacketCode::ControlData.to_u8(),
            (-4_i8).to_le_bytes()[0],
            (-90_i8).to_le_bytes()[0],
            0,
            0x92,
            8,
        ];
        control.extend_from_slice(&tag.to_le_bytes());
        control.extend_from_slice(&[0x44; 8]);
        let frames = [vec![PacketCode::Ok.to_u8()], control];
        let mut client = Client::new(ScriptedTransport::with_inbound_frames(frames));
        client.connected = true;

        match client
            .send_node_discovery(0x04, true, tag, Some(0xa1b2_c3d4))
            .await
        {
            Ok(()) => {}
            Err(error) => panic!("node discovery should be accepted: {error}"),
        }
        assert_eq!(
            client.transport.outbound_frames(),
            vec![vec![
                0x37, 0x81, 0x04, 0x78, 0x56, 0x34, 0x12, 0xd4, 0xc3, 0xb2, 0xa1,
            ]]
        );

        let event = match client.next_event().await {
            Ok(Some(event)) => event,
            Ok(None) => panic!("control data did not produce an event"),
            Err(error) => panic!("control event read failed: {error}"),
        };
        let Event::ControlData(data) = event else {
            panic!("unexpected node-discovery event: {event:?}");
        };
        let response = match data.node_discovery_response() {
            Ok(Some(response)) => response,
            Ok(None) => panic!("control event was not node discovery"),
            Err(error) => panic!("node-discovery event was malformed: {error}"),
        };
        assert_eq!(response.tag, tag);
        assert_eq!(response.node_type, 2);
        assert_eq!(response.public_key, [0x44; 8]);
    }

    #[tokio::test]
    async fn anonymous_request_preserves_direct_reply_route_and_correlates_exact_tag() {
        let key = [0x66; 32];
        let expected_tag = [4, 3, 2, 1];
        let frames = [
            message_sent_packet(expected_tag, 10),
            binary_response_packet([8, 8, 8, 8], b"unmatched"),
            binary_response_packet(expected_tag, b"owner"),
        ];
        let mut client = Client::new(ScriptedTransport::with_inbound_frames(frames));
        client.connected = true;
        let mut events = client.subscribe();
        let reply_path = Path::try_from_bytes(&[0x10, 0x20]).expect("test path should build");

        let response = client
            .anonymous_request(
                &key,
                2,
                ContactRoute::Path {
                    hash_mode: 0,
                    hop_count: 2,
                },
                &reply_path,
            )
            .await
            .expect("anonymous request should succeed");
        assert_eq!(response.tag, expected_tag);
        assert_eq!(response.payload, b"owner");
        assert!(matches!(
            events.try_recv(),
            Ok(Event::BinaryResponse(BinaryResponse { tag, payload }))
                if tag == [8, 8, 8, 8] && payload == b"unmatched"
        ));

        let outbound = client.transport.drain_outbound();
        assert_eq!(outbound.len(), 1);
        assert_eq!(
            outbound[0],
            Command::send_anonymous_request(
                &key,
                2,
                ContactRoute::Path {
                    hash_mode: 0,
                    hop_count: 2,
                },
                &reply_path,
            )
            .expect("test command should build")
            .encode()
        );
    }

    #[tokio::test]
    async fn signing_chunks_once_and_private_key_feature_failures_are_typed() {
        let mut signature_packet = vec![PacketCode::Signature.to_u8()];
        signature_packet.extend_from_slice(&[0x5a; 64]);
        let mut sign_start = vec![PacketCode::SignStart.to_u8(), 0];
        sign_start.extend_from_slice(&300_u32.to_le_bytes());
        let frames = [
            sign_start,
            vec![PacketCode::Ok.to_u8()],
            vec![PacketCode::Ok.to_u8()],
            signature_packet,
        ];
        let mut client = Client::new(ScriptedTransport::with_inbound_frames(frames));
        client.connected = true;
        let data = vec![0xa5; SIGNING_CHUNK_BYTES + 1];
        let signature = match client.sign(&data).await {
            Ok(signature) => signature,
            Err(error) => panic!("signing should succeed: {error}"),
        };
        assert_eq!(signature.as_bytes(), &[0x5a; 64]);
        let outbound = client.transport.drain_outbound();
        assert_eq!(outbound.len(), 4);
        assert_eq!(outbound[0], Command::sign_start().encode());
        assert_eq!(outbound[1].len(), SIGNING_CHUNK_BYTES + 1);
        assert_eq!(outbound[2].len(), 2);
        assert_eq!(outbound[3], Command::sign_finish().encode());

        let mut export_client = Client::new(ScriptedTransport::with_inbound_frames([vec![
            PacketCode::Disabled.to_u8(),
        ]]));
        export_client.connected = true;
        assert!(matches!(
            export_client.export_private_key().await,
            Err(CoreError::FeatureDisabled {
                feature: "private-key export"
            })
        ));
        assert_eq!(export_client.transport.outbound_frames().len(), 1);
    }

    #[tokio::test]
    async fn reboot_and_factory_reset_write_once_then_invalidate_the_session() {
        for (command, reboot) in [(Command::reboot(), true), (Command::factory_reset(), false)] {
            let mut client =
                Client::new(ScriptedTransport::with_inbound_frames::<[Vec<u8>; 0], _>([]));
            client.connected = true;
            let mut events = client.subscribe();
            let result = if reboot {
                client.reboot().await
            } else {
                client.factory_reset().await
            };
            assert!(result.is_ok());
            assert!(!client.is_connected());
            assert_eq!(client.transport.outbound_frames(), vec![command.encode()]);
            assert!(matches!(events.try_recv(), Ok(Event::Disconnected)));
        }
    }

    fn built_login(key: &[u8; 32], password: &str) -> Vec<u8> {
        match Command::send_login(key, password) {
            Ok(command) => command.encode(),
            Err(error) => panic!("login command should build: {error}"),
        }
    }

    #[test]
    fn completed_ack_retention_is_bounded_and_evicts_oldest() {
        let mut client = Client::new(ScriptedTransport::new());
        for index in 0..=CLIENT_COMPLETED_ACK_CAPACITY {
            let code = u32::try_from(index).unwrap_or(u32::MAX).to_le_bytes();
            client.remember_completed_ack(Ack {
                code,
                trip_time_ms: None,
            });
        }

        assert_eq!(client.completed_acks.len(), CLIENT_COMPLETED_ACK_CAPACITY);
        assert!(!client.completed_acks.contains_key(&0_u32.to_le_bytes()));
        assert!(
            client.completed_acks.contains_key(
                &u32::try_from(CLIENT_COMPLETED_ACK_CAPACITY)
                    .unwrap_or(u32::MAX)
                    .to_le_bytes()
            )
        );
        assert_eq!(
            client.completed_ack_order.front().copied(),
            Some(1_u32.to_le_bytes())
        );
    }

    #[test]
    fn pending_ack_retention_evicts_oldest_and_deduplicates_codes() {
        let mut client = Client::new(ScriptedTransport::new());
        for index in 0..=CLIENT_PENDING_ACK_CAPACITY {
            let ack_code = u32::try_from(index).unwrap_or(u32::MAX).to_le_bytes();
            client.remember_pending_ack(CommandTracking {
                ack_code,
                timeout_ms: u32::try_from(index).unwrap_or(u32::MAX),
            });
        }

        assert_eq!(client.pending_acks.len(), CLIENT_PENDING_ACK_CAPACITY);
        assert!(!client.pending_acks.contains_key(&0_u32.to_le_bytes()));
        assert_eq!(
            client.pending_ack_order.front().copied(),
            Some(1_u32.to_le_bytes())
        );
        assert_eq!(
            client.pending_ack_codes(),
            client.pending_ack_order.iter().copied().collect::<Vec<_>>()
        );

        let duplicate_code = 100_u32.to_le_bytes();
        client.remember_pending_ack(CommandTracking {
            ack_code: duplicate_code,
            timeout_ms: 999,
        });
        assert_eq!(client.pending_acks.len(), CLIENT_PENDING_ACK_CAPACITY);
        assert_eq!(
            client
                .pending_ack_order
                .iter()
                .filter(|code| **code == duplicate_code)
                .count(),
            1
        );
        assert_eq!(
            client.pending_ack_order.back().copied(),
            Some(duplicate_code)
        );
        assert_eq!(
            client
                .pending_acks
                .get(&duplicate_code)
                .map(|tracking| tracking.timeout_ms),
            Some(999)
        );

        client.update_tracking(&Packet::Ack(Ack {
            code: duplicate_code,
            trip_time_ms: None,
        }));
        assert!(!client.pending_acks.contains_key(&duplicate_code));
        assert!(!client.pending_ack_order.contains(&duplicate_code));
    }

    #[tokio::test]
    async fn pending_ack_order_is_cleared_on_timeout_and_disconnect() {
        let ack_code = [9, 8, 7, 6];
        let mut timeout_client = Client::new(BlockingReadTransport);
        timeout_client.connected = true;
        timeout_client.remember_pending_ack(CommandTracking {
            ack_code,
            timeout_ms: 1,
        });
        assert!(matches!(
            timeout_client
                .wait_for_ack(ack_code, Some(Duration::from_millis(1)))
                .await,
            Err(CoreError::Timeout)
        ));
        assert!(timeout_client.pending_acks.is_empty());
        assert!(timeout_client.pending_ack_order.is_empty());

        let mut disconnect_client = Client::new(ScriptedTransport::new());
        disconnect_client.connected = true;
        disconnect_client.remember_pending_ack(CommandTracking {
            ack_code,
            timeout_ms: 1,
        });
        assert!(disconnect_client.disconnect().await.is_ok());
        assert!(disconnect_client.pending_acks.is_empty());
        assert!(disconnect_client.pending_ack_order.is_empty());
    }

    #[tokio::test]
    async fn terminal_io_errors_disconnect_and_publish_once() {
        let terminal_kinds = [
            io::ErrorKind::ConnectionAborted,
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::BrokenPipe,
            io::ErrorKind::NotConnected,
            io::ErrorKind::UnexpectedEof,
        ];

        for kind in terminal_kinds {
            let mut client = Client::new(ReadErrorTransport { kind });
            client.connected = true;
            let mut events = client.subscribe();

            match client.next_event().await {
                Err(CoreError::Transport(TransportError::Io(error))) => {
                    assert_eq!(error.kind(), kind);
                }
                other => panic!("unexpected terminal read result: {other:?}"),
            }
            assert!(!client.is_connected());
            match events.try_recv() {
                Ok(Event::Disconnected) => {}
                other => panic!("expected one disconnected event, got {other:?}"),
            }

            assert!(matches!(
                client.next_event().await,
                Err(CoreError::Disconnected)
            ));
            assert!(matches!(
                events.try_recv(),
                Err(broadcast::error::TryRecvError::Empty)
            ));
        }
    }

    #[tokio::test]
    async fn invalid_data_io_error_remains_connected_for_resynchronization() {
        let mut client = Client::new(ReadErrorTransport {
            kind: io::ErrorKind::InvalidData,
        });
        client.connected = true;
        let mut events = client.subscribe();

        match client.next_event().await {
            Err(CoreError::Transport(TransportError::Io(error))) => {
                assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            }
            other => panic!("unexpected recoverable read result: {other:?}"),
        }
        assert!(client.is_connected());
        assert!(matches!(
            events.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
    }
}

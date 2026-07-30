use std::time::Duration;

use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time::{sleep, timeout};
use zeroize::Zeroizing;

use crate::client::Client;
use crate::domain::{
    Ack, AdvertPath, AutoAddConfig, BatteryInfo, BinaryResponse, ChannelInfo, CommandTracking,
    Contact, ContactRoute, ContactUri, CustomVariables, DefaultFloodScope, DeviceInfo, DeviceStats,
    Event, FloodScope, FrequencyRange, LoginSession, Message, Path, PathDiscovery,
    PrivateKeyMaterial, RadioParams, RemoteStatus, SelfInfo, Signature, StatsType,
    TelemetryResponse, TuningParams,
};
use crate::error::CoreError;
use crate::transport::{ReconnectableTransport, Transport};

/// Default number of operations that can wait in a managed client's command queue.
pub const MANAGED_CLIENT_COMMAND_CAPACITY: usize = 32;

const IDLE_ERROR_BACKOFF: Duration = Duration::from_millis(25);

type Reply<T> = oneshot::Sender<Result<T, CoreError>>;

/// Cloneable handle to a [`Client`] owned by one asynchronous actor task.
///
/// All protocol operations pass through a bounded queue and execute serially because companion
/// responses do not carry sequence identifiers. Once an operation has entered the queue, dropping
/// its caller only drops the reply receiver: the actor still finishes the device operation and
/// never implicitly retries or replays a mutating command.
///
/// While connected and otherwise idle, transport or parse errors that do not prove a disconnect
/// leave the session available for an explicit caller decision. The actor rate-limits the next
/// idle read after such an error to avoid spinning; a queued command preempts that backoff. Clean
/// EOF and terminal transport errors (including `NotConnected`, `Closed`, broken pipes, resets,
/// aborted connections, and unexpected EOF) instead transition to disconnected state.
#[derive(Clone)]
pub struct ManagedClient {
    command_tx: mpsc::Sender<ActorCommand>,
    event_tx: broadcast::Sender<Event>,
}

impl ManagedClient {
    /// Starts a managed client actor with the default bounded command capacity.
    ///
    /// # Panics
    ///
    /// Panics when called outside a Tokio runtime.
    #[must_use]
    pub fn spawn<T>(client: Client<T>) -> Self
    where
        T: ReconnectableTransport + Send + 'static,
    {
        Self::spawn_inner(client, MANAGED_CLIENT_COMMAND_CAPACITY)
    }

    /// Starts a managed client actor with an explicit bounded command capacity.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidArgument`] when `command_capacity` is zero.
    ///
    /// # Panics
    ///
    /// Panics when called outside a Tokio runtime.
    pub fn spawn_with_capacity<T>(
        client: Client<T>,
        command_capacity: usize,
    ) -> Result<Self, CoreError>
    where
        T: ReconnectableTransport + Send + 'static,
    {
        if command_capacity == 0 {
            return Err(CoreError::InvalidArgument {
                field: "command_capacity",
                message: "must be greater than zero".to_owned(),
            });
        }
        Ok(Self::spawn_inner(client, command_capacity))
    }

    fn spawn_inner<T>(client: Client<T>, command_capacity: usize) -> Self
    where
        T: ReconnectableTransport + Send + 'static,
    {
        let event_tx = client.event_sender();
        let (command_tx, command_rx) = mpsc::channel(command_capacity);
        std::mem::drop(tokio::spawn(run_actor(client, command_rx)));
        Self {
            command_tx,
            event_tx,
        }
    }

    /// Returns a receiver for the client's bounded broadcast event stream.
    ///
    /// If this receiver falls behind far enough for retained events to be overwritten,
    /// [`broadcast::Receiver::recv`] returns [`broadcast::error::RecvError::Lagged`]. The receiver
    /// then resumes with the oldest event still in the buffer. One slow subscriber never blocks
    /// the actor or another subscriber.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.event_tx.subscribe()
    }

    /// Connects the transport and performs the `APP_START` handshake.
    ///
    /// # Errors
    ///
    /// Returns the underlying client error, or [`CoreError::ActorStopped`] if the actor has exited.
    pub async fn connect(&self) -> Result<SelfInfo, CoreError> {
        self.request(ActorCommand::Connect).await
    }

    /// Explicitly disconnects the active transport.
    ///
    /// # Errors
    ///
    /// Returns the underlying client error, or [`CoreError::ActorStopped`] if the actor has exited.
    pub async fn disconnect(&self) -> Result<(), CoreError> {
        self.request(ActorCommand::Disconnect).await
    }

    /// Explicitly reconnects the transport and performs a fresh `APP_START` handshake.
    ///
    /// Previously attempted mutating commands are never replayed.
    ///
    /// # Errors
    ///
    /// Returns the underlying client error, or [`CoreError::ActorStopped`] if the actor has exited.
    pub async fn reconnect(&self) -> Result<SelfInfo, CoreError> {
        self.request(ActorCommand::Reconnect).await
    }

    /// Queries firmware and device metadata.
    ///
    /// # Errors
    ///
    /// Returns the underlying client error, or [`CoreError::ActorStopped`] if the actor has exited.
    pub async fn query_device_info(&self) -> Result<DeviceInfo, CoreError> {
        self.request(ActorCommand::QueryDeviceInfo).await
    }

    /// Lists contacts, optionally starting from a firmware last-modified marker.
    ///
    /// # Errors
    ///
    /// Returns the underlying client error, or [`CoreError::ActorStopped`] if the actor has exited.
    pub async fn list_contacts(&self, lastmod: Option<u32>) -> Result<Vec<Contact>, CoreError> {
        self.request(|reply| ActorCommand::ListContacts { lastmod, reply })
            .await
    }

    /// Queries companion time.
    ///
    /// # Errors
    ///
    /// Returns the underlying client error, or [`CoreError::ActorStopped`] if the actor has exited.
    pub async fn get_time(&self) -> Result<u32, CoreError> {
        self.request(ActorCommand::GetTime).await
    }

    /// Updates companion time without retry or replay.
    ///
    /// # Errors
    ///
    /// Returns the underlying client error, or [`CoreError::ActorStopped`] if the actor has exited.
    pub async fn set_time(&self, value: u32) -> Result<(), CoreError> {
        self.request(|reply| ActorCommand::SetTime { value, reply })
            .await
    }

    /// Queries battery voltage and storage telemetry.
    ///
    /// # Errors
    ///
    /// Returns the underlying client error, or [`CoreError::ActorStopped`] if the actor has exited.
    pub async fn get_battery(&self) -> Result<BatteryInfo, CoreError> {
        self.request(ActorCommand::GetBattery).await
    }

    /// Sends this device's self-advertisement.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or actor lifecycle errors from the underlying
    /// operation.
    pub async fn send_self_advert(&self, flood: bool) -> Result<(), CoreError> {
        self.request(|reply| ActorCommand::SendSelfAdvert { flood, reply })
            .await
    }

    /// Updates the advertised device name.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or actor lifecycle errors.
    pub async fn set_advert_name(&self, name: &str) -> Result<(), CoreError> {
        let name = name.to_owned();
        self.request(|reply| ActorCommand::SetAdvertName { name, reply })
            .await
    }

    /// Updates advertised coordinates.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or actor lifecycle errors.
    pub async fn set_coordinates(&self, latitude: f64, longitude: f64) -> Result<(), CoreError> {
        self.request(|reply| ActorCommand::SetCoordinates {
            latitude,
            longitude,
            reply,
        })
        .await
    }

    /// Updates radio transmit power.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or actor lifecycle errors.
    pub async fn set_tx_power(&self, power_dbm: i8) -> Result<(), CoreError> {
        self.request(|reply| ActorCommand::SetTxPower { power_dbm, reply })
            .await
    }

    /// Updates radio parameters.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or actor lifecycle errors.
    pub async fn set_radio_params(&self, params: &RadioParams) -> Result<(), CoreError> {
        let params = params.clone();
        self.request(|reply| ActorCommand::SetRadioParams { params, reply })
            .await
    }

    /// Updates packet-timing parameters.
    ///
    /// # Errors
    ///
    /// Returns the underlying client error, or [`CoreError::ActorStopped`] if the actor has exited.
    pub async fn set_tuning(&self, params: TuningParams) -> Result<(), CoreError> {
        self.request(|reply| ActorCommand::SetTuning { params, reply })
            .await
    }

    /// Queries packet-timing parameters.
    ///
    /// # Errors
    ///
    /// Returns the underlying client error, or [`CoreError::ActorStopped`] if the actor has exited.
    pub async fn get_tuning(&self) -> Result<TuningParams, CoreError> {
        self.request(ActorCommand::GetTuning).await
    }

    /// Clears a contact's cached route.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or actor lifecycle errors.
    pub async fn reset_path(&self, public_key: &[u8]) -> Result<(), CoreError> {
        let public_key = public_key.to_vec();
        self.request(|reply| ActorCommand::ResetPath { public_key, reply })
            .await
    }

    /// Replaces one contact's mutable metadata using a complete fresh record.
    ///
    /// # Errors
    ///
    /// Returns the underlying client error, or [`CoreError::ActorStopped`] if the actor exits.
    pub async fn update_contact(&self, contact: &Contact) -> Result<(), CoreError> {
        let contact = contact.clone();
        self.request(|reply| ActorCommand::UpdateContact { contact, reply })
            .await
    }

    /// Shares a contact over zero-hop.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or actor lifecycle errors.
    pub async fn share_contact(&self, public_key: &[u8]) -> Result<(), CoreError> {
        let public_key = public_key.to_vec();
        self.request(|reply| ActorCommand::ShareContact { public_key, reply })
            .await
    }

    /// Exports this device or a stored contact's card.
    ///
    /// # Errors
    ///
    /// Returns the underlying client error, or [`CoreError::ActorStopped`] if the actor has exited.
    pub async fn export_contact(&self, public_key: Option<&[u8]>) -> Result<ContactUri, CoreError> {
        let public_key = public_key.map(<[u8]>::to_vec);
        self.request(|reply| ActorCommand::ExportContact { public_key, reply })
            .await
    }

    /// Imports a validated contact card.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or actor lifecycle errors.
    pub async fn import_contact(&self, card: &[u8]) -> Result<(), CoreError> {
        let card = card.to_vec();
        self.request(|reply| ActorCommand::ImportContact { card, reply })
            .await
    }

    /// Queries one contact.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or actor lifecycle errors.
    pub async fn get_contact(&self, public_key: &[u8]) -> Result<Contact, CoreError> {
        let public_key = public_key.to_vec();
        self.request(|reply| ActorCommand::GetContact { public_key, reply })
            .await
    }

    /// Queries the latest advert path for one contact.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or actor lifecycle errors.
    pub async fn get_advert_path(&self, public_key: &[u8]) -> Result<AdvertPath, CoreError> {
        let public_key = public_key.to_vec();
        self.request(|reply| ActorCommand::GetAdvertPath { public_key, reply })
            .await
    }

    /// Updates auto-add configuration.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or actor lifecycle errors.
    pub async fn set_auto_add_config(&self, config: AutoAddConfig) -> Result<(), CoreError> {
        self.request(|reply| ActorCommand::SetAutoAddConfig { config, reply })
            .await
    }

    /// Queries auto-add configuration.
    ///
    /// # Errors
    ///
    /// Returns the underlying client error, or [`CoreError::ActorStopped`] if the actor has exited.
    pub async fn get_auto_add_config(&self) -> Result<AutoAddConfig, CoreError> {
        self.request(ActorCommand::GetAutoAddConfig).await
    }

    /// Queries strict firmware custom variables.
    ///
    /// # Errors
    ///
    /// Returns the underlying client error, or [`CoreError::ActorStopped`] if the actor has exited.
    pub async fn get_custom_vars(&self) -> Result<CustomVariables, CoreError> {
        self.request(ActorCommand::GetCustomVars).await
    }

    /// Updates one custom variable.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or actor lifecycle errors.
    pub async fn set_custom_var(&self, key: &str, value: &str) -> Result<(), CoreError> {
        let key = key.to_owned();
        let value = value.to_owned();
        self.request(|reply| ActorCommand::SetCustomVar { key, value, reply })
            .await
    }

    /// Queries one statistic family.
    ///
    /// # Errors
    ///
    /// Returns the underlying client error, or [`CoreError::ActorStopped`] if the actor has exited.
    pub async fn get_stats(&self, stats_type: StatsType) -> Result<DeviceStats, CoreError> {
        self.request(|reply| ActorCommand::GetStats { stats_type, reply })
            .await
    }

    /// Queries allowed repeat frequencies.
    ///
    /// # Errors
    ///
    /// Returns the underlying client error, or [`CoreError::ActorStopped`] if the actor has exited.
    pub async fn get_allowed_repeat_frequencies(&self) -> Result<Vec<FrequencyRange>, CoreError> {
        self.request(ActorCommand::GetAllowedRepeatFrequencies)
            .await
    }

    /// Sets the path-hash mode.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or actor lifecycle errors.
    pub async fn set_path_hash_mode(&self, mode: u8) -> Result<(), CoreError> {
        self.request(|reply| ActorCommand::SetPathHashMode { mode, reply })
            .await
    }

    /// Queries path-hash mode.
    ///
    /// # Errors
    ///
    /// Returns the underlying client error, or [`CoreError::ActorStopped`] if the actor has exited.
    pub async fn get_path_hash_mode(&self) -> Result<u8, CoreError> {
        self.request(ActorCommand::GetPathHashMode).await
    }

    /// Updates the active flood scope.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or actor lifecycle errors.
    pub async fn set_flood_scope(&self, scope: &FloodScope) -> Result<(), CoreError> {
        let scope = scope.clone();
        self.request(|reply| ActorCommand::SetFloodScope { scope, reply })
            .await
    }

    /// Configures the default flood scope.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or actor lifecycle errors.
    pub async fn set_default_flood_scope(
        &self,
        name: &str,
        key: [u8; 16],
    ) -> Result<(), CoreError> {
        let name = name.to_owned();
        self.request(|reply| ActorCommand::SetDefaultFloodScope { name, key, reply })
            .await
    }

    /// Clears the configured default flood scope.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, parse, or actor lifecycle errors.
    pub async fn clear_default_flood_scope(&self) -> Result<(), CoreError> {
        self.request(ActorCommand::ClearDefaultFloodScope).await
    }

    /// Queries the configured default flood scope.
    ///
    /// # Errors
    ///
    /// Returns the underlying client error, or [`CoreError::ActorStopped`] if the actor has exited.
    pub async fn get_default_flood_scope(&self) -> Result<DefaultFloodScope, CoreError> {
        self.request(ActorCommand::GetDefaultFloodScope).await
    }

    /// Queries one channel slot.
    ///
    /// # Errors
    ///
    /// Returns the underlying client error, or [`CoreError::ActorStopped`] if the actor has exited.
    pub async fn get_channel(&self, idx: u8) -> Result<ChannelInfo, CoreError> {
        self.request(|reply| ActorCommand::GetChannel { idx, reply })
            .await
    }

    /// Updates one channel slot without replay.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, protocol, or actor lifecycle errors.
    pub async fn set_channel(
        &self,
        idx: u8,
        name: &str,
        secret: [u8; 16],
    ) -> Result<(), CoreError> {
        let name = name.to_owned();
        self.request(|reply| ActorCommand::SetChannel {
            idx,
            name,
            secret,
            reply,
        })
        .await
    }

    /// Clears one channel slot without replay.
    ///
    /// # Errors
    ///
    /// Returns transport, protocol, or actor lifecycle errors.
    pub async fn clear_channel(&self, idx: u8) -> Result<(), CoreError> {
        self.request(|reply| ActorCommand::ClearChannel { idx, reply })
            .await
    }

    /// Removes one exact contact without replay.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, protocol, or actor lifecycle errors.
    pub async fn remove_contact(&self, public_key: &[u8]) -> Result<(), CoreError> {
        let public_key = public_key.to_vec();
        self.request(|reply| ActorCommand::RemoveContact { public_key, reply })
            .await
    }

    /// Authenticates to one exact remote contact.
    ///
    /// The queued password is zeroized when the actor command is consumed or dropped.
    ///
    /// # Errors
    ///
    /// Returns validation, authentication, transport, protocol, timeout, or actor errors.
    pub async fn login(
        &self,
        public_key: &[u8],
        password: &str,
    ) -> Result<LoginSession, CoreError> {
        let public_key = public_key.to_vec();
        let password = Zeroizing::new(password.to_owned());
        self.request(|reply| ActorCommand::Login {
            public_key,
            password,
            reply,
        })
        .await
    }

    /// Ends one remote authenticated session.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, protocol, or actor lifecycle errors.
    pub async fn logout(&self, public_key: &[u8]) -> Result<(), CoreError> {
        let public_key = public_key.to_vec();
        self.request(|reply| ActorCommand::Logout { public_key, reply })
            .await
    }

    /// Checks whether one remote authenticated session exists.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, protocol, or actor lifecycle errors.
    pub async fn has_connection(&self, public_key: &[u8]) -> Result<bool, CoreError> {
        let public_key = public_key.to_vec();
        self.request(|reply| ActorCommand::HasConnection { public_key, reply })
            .await
    }

    /// Requests matching status from one remote contact.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, protocol, timeout, or actor lifecycle errors.
    pub async fn remote_status(&self, public_key: &[u8]) -> Result<RemoteStatus, CoreError> {
        let public_key = public_key.to_vec();
        self.request(|reply| ActorCommand::RemoteStatus { public_key, reply })
            .await
    }

    /// Queries local telemetry bytes.
    ///
    /// # Errors
    ///
    /// Returns transport, protocol, timeout, or actor lifecycle errors.
    pub async fn get_self_telemetry(&self) -> Result<TelemetryResponse, CoreError> {
        self.request(ActorCommand::GetSelfTelemetry).await
    }

    /// Sends a correlated remote binary request.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, protocol, timeout, or actor lifecycle errors.
    pub async fn binary_request(
        &self,
        public_key: &[u8],
        request_type: u8,
        data: &[u8],
    ) -> Result<BinaryResponse, CoreError> {
        let public_key = public_key.to_vec();
        let data = data.to_vec();
        self.request(|reply| ActorCommand::BinaryRequest {
            public_key,
            request_type,
            data,
            reply,
        })
        .await
    }

    /// Sends one anonymous metadata request with an explicit direct reply route.
    ///
    /// # Errors
    ///
    /// Returns the underlying client error, or [`CoreError::ActorStopped`] if the actor exits.
    pub async fn anonymous_request(
        &self,
        public_key: &[u8],
        request_type: u8,
        reply_route: ContactRoute,
        reply_path: &Path,
    ) -> Result<BinaryResponse, CoreError> {
        let public_key = public_key.to_vec();
        let reply_path = reply_path.clone();
        self.request(|reply| ActorCommand::AnonymousRequest {
            public_key,
            request_type,
            reply_route,
            reply_path,
            reply,
        })
        .await
    }

    /// Sends one correlated node-discovery request and returns after companion acceptance.
    ///
    /// Matching responses are subsequently published as [`Event::ControlData`] by the actor's
    /// idle reader.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, protocol, or actor lifecycle errors.
    pub async fn send_node_discovery(
        &self,
        filter: u8,
        prefix_only: bool,
        tag: u32,
        since: Option<u32>,
    ) -> Result<(), CoreError> {
        self.request(|reply| ActorCommand::SendNodeDiscovery {
            filter,
            prefix_only,
            tag,
            since,
            reply,
        })
        .await
    }

    /// Discovers paths for one exact contact.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, protocol, timeout, or actor lifecycle errors.
    pub async fn discover_path(&self, public_key: &[u8]) -> Result<PathDiscovery, CoreError> {
        let public_key = public_key.to_vec();
        self.request(|reply| ActorCommand::DiscoverPath { public_key, reply })
            .await
    }

    /// Signs bounded bytes with the companion identity.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, protocol, timeout, or actor lifecycle errors.
    pub async fn sign(&self, data: &[u8]) -> Result<Signature, CoreError> {
        let data = data.to_vec();
        self.request(|reply| ActorCommand::Sign { data, reply })
            .await
    }

    /// Exports zeroizing private-key material if firmware enables it.
    ///
    /// # Errors
    ///
    /// Returns transport, protocol, disabled-feature, timeout, or actor lifecycle errors.
    pub async fn export_private_key(&self) -> Result<PrivateKeyMaterial, CoreError> {
        self.request(ActorCommand::ExportPrivateKey).await
    }

    /// Imports zeroizing private-key material if firmware enables it.
    ///
    /// # Errors
    ///
    /// Returns transport, protocol, disabled-feature, timeout, or actor lifecycle errors.
    pub async fn import_private_key(&self, key: &PrivateKeyMaterial) -> Result<(), CoreError> {
        let key = key.clone();
        self.request(|reply| ActorCommand::ImportPrivateKey { key, reply })
            .await
    }

    /// Updates the local pairing PIN.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, protocol, timeout, or actor lifecycle errors.
    pub async fn set_device_pin(&self, pin: u32) -> Result<(), CoreError> {
        self.request(|reply| ActorCommand::SetDevicePin { pin, reply })
            .await
    }

    /// Requests a local reboot and invalidates the current session.
    ///
    /// # Errors
    ///
    /// Returns transport or actor lifecycle errors.
    pub async fn reboot(&self) -> Result<(), CoreError> {
        self.request(ActorCommand::Reboot).await
    }

    /// Requests a factory reset and invalidates the current session.
    ///
    /// # Errors
    ///
    /// Returns transport or actor lifecycle errors.
    pub async fn factory_reset(&self) -> Result<(), CoreError> {
        self.request(ActorCommand::FactoryReset).await
    }

    /// Sends direct text without implicit retry or replay.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, protocol, or actor lifecycle errors from the underlying
    /// operation. A transport write error can have an ambiguous device outcome and is never
    /// automatically resent.
    pub async fn send_direct_text(
        &self,
        destination_prefix: &[u8],
        attempt: u8,
        text: &str,
    ) -> Result<CommandTracking, CoreError> {
        let destination_prefix = destination_prefix.to_vec();
        let text = text.to_owned();
        self.request(|reply| ActorCommand::SendDirectText {
            destination_prefix,
            attempt,
            text,
            reply,
        })
        .await
    }

    /// Sends a direct command payload without implicit retry or replay.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, protocol, or actor lifecycle errors from the underlying
    /// operation. A transport write error can have an ambiguous device outcome and is never
    /// automatically resent.
    pub async fn send_direct_command(
        &self,
        destination_prefix: &[u8],
        attempt: u8,
        command_text: &str,
    ) -> Result<CommandTracking, CoreError> {
        let destination_prefix = destination_prefix.to_vec();
        let command_text = command_text.to_owned();
        self.request(|reply| ActorCommand::SendDirectCommand {
            destination_prefix,
            attempt,
            command_text,
            reply,
        })
        .await
    }

    /// Sends a channel message without implicit retry or replay.
    ///
    /// # Errors
    ///
    /// Returns validation, transport, protocol, or actor lifecycle errors from the underlying
    /// operation. A transport write error can have an ambiguous device outcome and is never
    /// automatically resent.
    pub async fn send_channel_message(
        &self,
        channel: u8,
        txt_type: u8,
        text: &str,
    ) -> Result<(), CoreError> {
        let text = text.to_owned();
        self.request(|reply| ActorCommand::SendChannelMessage {
            channel,
            txt_type,
            text,
            reply,
        })
        .await
    }

    /// Requests one queued message from firmware.
    ///
    /// # Errors
    ///
    /// Returns the underlying client error, or [`CoreError::ActorStopped`] if the actor has exited.
    pub async fn sync_next_message(&self) -> Result<Option<Message>, CoreError> {
        self.request(ActorCommand::SyncNextMessage).await
    }

    /// Waits for an ACK code already tracked by a direct send operation.
    ///
    /// An ACK received by the idle actor just before this command is retained for this wait.
    ///
    /// # Errors
    ///
    /// Returns the underlying client error, or [`CoreError::ActorStopped`] if the actor has exited.
    pub async fn wait_for_ack(
        &self,
        ack_code: [u8; 4],
        request_timeout: Option<Duration>,
    ) -> Result<Ack, CoreError> {
        self.request(|reply| ActorCommand::WaitForAck {
            ack_code,
            request_timeout,
            reply,
        })
        .await
    }

    /// Gracefully disconnects and stops the actor.
    ///
    /// The disconnect is bounded by the client's request timeout. All handle clones become
    /// unusable after shutdown completes. Dropping every handle also closes the command queue and
    /// asks the actor to perform the same bounded disconnect.
    ///
    /// # Errors
    ///
    /// Returns a disconnect error, [`CoreError::Timeout`] if bounded shutdown expires, or
    /// [`CoreError::ActorStopped`] if the actor had already exited.
    pub async fn shutdown(&self) -> Result<(), CoreError> {
        self.request(ActorCommand::Shutdown).await
    }

    async fn request<R, F>(&self, make_command: F) -> Result<R, CoreError>
    where
        R: Send + 'static,
        F: FnOnce(Reply<R>) -> ActorCommand,
    {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.command_tx
            .send(make_command(reply_tx))
            .await
            .map_err(|_| CoreError::ActorStopped)?;
        reply_rx.await.map_err(|_| CoreError::ActorStopped)?
    }
}

enum ActorCommand {
    Connect(Reply<SelfInfo>),
    Disconnect(Reply<()>),
    Reconnect(Reply<SelfInfo>),
    QueryDeviceInfo(Reply<DeviceInfo>),
    ListContacts {
        lastmod: Option<u32>,
        reply: Reply<Vec<Contact>>,
    },
    GetTime(Reply<u32>),
    SetTime {
        value: u32,
        reply: Reply<()>,
    },
    GetBattery(Reply<BatteryInfo>),
    SendSelfAdvert {
        flood: bool,
        reply: Reply<()>,
    },
    SetAdvertName {
        name: String,
        reply: Reply<()>,
    },
    SetCoordinates {
        latitude: f64,
        longitude: f64,
        reply: Reply<()>,
    },
    SetTxPower {
        power_dbm: i8,
        reply: Reply<()>,
    },
    SetRadioParams {
        params: RadioParams,
        reply: Reply<()>,
    },
    SetTuning {
        params: TuningParams,
        reply: Reply<()>,
    },
    GetTuning(Reply<TuningParams>),
    ResetPath {
        public_key: Vec<u8>,
        reply: Reply<()>,
    },
    UpdateContact {
        contact: Contact,
        reply: Reply<()>,
    },
    ShareContact {
        public_key: Vec<u8>,
        reply: Reply<()>,
    },
    ExportContact {
        public_key: Option<Vec<u8>>,
        reply: Reply<ContactUri>,
    },
    ImportContact {
        card: Vec<u8>,
        reply: Reply<()>,
    },
    GetContact {
        public_key: Vec<u8>,
        reply: Reply<Contact>,
    },
    GetAdvertPath {
        public_key: Vec<u8>,
        reply: Reply<AdvertPath>,
    },
    SetAutoAddConfig {
        config: AutoAddConfig,
        reply: Reply<()>,
    },
    GetAutoAddConfig(Reply<AutoAddConfig>),
    GetCustomVars(Reply<CustomVariables>),
    SetCustomVar {
        key: String,
        value: String,
        reply: Reply<()>,
    },
    GetStats {
        stats_type: StatsType,
        reply: Reply<DeviceStats>,
    },
    GetAllowedRepeatFrequencies(Reply<Vec<FrequencyRange>>),
    SetPathHashMode {
        mode: u8,
        reply: Reply<()>,
    },
    GetPathHashMode(Reply<u8>),
    SetFloodScope {
        scope: FloodScope,
        reply: Reply<()>,
    },
    SetDefaultFloodScope {
        name: String,
        key: [u8; 16],
        reply: Reply<()>,
    },
    ClearDefaultFloodScope(Reply<()>),
    GetDefaultFloodScope(Reply<DefaultFloodScope>),
    GetChannel {
        idx: u8,
        reply: Reply<ChannelInfo>,
    },
    SetChannel {
        idx: u8,
        name: String,
        secret: [u8; 16],
        reply: Reply<()>,
    },
    ClearChannel {
        idx: u8,
        reply: Reply<()>,
    },
    RemoveContact {
        public_key: Vec<u8>,
        reply: Reply<()>,
    },
    Login {
        public_key: Vec<u8>,
        password: Zeroizing<String>,
        reply: Reply<LoginSession>,
    },
    Logout {
        public_key: Vec<u8>,
        reply: Reply<()>,
    },
    HasConnection {
        public_key: Vec<u8>,
        reply: Reply<bool>,
    },
    RemoteStatus {
        public_key: Vec<u8>,
        reply: Reply<RemoteStatus>,
    },
    GetSelfTelemetry(Reply<TelemetryResponse>),
    BinaryRequest {
        public_key: Vec<u8>,
        request_type: u8,
        data: Vec<u8>,
        reply: Reply<BinaryResponse>,
    },
    AnonymousRequest {
        public_key: Vec<u8>,
        request_type: u8,
        reply_route: ContactRoute,
        reply_path: Path,
        reply: Reply<BinaryResponse>,
    },
    SendNodeDiscovery {
        filter: u8,
        prefix_only: bool,
        tag: u32,
        since: Option<u32>,
        reply: Reply<()>,
    },
    DiscoverPath {
        public_key: Vec<u8>,
        reply: Reply<PathDiscovery>,
    },
    Sign {
        data: Vec<u8>,
        reply: Reply<Signature>,
    },
    ExportPrivateKey(Reply<PrivateKeyMaterial>),
    ImportPrivateKey {
        key: PrivateKeyMaterial,
        reply: Reply<()>,
    },
    SetDevicePin {
        pin: u32,
        reply: Reply<()>,
    },
    Reboot(Reply<()>),
    FactoryReset(Reply<()>),
    SendDirectText {
        destination_prefix: Vec<u8>,
        attempt: u8,
        text: String,
        reply: Reply<CommandTracking>,
    },
    SendDirectCommand {
        destination_prefix: Vec<u8>,
        attempt: u8,
        command_text: String,
        reply: Reply<CommandTracking>,
    },
    SendChannelMessage {
        channel: u8,
        txt_type: u8,
        text: String,
        reply: Reply<()>,
    },
    SyncNextMessage(Reply<Option<Message>>),
    WaitForAck {
        ack_code: [u8; 4],
        request_timeout: Option<Duration>,
        reply: Reply<Ack>,
    },
    Shutdown(Reply<()>),
}

async fn run_actor<T>(mut client: Client<T>, mut command_rx: mpsc::Receiver<ActorCommand>)
where
    T: ReconnectableTransport + Send + 'static,
{
    let mut running = true;
    while running {
        if client.is_connected() {
            tokio::select! {
                biased;
                command = command_rx.recv() => {
                    running = handle_optional_command(&mut client, command).await;
                }
                event_result = client.next_event() => {
                    if event_result.is_err() && client.is_connected() {
                        tokio::select! {
                            biased;
                            command = command_rx.recv() => {
                                running = handle_optional_command(&mut client, command).await;
                            }
                            () = sleep(IDLE_ERROR_BACKOFF) => {}
                        }
                    }
                }
            }
        } else {
            running = handle_optional_command(&mut client, command_rx.recv().await).await;
        }
    }

    if client.is_connected() {
        let _ = bounded_disconnect(&mut client).await;
    }
}

async fn handle_optional_command<T>(client: &mut Client<T>, command: Option<ActorCommand>) -> bool
where
    T: ReconnectableTransport + Send,
{
    match command {
        Some(command) => handle_command(client, command).await,
        None => false,
    }
}

// Keeping the actor's exhaustive dispatch in one match makes the no-replay serialization boundary
// auditable even as the safe command surface grows.
#[allow(clippy::too_many_lines)]
async fn handle_command<T>(client: &mut Client<T>, command: ActorCommand) -> bool
where
    T: ReconnectableTransport + Send,
{
    match command {
        ActorCommand::Connect(reply) => {
            let _ = reply.send(client.connect().await);
        }
        ActorCommand::Disconnect(reply) => {
            let _ = reply.send(client.disconnect().await);
        }
        ActorCommand::Reconnect(reply) => {
            let _ = reply.send(client.reconnect().await);
        }
        ActorCommand::QueryDeviceInfo(reply) => {
            let _ = reply.send(client.query_device_info().await);
        }
        ActorCommand::ListContacts { lastmod, reply } => {
            let _ = reply.send(client.list_contacts(lastmod).await);
        }
        ActorCommand::SendDirectText {
            destination_prefix,
            attempt,
            text,
            reply,
        } => {
            let result = client
                .send_direct_text(&destination_prefix, attempt, &text)
                .await;
            let _ = reply.send(result);
        }
        ActorCommand::SendDirectCommand {
            destination_prefix,
            attempt,
            command_text,
            reply,
        } => {
            let result = client
                .send_direct_command(&destination_prefix, attempt, &command_text)
                .await;
            let _ = reply.send(result);
        }
        ActorCommand::SendChannelMessage {
            channel,
            txt_type,
            text,
            reply,
        } => {
            let result = client.send_channel_message(channel, txt_type, &text).await;
            let _ = reply.send(result);
        }
        ActorCommand::GetTime(reply) => {
            let _ = reply.send(client.get_time().await);
        }
        ActorCommand::SetTime { value, reply } => {
            let _ = reply.send(client.set_time(value).await);
        }
        ActorCommand::GetBattery(reply) => {
            let _ = reply.send(client.get_battery().await);
        }
        ActorCommand::SendSelfAdvert { flood, reply } => {
            let _ = reply.send(client.send_self_advert(flood).await);
        }
        ActorCommand::SetAdvertName { name, reply } => {
            let _ = reply.send(client.set_advert_name(&name).await);
        }
        ActorCommand::SetCoordinates {
            latitude,
            longitude,
            reply,
        } => {
            let _ = reply.send(client.set_coordinates(latitude, longitude).await);
        }
        ActorCommand::SetTxPower { power_dbm, reply } => {
            let _ = reply.send(client.set_tx_power(power_dbm).await);
        }
        ActorCommand::SetRadioParams { params, reply } => {
            let _ = reply.send(client.set_radio_params(&params).await);
        }
        ActorCommand::SetTuning { params, reply } => {
            let _ = reply.send(client.set_tuning(params).await);
        }
        ActorCommand::GetTuning(reply) => {
            let _ = reply.send(client.get_tuning().await);
        }
        ActorCommand::ResetPath { public_key, reply } => {
            let _ = reply.send(client.reset_path(&public_key).await);
        }
        ActorCommand::UpdateContact { contact, reply } => {
            let _ = reply.send(client.update_contact(&contact).await);
        }
        ActorCommand::ShareContact { public_key, reply } => {
            let _ = reply.send(client.share_contact(&public_key).await);
        }
        ActorCommand::ExportContact { public_key, reply } => {
            let public_key = public_key.as_deref();
            let _ = reply.send(client.export_contact(public_key).await);
        }
        ActorCommand::ImportContact { card, reply } => {
            let _ = reply.send(client.import_contact(&card).await);
        }
        ActorCommand::GetContact { public_key, reply } => {
            let _ = reply.send(client.get_contact(&public_key).await);
        }
        ActorCommand::GetAdvertPath { public_key, reply } => {
            let _ = reply.send(client.get_advert_path(&public_key).await);
        }
        ActorCommand::SetAutoAddConfig { config, reply } => {
            let _ = reply.send(client.set_auto_add_config(config).await);
        }
        ActorCommand::GetAutoAddConfig(reply) => {
            let _ = reply.send(client.get_auto_add_config().await);
        }
        ActorCommand::GetCustomVars(reply) => {
            let _ = reply.send(client.get_custom_vars().await);
        }
        ActorCommand::SetCustomVar { key, value, reply } => {
            let _ = reply.send(client.set_custom_var(&key, &value).await);
        }
        ActorCommand::GetStats { stats_type, reply } => {
            let _ = reply.send(client.get_stats(stats_type).await);
        }
        ActorCommand::GetAllowedRepeatFrequencies(reply) => {
            let _ = reply.send(client.get_allowed_repeat_frequencies().await);
        }
        ActorCommand::SetPathHashMode { mode, reply } => {
            let _ = reply.send(client.set_path_hash_mode(mode).await);
        }
        ActorCommand::GetPathHashMode(reply) => {
            let _ = reply.send(client.get_path_hash_mode().await);
        }
        ActorCommand::SetFloodScope { scope, reply } => {
            let _ = reply.send(client.set_flood_scope(&scope).await);
        }
        ActorCommand::SetDefaultFloodScope { name, key, reply } => {
            let _ = reply.send(client.set_default_flood_scope(&name, key).await);
        }
        ActorCommand::ClearDefaultFloodScope(reply) => {
            let _ = reply.send(client.clear_default_flood_scope().await);
        }
        ActorCommand::GetDefaultFloodScope(reply) => {
            let _ = reply.send(client.get_default_flood_scope().await);
        }
        ActorCommand::GetChannel { idx, reply } => {
            let _ = reply.send(client.get_channel(idx).await);
        }
        ActorCommand::SetChannel {
            idx,
            name,
            secret,
            reply,
        } => {
            let _ = reply.send(client.set_channel(idx, &name, secret).await);
        }
        ActorCommand::ClearChannel { idx, reply } => {
            let _ = reply.send(client.clear_channel(idx).await);
        }
        ActorCommand::RemoveContact { public_key, reply } => {
            let _ = reply.send(client.remove_contact(&public_key).await);
        }
        ActorCommand::Login {
            public_key,
            password,
            reply,
        } => {
            let _ = reply.send(client.login(&public_key, password.as_str()).await);
        }
        ActorCommand::Logout { public_key, reply } => {
            let _ = reply.send(client.logout(&public_key).await);
        }
        ActorCommand::HasConnection { public_key, reply } => {
            let _ = reply.send(client.has_connection(&public_key).await);
        }
        ActorCommand::RemoteStatus { public_key, reply } => {
            let _ = reply.send(client.remote_status(&public_key).await);
        }
        ActorCommand::GetSelfTelemetry(reply) => {
            let _ = reply.send(client.get_self_telemetry().await);
        }
        ActorCommand::BinaryRequest {
            public_key,
            request_type,
            data,
            reply,
        } => {
            let _ = reply.send(
                client
                    .binary_request(&public_key, request_type, &data)
                    .await,
            );
        }
        ActorCommand::AnonymousRequest {
            public_key,
            request_type,
            reply_route,
            reply_path,
            reply,
        } => {
            let _ = reply.send(
                client
                    .anonymous_request(&public_key, request_type, reply_route, &reply_path)
                    .await,
            );
        }
        ActorCommand::SendNodeDiscovery {
            filter,
            prefix_only,
            tag,
            since,
            reply,
        } => {
            let _ = reply.send(
                client
                    .send_node_discovery(filter, prefix_only, tag, since)
                    .await,
            );
        }
        ActorCommand::DiscoverPath { public_key, reply } => {
            let _ = reply.send(client.discover_path(&public_key).await);
        }
        ActorCommand::Sign { data, reply } => {
            let _ = reply.send(client.sign(&data).await);
        }
        ActorCommand::ExportPrivateKey(reply) => {
            let _ = reply.send(client.export_private_key().await);
        }
        ActorCommand::ImportPrivateKey { key, reply } => {
            let _ = reply.send(client.import_private_key(&key).await);
        }
        ActorCommand::SetDevicePin { pin, reply } => {
            let _ = reply.send(client.set_device_pin(pin).await);
        }
        ActorCommand::Reboot(reply) => {
            let _ = reply.send(client.reboot().await);
        }
        ActorCommand::FactoryReset(reply) => {
            let _ = reply.send(client.factory_reset().await);
        }
        ActorCommand::SyncNextMessage(reply) => {
            let _ = reply.send(client.sync_next_message().await);
        }
        ActorCommand::WaitForAck {
            ack_code,
            request_timeout,
            reply,
        } => {
            let _ = reply.send(client.wait_for_ack(ack_code, request_timeout).await);
        }
        ActorCommand::Shutdown(reply) => {
            let result = bounded_disconnect(client).await;
            let _ = reply.send(result);
            return false;
        }
    }
    true
}

async fn bounded_disconnect<T>(client: &mut Client<T>) -> Result<(), CoreError>
where
    T: Transport + Send,
{
    if !client.is_connected() {
        return Ok(());
    }
    let request_timeout = client.request_timeout();
    timeout(request_timeout, client.disconnect())
        .await
        .map_err(|_| CoreError::Timeout)?
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use std::future::pending;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use tokio::task::JoinHandle;

    use super::*;
    use crate::error::TransportError;
    use crate::protocol::{Command, CommandCode, PacketCode};
    use crate::transport::TransportKind;

    const TEST_TIMEOUT: Duration = Duration::from_secs(1);
    const SHORT_TIMEOUT: Duration = Duration::from_millis(30);
    const ACK_ONE: [u8; 4] = [1, 2, 3, 4];
    const ACK_TWO: [u8; 4] = [5, 6, 7, 8];

    #[derive(Debug)]
    enum ReadStep {
        Packet(Vec<u8>),
        CleanClose,
        Timeout,
    }

    #[derive(Debug)]
    enum Observation {
        Connect,
        Disconnect,
        ReadTimeout,
        Write(Vec<u8>),
    }

    struct ActorTestTransport {
        connected: bool,
        hang_disconnect: bool,
        inbound_rx: mpsc::UnboundedReceiver<ReadStep>,
        observation_tx: mpsc::UnboundedSender<Observation>,
        read_attempts: Arc<AtomicUsize>,
    }

    struct Driver {
        inbound_tx: mpsc::UnboundedSender<ReadStep>,
        observation_rx: mpsc::UnboundedReceiver<Observation>,
        read_attempts: Arc<AtomicUsize>,
    }

    fn actor_test_transport(hang_disconnect: bool) -> (ActorTestTransport, Driver) {
        let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
        let (observation_tx, observation_rx) = mpsc::unbounded_channel();
        let read_attempts = Arc::new(AtomicUsize::new(0));
        (
            ActorTestTransport {
                connected: false,
                hang_disconnect,
                inbound_rx,
                observation_tx,
                read_attempts: Arc::clone(&read_attempts),
            },
            Driver {
                inbound_tx,
                observation_rx,
                read_attempts,
            },
        )
    }

    impl ActorTestTransport {
        fn observe(&self, observation: Observation) -> Result<(), TransportError> {
            self.observation_tx
                .send(observation)
                .map_err(|_| TransportError::Closed)
        }
    }

    #[async_trait]
    impl Transport for ActorTestTransport {
        fn kind(&self) -> TransportKind {
            TransportKind::Scripted
        }

        async fn connect(&mut self) -> Result<(), TransportError> {
            self.observe(Observation::Connect)?;
            self.connected = true;
            Ok(())
        }

        async fn disconnect(&mut self) -> Result<(), TransportError> {
            self.observe(Observation::Disconnect)?;
            if self.hang_disconnect {
                pending::<()>().await;
            }
            self.connected = false;
            Ok(())
        }

        async fn write(&mut self, payload: &[u8]) -> Result<(), TransportError> {
            if !self.connected {
                return Err(TransportError::NotConnected);
            }
            self.observe(Observation::Write(payload.to_vec()))
        }

        async fn read(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
            if !self.connected {
                return Err(TransportError::NotConnected);
            }
            self.read_attempts.fetch_add(1, Ordering::SeqCst);
            match self.inbound_rx.recv().await {
                Some(ReadStep::Packet(packet)) => Ok(Some(packet)),
                Some(ReadStep::CleanClose) | None => Ok(None),
                Some(ReadStep::Timeout) => {
                    self.observe(Observation::ReadTimeout)?;
                    Err(TransportError::Timeout)
                }
            }
        }
    }

    impl ReconnectableTransport for ActorTestTransport {}

    impl Driver {
        fn packet(&self, packet: Vec<u8>) {
            assert!(
                self.inbound_tx.send(ReadStep::Packet(packet)).is_ok(),
                "actor test transport stopped before packet injection"
            );
        }

        fn clean_close(&self) {
            assert!(
                self.inbound_tx.send(ReadStep::CleanClose).is_ok(),
                "actor test transport stopped before clean close"
            );
        }

        fn idle_timeout(&self) {
            assert!(
                self.inbound_tx.send(ReadStep::Timeout).is_ok(),
                "actor test transport stopped before idle timeout"
            );
        }

        fn read_attempts(&self) -> usize {
            self.read_attempts.load(Ordering::SeqCst)
        }

        async fn next_observation(&mut self) -> Observation {
            match timeout(TEST_TIMEOUT, self.observation_rx.recv()).await {
                Ok(Some(observation)) => observation,
                Ok(None) => panic!("actor test observation channel closed"),
                Err(error) => panic!("timed out waiting for actor test observation: {error}"),
            }
        }

        async fn expect_no_observation(&mut self) {
            match timeout(SHORT_TIMEOUT, self.observation_rx.recv()).await {
                Err(_) => {}
                Ok(Some(observation)) => panic!("unexpected observation: {observation:?}"),
                Ok(None) => panic!("actor test observation channel closed"),
            }
        }

        async fn expect_connect(&mut self) {
            match self.next_observation().await {
                Observation::Connect => {}
                other => panic!("expected connect, got {other:?}"),
            }
        }

        async fn expect_disconnect(&mut self) {
            match self.next_observation().await {
                Observation::Disconnect => {}
                other => panic!("expected disconnect, got {other:?}"),
            }
        }

        async fn expect_read_timeout(&mut self) {
            match self.next_observation().await {
                Observation::ReadTimeout => {}
                other => panic!("expected idle read timeout, got {other:?}"),
            }
        }

        async fn expect_write(&mut self, expected: &[u8]) {
            match self.next_observation().await {
                Observation::Write(actual) => assert_eq!(actual, expected),
                other => panic!("expected write, got {other:?}"),
            }
        }
    }

    fn self_info_packet() -> Vec<u8> {
        let mut raw = vec![0_u8; 62];
        raw[0] = PacketCode::SelfInfo.to_u8();
        raw[1] = 2;
        raw[2] = 17;
        raw[3] = 22;
        raw[4..36].fill(0x22);
        raw[48..52].copy_from_slice(&915_000_u32.to_le_bytes());
        raw[52..56].copy_from_slice(&62_500_u32.to_le_bytes());
        raw[56] = 7;
        raw[57] = 5;
        raw[58..62].copy_from_slice(b"node");
        raw
    }

    fn device_info_packet() -> Vec<u8> {
        let mut raw = vec![0_u8; 82];
        raw[0] = PacketCode::DeviceInfo.to_u8();
        raw[1] = 10;
        raw[2] = 50;
        raw[3] = 8;
        raw[4..8].copy_from_slice(&123_456_u32.to_le_bytes());
        raw[8..13].copy_from_slice(b"build");
        raw[20..25].copy_from_slice(b"model");
        raw[60..66].copy_from_slice(b"1.16.0");
        raw[80] = 1;
        raw[81] = 2;
        raw
    }

    fn channel_message_packet(text: &str) -> Vec<u8> {
        let mut raw = vec![PacketCode::ChannelMsgRecv.to_u8(), 3, u8::MAX, 0];
        raw.extend_from_slice(&77_u32.to_le_bytes());
        raw.extend_from_slice(text.as_bytes());
        raw
    }

    fn message_sent_packet(ack_code: [u8; 4]) -> Vec<u8> {
        let mut raw = vec![PacketCode::MsgSent.to_u8(), 0];
        raw.extend_from_slice(&ack_code);
        raw.extend_from_slice(&250_u32.to_le_bytes());
        raw
    }

    fn ack_packet(ack_code: [u8; 4]) -> Vec<u8> {
        let mut raw = vec![PacketCode::Ack.to_u8()];
        raw.extend_from_slice(&ack_code);
        raw
    }

    fn current_time_packet(timestamp: u32) -> Vec<u8> {
        let mut raw = vec![PacketCode::CurrentTime.to_u8()];
        raw.extend_from_slice(&timestamp.to_le_bytes());
        raw
    }

    fn battery_packet(level: u16, used_kb: u32, total_kb: u32) -> Vec<u8> {
        let mut raw = vec![PacketCode::Battery.to_u8()];
        raw.extend_from_slice(&level.to_le_bytes());
        raw.extend_from_slice(&used_kb.to_le_bytes());
        raw.extend_from_slice(&total_kb.to_le_bytes());
        raw
    }

    fn node_discovery_packet(tag: u32) -> Vec<u8> {
        let mut raw = vec![
            PacketCode::ControlData.to_u8(),
            4,
            (-88_i8).to_le_bytes()[0],
            0,
            0x92,
            12,
        ];
        raw.extend_from_slice(&tag.to_le_bytes());
        raw.extend_from_slice(&[0x45; 8]);
        raw
    }

    fn managed_client(transport: ActorTestTransport) -> ManagedClient {
        ManagedClient::spawn(Client::with_timeout(transport, Duration::from_millis(100)))
    }

    async fn join_ok<T>(task: JoinHandle<Result<T, CoreError>>) -> T
    where
        T: Send + 'static,
    {
        match timeout(TEST_TIMEOUT, task).await {
            Ok(Ok(Ok(value))) => value,
            Ok(Ok(Err(error))) => panic!("managed operation failed: {error}"),
            Ok(Err(error)) => panic!("managed operation task failed: {error}"),
            Err(error) => panic!("managed operation did not finish: {error}"),
        }
    }

    async fn connect_client(client: &ManagedClient, driver: &mut Driver) -> SelfInfo {
        let task = tokio::spawn({
            let client = client.clone();
            async move { client.connect().await }
        });
        driver.expect_connect().await;
        driver.expect_write(&Command::app_start().encode()).await;
        driver.packet(self_info_packet());
        join_ok(task).await
    }

    async fn shutdown_client(client: &ManagedClient, driver: &mut Driver) {
        let task = tokio::spawn({
            let client = client.clone();
            async move { client.shutdown().await }
        });
        driver.expect_disconnect().await;
        join_ok(task).await;
    }

    async fn recv_event(receiver: &mut broadcast::Receiver<Event>) -> Event {
        match timeout(TEST_TIMEOUT, receiver.recv()).await {
            Ok(Ok(event)) => event,
            Ok(Err(error)) => panic!("event receive failed: {error}"),
            Err(error) => panic!("timed out waiting for event: {error}"),
        }
    }

    #[tokio::test]
    async fn handshake_then_device_query_runs_through_actor() {
        let (transport, mut driver) = actor_test_transport(false);
        let client = managed_client(transport);

        let self_info = connect_client(&client, &mut driver).await;
        assert_eq!(self_info.name, "node");

        let query_task = tokio::spawn({
            let client = client.clone();
            async move { client.query_device_info().await }
        });
        driver.expect_write(&Command::device_query().encode()).await;
        driver.packet(device_info_packet());
        let device_info = join_ok(query_task).await;
        assert_eq!(device_info.protocol_version, 10);
        assert_eq!(device_info.model.as_deref(), Some("model"));

        shutdown_client(&client, &mut driver).await;
    }

    #[tokio::test]
    async fn idle_packets_are_published_and_clean_close_is_emitted_once() {
        let (transport, mut driver) = actor_test_transport(false);
        let client = managed_client(transport);
        let _ = connect_client(&client, &mut driver).await;
        let mut events = client.subscribe();

        driver.packet(channel_message_packet("idle push"));
        match recv_event(&mut events).await {
            Event::Message(message) => assert_eq!(message.text, "idle push"),
            other => panic!("expected message event, got {other:?}"),
        }

        driver.clean_close();
        match recv_event(&mut events).await {
            Event::Disconnected => {}
            other => panic!("expected disconnected event, got {other:?}"),
        }
        match timeout(SHORT_TIMEOUT, events.recv()).await {
            Err(_) => {}
            Ok(Ok(event)) => panic!("unexpected duplicate event: {event:?}"),
            Ok(Err(error)) => panic!("event stream failed: {error}"),
        }

        match client.shutdown().await {
            Ok(()) => {}
            Err(error) => panic!("shutdown failed: {error}"),
        }
    }

    #[tokio::test]
    async fn node_discovery_response_is_published_by_idle_reader_after_ok() {
        let (transport, mut driver) = actor_test_transport(false);
        let client = managed_client(transport);
        let _ = connect_client(&client, &mut driver).await;
        let mut events = client.subscribe();
        let tag = 0x1234_5678;

        let request = tokio::spawn({
            let client = client.clone();
            async move {
                client
                    .send_node_discovery(0x04, true, tag, Some(0xa1b2_c3d4))
                    .await
            }
        });
        driver
            .expect_write(&[
                0x37, 0x81, 0x04, 0x78, 0x56, 0x34, 0x12, 0xd4, 0xc3, 0xb2, 0xa1,
            ])
            .await;
        driver.packet(vec![PacketCode::Ok.to_u8()]);
        driver.packet(node_discovery_packet(tag));
        join_ok(request).await;

        let event = recv_event(&mut events).await;
        let Event::ControlData(data) = event else {
            panic!("expected control-data event, got {event:?}");
        };
        let response = match data.node_discovery_response() {
            Ok(Some(response)) => response,
            Ok(None) => panic!("control-data event was not node discovery"),
            Err(error) => panic!("node-discovery response was malformed: {error}"),
        };
        assert_eq!(response.tag, tag);
        assert_eq!(response.public_key, [0x45; 8]);

        shutdown_client(&client, &mut driver).await;
    }

    #[tokio::test]
    async fn idle_transport_timeout_is_backed_off_but_commands_stay_responsive() {
        let (transport, mut driver) = actor_test_transport(false);
        let client = managed_client(transport);
        let _ = connect_client(&client, &mut driver).await;

        driver.idle_timeout();
        driver.expect_read_timeout().await;
        let attempts_after_timeout = driver.read_attempts();
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert_eq!(driver.read_attempts(), attempts_after_timeout);

        let query_task = tokio::spawn({
            let client = client.clone();
            async move { client.query_device_info().await }
        });
        driver.expect_write(&Command::device_query().encode()).await;
        driver.packet(device_info_packet());
        let _ = join_ok(query_task).await;

        shutdown_client(&client, &mut driver).await;
    }

    #[tokio::test]
    async fn commands_are_serialized_until_the_active_response_finishes() {
        let (transport, mut driver) = actor_test_transport(false);
        let client = managed_client(transport);
        let _ = connect_client(&client, &mut driver).await;

        let query_task = tokio::spawn({
            let client = client.clone();
            async move { client.query_device_info().await }
        });
        driver.expect_write(&Command::device_query().encode()).await;

        let contacts_task = tokio::spawn({
            let client = client.clone();
            async move { client.list_contacts(None).await }
        });
        driver.expect_no_observation().await;

        driver.packet(device_info_packet());
        let _ = join_ok(query_task).await;
        driver
            .expect_write(&Command::get_contacts(None).encode())
            .await;
        driver.packet(vec![PacketCode::NoMoreMsgs.to_u8()]);
        assert!(join_ok(contacts_task).await.is_empty());

        shutdown_client(&client, &mut driver).await;
    }

    #[tokio::test]
    async fn concurrent_new_operations_are_serialized_with_typed_response_ordering() {
        let (transport, mut driver) = actor_test_transport(false);
        let client = managed_client(transport);
        let _ = connect_client(&client, &mut driver).await;

        let get_time_task = tokio::spawn({
            let client = client.clone();
            async move { client.get_time().await }
        });
        driver.expect_write(&Command::get_time().encode()).await;

        let get_battery_task = tokio::spawn({
            let client = client.clone();
            async move { client.get_battery().await }
        });
        driver.expect_no_observation().await;

        driver.packet(current_time_packet(55_000));
        assert_eq!(join_ok(get_time_task).await, 55_000);

        driver.expect_write(&Command::get_battery().encode()).await;
        driver.packet(battery_packet(3_800, 1_024, 8_192));
        let battery = join_ok(get_battery_task).await;
        assert_eq!(battery.level, 3_800);
        assert_eq!(battery.used_kb, Some(1_024));
        assert_eq!(battery.total_kb, Some(8_192));

        shutdown_client(&client, &mut driver).await;
    }

    #[tokio::test]
    async fn idle_ack_is_retained_and_timed_out_ack_tracking_is_cleaned() {
        let (transport, mut driver) = actor_test_transport(false);
        let client = managed_client(transport);
        let _ = connect_client(&client, &mut driver).await;
        let mut events = client.subscribe();

        let send_task = tokio::spawn({
            let client = client.clone();
            async move { client.send_direct_text(&[9; 6], 0, "one").await }
        });
        match driver.next_observation().await {
            Observation::Write(payload) => {
                assert_eq!(
                    payload.first().copied(),
                    Some(CommandCode::SendTxtMsg.to_u8())
                );
            }
            other => panic!("expected direct send, got {other:?}"),
        }
        driver.packet(message_sent_packet(ACK_ONE));
        assert_eq!(join_ok(send_task).await.ack_code, ACK_ONE);

        driver.packet(ack_packet(ACK_ONE));
        match recv_event(&mut events).await {
            Event::Ack(ack) => assert_eq!(ack.code, ACK_ONE),
            other => panic!("expected ack event, got {other:?}"),
        }
        match client.wait_for_ack(ACK_ONE, Some(SHORT_TIMEOUT)).await {
            Ok(ack) => assert_eq!(ack.code, ACK_ONE),
            Err(error) => panic!("retained ACK wait failed: {error}"),
        }

        let second_send = tokio::spawn({
            let client = client.clone();
            async move { client.send_direct_text(&[8; 6], 0, "two").await }
        });
        match driver.next_observation().await {
            Observation::Write(payload) => {
                assert_eq!(
                    payload.first().copied(),
                    Some(CommandCode::SendTxtMsg.to_u8())
                );
            }
            other => panic!("expected second direct send, got {other:?}"),
        }
        driver.packet(message_sent_packet(ACK_TWO));
        assert_eq!(join_ok(second_send).await.ack_code, ACK_TWO);

        let timeout_error = client.wait_for_ack(ACK_TWO, Some(SHORT_TIMEOUT)).await;
        assert!(matches!(timeout_error, Err(CoreError::Timeout)));

        driver.packet(ack_packet(ACK_TWO));
        match recv_event(&mut events).await {
            Event::Ack(ack) => assert_eq!(ack.code, ACK_TWO),
            other => panic!("expected late ack event, got {other:?}"),
        }
        let second_timeout = client.wait_for_ack(ACK_TWO, Some(SHORT_TIMEOUT)).await;
        assert!(matches!(second_timeout, Err(CoreError::Timeout)));

        shutdown_client(&client, &mut driver).await;
    }

    #[tokio::test]
    async fn cancelling_caller_does_not_cancel_or_duplicate_started_send() {
        let (transport, mut driver) = actor_test_transport(false);
        let client = managed_client(transport);
        let _ = connect_client(&client, &mut driver).await;

        let send_task = tokio::spawn({
            let client = client.clone();
            async move { client.send_direct_text(&[7; 6], 0, "once").await }
        });
        match driver.next_observation().await {
            Observation::Write(payload) => {
                assert_eq!(
                    payload.first().copied(),
                    Some(CommandCode::SendTxtMsg.to_u8())
                );
            }
            other => panic!("expected direct send, got {other:?}"),
        }
        send_task.abort();
        match send_task.await {
            Err(error) if error.is_cancelled() => {}
            other => panic!("expected cancelled caller task, got {other:?}"),
        }

        driver.packet(message_sent_packet(ACK_ONE));
        let query_task = tokio::spawn({
            let client = client.clone();
            async move { client.query_device_info().await }
        });
        driver.expect_write(&Command::device_query().encode()).await;
        driver.packet(device_info_packet());
        let _ = join_ok(query_task).await;
        driver.expect_no_observation().await;

        shutdown_client(&client, &mut driver).await;
    }

    #[tokio::test]
    async fn explicit_reconnect_handshakes_without_replaying_prior_send() {
        let (transport, mut driver) = actor_test_transport(false);
        let client = managed_client(transport);
        let _ = connect_client(&client, &mut driver).await;

        let send_task = tokio::spawn({
            let client = client.clone();
            async move {
                client
                    .send_direct_text(&[6; 6], 0, "before reconnect")
                    .await
            }
        });
        match driver.next_observation().await {
            Observation::Write(payload) => {
                assert_eq!(
                    payload.first().copied(),
                    Some(CommandCode::SendTxtMsg.to_u8())
                );
            }
            other => panic!("expected direct send, got {other:?}"),
        }
        driver.packet(message_sent_packet(ACK_ONE));
        let _ = join_ok(send_task).await;

        match client.disconnect().await {
            Ok(()) => {}
            Err(error) => panic!("disconnect failed: {error}"),
        }
        driver.expect_disconnect().await;

        let reconnect_task = tokio::spawn({
            let client = client.clone();
            async move { client.reconnect().await }
        });
        driver.expect_disconnect().await;
        driver.expect_connect().await;
        driver.expect_write(&Command::app_start().encode()).await;
        driver.packet(self_info_packet());
        let _ = join_ok(reconnect_task).await;
        driver.expect_no_observation().await;

        shutdown_client(&client, &mut driver).await;
    }

    #[tokio::test]
    async fn shutdown_is_bounded_and_last_handle_drop_disconnects() {
        let (hanging_transport, mut hanging_driver) = actor_test_transport(true);
        let hanging_client =
            ManagedClient::spawn(Client::with_timeout(hanging_transport, SHORT_TIMEOUT));
        let _ = connect_client(&hanging_client, &mut hanging_driver).await;

        let shutdown_task = tokio::spawn({
            let client = hanging_client.clone();
            async move { client.shutdown().await }
        });
        hanging_driver.expect_disconnect().await;
        match timeout(TEST_TIMEOUT, shutdown_task).await {
            Ok(Ok(Err(CoreError::Timeout))) => {}
            Ok(other) => panic!("unexpected bounded shutdown result: {other:?}"),
            Err(error) => panic!("shutdown exceeded its bound: {error}"),
        }

        let (transport, mut driver) = actor_test_transport(false);
        let client = managed_client(transport);
        let _ = connect_client(&client, &mut driver).await;
        drop(client);
        driver.expect_disconnect().await;
    }
}

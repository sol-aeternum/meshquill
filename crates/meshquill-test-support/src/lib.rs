//! Deterministic in-memory companion transport and fixtures for Meshquill tests.

use std::collections::{HashSet, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use meshquill_core::{
    error::TransportError,
    protocol::{CommandCode, MAX_INNER_PAYLOAD, PacketCode},
    transport::{ReadyRead, ReconnectableTransport, Transport, TransportKind},
};

const APP_START_COMMAND: &[u8] = b"\x01\x03      mccli";
const DEVICE_QUERY_COMMAND: &[u8] = b"\x16\x03";
const CONTACT_PACKET_LEN: usize = 148;
const CONTACT_FIXED_PATH_LEN: usize = 64;
const DEFAULT_SELF_KEY: [u8; 32] = [1_u8; 32];
const DEFAULT_OUTBOUND_QUEUE_CAPACITY: usize = 64;
const DEFAULT_INBOUND_QUEUE_CAPACITY: usize = 128;
const DEFAULT_CONTACT_CAPACITY: usize = 16;
const DEFAULT_SYNC_MESSAGE_CAPACITY: usize = 16;
const DEFAULT_CHANNEL_CAPACITY: usize = 256;
const DEFAULT_ACK_CODE: [u8; 4] = [0x12, 0x34, 0x56, 0x78];
const DEFAULT_ACK_TIMEOUT_MS: u32 = 1_000;
const DEFAULT_ACK_PACKET_TIMEOUT_MS: u32 = 1_500;
const DEFAULT_DEVICE_TIME_SECONDS: u32 = 1_727_000_000;
const DEFAULT_BATTERY_LEVEL: u16 = 4_500;
const DEFAULT_BATTERY_USED_KB: u32 = 12_288;
const DEFAULT_BATTERY_TOTAL_KB: u32 = 49_152;
const DEFAULT_CHANNEL_NAME: &str = "meshquill-channel";
const DEFAULT_CHANNEL_SECRET: [u8; 16] = [0x3f; 16];
const DEFAULT_TELEMETRY_PREFIX: [u8; 6] = [1; 6];
const DEFAULT_TELEMETRY_PAYLOAD: [u8; 4] = [0xaa, 0xbb, 0xcc, 0xdd];
const DEFAULT_NODE_DISCOVERY_TYPE: u8 = 2;
const DEFAULT_NODE_DISCOVERY_KEY: [u8; 32] = [0x42; 32];
const DEFAULT_NODE_DISCOVERY_SNR_QDB: i8 = 20;
const DEFAULT_NODE_DISCOVERY_RSSI: i8 = -91;
const DEFAULT_NODE_DISCOVERY_INBOUND_SNR_QDB: i8 = 12;

/// Password used by the deterministic virtual remote authentication fixture.
pub const DEFAULT_REMOTE_PASSWORD: &str = "meshquill-demo";

const DEFAULT_REMOTE_STATUS_BATTERY_MV: u16 = 4_200;
const DEFAULT_REMOTE_STATUS_TX_QUEUE_LEN: u16 = 6;
const DEFAULT_REMOTE_STATUS_NOISE_FLOOR_DBM: i16 = -105;
const DEFAULT_REMOTE_STATUS_LAST_RSSI_DBM: i16 = -88;
const DEFAULT_REMOTE_STATUS_PACKETS_RECEIVED: u32 = 13_421;
const DEFAULT_REMOTE_STATUS_PACKETS_SENT: u32 = 12_118;
const DEFAULT_REMOTE_STATUS_TX_AIRTIME_SECONDS: u32 = 5_000;
const DEFAULT_REMOTE_STATUS_UPTIME_SECONDS: u32 = 72_100;
const DEFAULT_REMOTE_STATUS_SENT_FLOOD: u32 = 1_234;
const DEFAULT_REMOTE_STATUS_SENT_DIRECT: u32 = 2_345;
const DEFAULT_REMOTE_STATUS_RECEIVED_FLOOD: u32 = 987;
const DEFAULT_REMOTE_STATUS_RECEIVED_DIRECT: u32 = 876;
const DEFAULT_REMOTE_STATUS_ERROR_EVENTS: u16 = 15;
const DEFAULT_REMOTE_STATUS_LAST_SNR_QDB: i16 = 32;
const DEFAULT_REMOTE_STATUS_DIRECT_DUPLICATES: u16 = 7;
const DEFAULT_REMOTE_STATUS_FLOOD_DUPLICATES: u16 = 3;
const DEFAULT_REMOTE_STATUS_RX_AIRTIME_SECONDS: u32 = 4_222;
const DEFAULT_REMOTE_STATUS_RX_ERRORS: u32 = 19;
const DEFAULT_REMOTE_SESSION_PERMISSIONS: u8 = 0x01;
const DEFAULT_REMOTE_SESSION_ACL_PERMISSIONS: u8 = 0x5a;
const DEFAULT_REMOTE_SESSION_FIRMWARE_LEVEL: u8 = 0x83;
const DEFAULT_REMOTE_SESSION_CLOCK: u32 = DEFAULT_DEVICE_TIME_SECONDS;
const REMOTE_BINARY_VOLTAGE_KIND: u8 = 116;
const REMOTE_BINARY_TEMPERATURE_KIND: u8 = 103;
const DEFAULT_BINARY_CLOCK: u32 = 12_345;

#[derive(Clone, Copy, Debug, Default)]
struct ChannelSlot {
    name: [u8; 32],
    secret: [u8; 16],
}

/// Bounded configuration for all internal queues.
#[derive(Debug, Clone, Copy)]
pub struct VirtualCompanionCapacities {
    /// Maximum number of outbound packets available for inspection.
    pub outbound_queue: usize,
    /// Maximum number of inbound packets waiting for `read`.
    pub inbound_queue: usize,
    /// Maximum number of configured contact rows.
    pub contacts: usize,
    /// Maximum number of queued messages used by `SYNC_NEXT_MESSAGE`.
    pub sync_messages: usize,
}

impl Default for VirtualCompanionCapacities {
    fn default() -> Self {
        Self {
            outbound_queue: DEFAULT_OUTBOUND_QUEUE_CAPACITY,
            inbound_queue: DEFAULT_INBOUND_QUEUE_CAPACITY,
            contacts: DEFAULT_CONTACT_CAPACITY,
            sync_messages: DEFAULT_SYNC_MESSAGE_CAPACITY,
        }
    }
}

impl VirtualCompanionCapacities {
    /// Creates explicit limits for a new companion instance.
    #[must_use]
    pub const fn new(
        outbound_queue: usize,
        inbound_queue: usize,
        contacts: usize,
        sync_messages: usize,
    ) -> Self {
        Self {
            outbound_queue,
            inbound_queue,
            contacts,
            sync_messages,
        }
    }
}

/// Deterministic fault injected on the next `read` call.
#[derive(Debug, Clone, Copy)]
pub enum VirtualCompanionFault {
    /// `read` returns `TransportError::Timeout`.
    Timeout,
    /// `read` returns `Ok(None)` and marks transport disconnected.
    CleanDisconnect,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum DirectSendWriteFault {
    #[default]
    None,
    DisconnectBeforeWrite,
}

/// Behavior when `read` finds no inbound packet or configured fault.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum VirtualCompanionIdleReadMode {
    /// Return [`TransportError::Timeout`] immediately.
    #[default]
    Timeout,
    /// Remain pending until the caller cancels the read future.
    Pending,
}

/// Errors from configuration APIs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VirtualCompanionError {
    /// Configuration overflow.
    QueueFull {
        /// Queue name.
        queue: &'static str,
        /// Capacity at the moment of insertion.
        capacity: usize,
    },
    /// Contact packet must be protocol-width 148 bytes.
    InvalidContactPacket {
        /// Expected width in bytes.
        expected: usize,
        /// Observed width in bytes.
        actual: usize,
    },
    /// Packet payload exceeds protocol payload bound.
    PacketTooLarge {
        /// Maximum payload bytes.
        max: usize,
        /// Actual payload bytes.
        actual: usize,
    },
    /// Contact path bytes do not match the route descriptor exactly.
    InvalidContactPathLength {
        /// Path length encoded by the route descriptor.
        expected: usize,
        /// Supplied path length.
        actual: usize,
    },
    /// A contact coordinate is non-finite or outside geographic bounds.
    InvalidCoordinate {
        /// Coordinate field that failed validation.
        field: &'static str,
    },
}

impl fmt::Display for VirtualCompanionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QueueFull { queue, capacity } => {
                write!(f, "queue '{queue}' overflow (capacity {capacity})")
            }
            Self::InvalidContactPacket { expected, actual } => {
                write!(
                    f,
                    "invalid contact packet: expected {expected}, got {actual}"
                )
            }
            Self::PacketTooLarge { max, actual } => {
                write!(f, "packet too large: max {max}, got {actual}")
            }
            Self::InvalidContactPathLength { expected, actual } => {
                write!(
                    f,
                    "contact path length mismatch: expected {expected}, got {actual}"
                )
            }
            Self::InvalidCoordinate { field } => {
                write!(f, "invalid contact coordinate '{field}'")
            }
        }
    }
}

impl std::error::Error for VirtualCompanionError {}

#[derive(Debug)]
struct VirtualCompanionState {
    connected: bool,
    outbound: VecDeque<Vec<u8>>,
    inbound: VecDeque<Vec<u8>>,
    contact_rows: Vec<Vec<u8>>,
    sync_queue: VecDeque<Vec<u8>>,
    channels: Vec<ChannelSlot>,
    reconnect_count: usize,
    reconnect_failures_remaining: usize,
    next_reconnect_push: Option<Vec<u8>>,
    next_fault: Option<VirtualCompanionFault>,
    idle_disconnects_remaining: usize,
    direct_send_write_fault: DirectSendWriteFault,
    idle_read_mode: VirtualCompanionIdleReadMode,
    duplicate_next_inbound: bool,
    send_txt_ack: [u8; 4],
    send_txt_timeout_ms: u32,
    emit_send_txt_ack: bool,
    remote_sessions: HashSet<[u8; 32]>,
    device_time_seconds: u32,
    battery_level: u16,
    battery_used_kb: u32,
    battery_total_kb: u32,
    default_scope: Option<([u8; 31], [u8; 16])>,
    capacities: VirtualCompanionCapacities,
}

impl VirtualCompanionState {
    fn with_capacities(capacities: VirtualCompanionCapacities) -> Self {
        let mut channels = vec![ChannelSlot::default(); DEFAULT_CHANNEL_CAPACITY];
        channels[0].name[..DEFAULT_CHANNEL_NAME.len()]
            .copy_from_slice(DEFAULT_CHANNEL_NAME.as_bytes());
        channels[0].secret = DEFAULT_CHANNEL_SECRET;
        Self {
            connected: false,
            outbound: VecDeque::new(),
            inbound: VecDeque::new(),
            contact_rows: Vec::new(),
            sync_queue: VecDeque::new(),
            channels,
            reconnect_count: 0,
            reconnect_failures_remaining: 0,
            next_reconnect_push: None,
            next_fault: None,
            idle_disconnects_remaining: 0,
            direct_send_write_fault: DirectSendWriteFault::None,
            idle_read_mode: VirtualCompanionIdleReadMode::default(),
            duplicate_next_inbound: false,
            send_txt_ack: DEFAULT_ACK_CODE,
            send_txt_timeout_ms: DEFAULT_ACK_TIMEOUT_MS,
            emit_send_txt_ack: false,
            remote_sessions: HashSet::new(),
            device_time_seconds: DEFAULT_DEVICE_TIME_SECONDS,
            battery_level: DEFAULT_BATTERY_LEVEL,
            battery_used_kb: DEFAULT_BATTERY_USED_KB,
            battery_total_kb: DEFAULT_BATTERY_TOTAL_KB,
            default_scope: None,
            capacities,
        }
    }

    fn required_inbound_slots(&self, packet_count: usize) -> usize {
        if self.duplicate_next_inbound {
            packet_count.saturating_mul(2)
        } else {
            packet_count
        }
    }

    fn ensure_inbound_capacity(&self, packet_count: usize) -> Result<(), VirtualCompanionError> {
        let required = self.required_inbound_slots(packet_count);
        let available = self
            .capacities
            .inbound_queue
            .saturating_sub(self.inbound.len());
        if required > available {
            return Err(VirtualCompanionError::QueueFull {
                queue: "inbound_queue",
                capacity: self.capacities.inbound_queue,
            });
        }
        Ok(())
    }

    fn queue_inbound(&mut self, packets: &[Vec<u8>]) -> Result<(), VirtualCompanionError> {
        self.ensure_inbound_capacity(packets.len())?;
        let duplicate = self.duplicate_next_inbound;
        self.duplicate_next_inbound = false;

        for packet in packets {
            self.inbound.push_back(packet.clone());
            if duplicate {
                self.inbound.push_back(packet.clone());
            }
        }
        Ok(())
    }

    fn queue_outbound(&mut self, payload: Vec<u8>) -> Result<(), VirtualCompanionError> {
        self.ensure_outbound_capacity()?;
        self.outbound.push_back(payload);
        Ok(())
    }

    fn ensure_outbound_capacity(&self) -> Result<(), VirtualCompanionError> {
        if self.outbound.len() >= self.capacities.outbound_queue {
            return Err(VirtualCompanionError::QueueFull {
                queue: "outbound_queue",
                capacity: self.capacities.outbound_queue,
            });
        }
        Ok(())
    }
}

/// Deterministic cloneable in-memory companion transport.
#[derive(Clone)]
pub struct VirtualCompanion {
    state: Arc<Mutex<VirtualCompanionState>>,
}

impl Default for VirtualCompanion {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for VirtualCompanion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = lock_state(&self.state);
        write!(
            f,
            "VirtualCompanion {{ connected: {}, reconnects: {}, outbound_queue_len: {}, inbound_queue_len: {}, contacts: {}, sync_messages: {} }}",
            state.connected,
            state.reconnect_count,
            state.outbound.len(),
            state.inbound.len(),
            state.contact_rows.len(),
            state.sync_queue.len()
        )
    }
}

impl VirtualCompanion {
    /// Creates a transport with default capacities.
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacities(VirtualCompanionCapacities::default())
    }

    /// Creates a transport with explicit capacities.
    #[must_use]
    pub fn with_capacities(capacities: VirtualCompanionCapacities) -> Self {
        Self {
            state: Arc::new(Mutex::new(VirtualCompanionState::with_capacities(
                capacities,
            ))),
        }
    }

    /// Replaces all contacts returned by `GET_CONTACTS`.
    ///
    /// # Errors
    ///
    /// Returns an error if a row is malformed or oversized, or the configured
    /// contact capacity would be exceeded.
    pub fn set_contacts<I>(&self, contacts: I) -> Result<(), VirtualCompanionError>
    where
        I: IntoIterator<Item = Vec<u8>>,
    {
        let mut next = Vec::new();
        for contact in contacts {
            if contact.len() != CONTACT_PACKET_LEN {
                return Err(VirtualCompanionError::InvalidContactPacket {
                    expected: CONTACT_PACKET_LEN,
                    actual: contact.len(),
                });
            }
            if contact.len() > MAX_INNER_PAYLOAD {
                return Err(VirtualCompanionError::PacketTooLarge {
                    max: MAX_INNER_PAYLOAD,
                    actual: contact.len(),
                });
            }
            next.push(contact);
        }

        let mut state = lock_state(&self.state);
        if next.len() > state.capacities.contacts {
            return Err(VirtualCompanionError::QueueFull {
                queue: "contacts",
                capacity: state.capacities.contacts,
            });
        }
        state.contact_rows = next;
        Ok(())
    }

    /// Pushes one queued `SYNC_NEXT_MESSAGE` packet.
    ///
    /// # Errors
    ///
    /// Returns an error if the packet is oversized or the sync queue is full.
    pub fn push_sync_message(&self, message: Vec<u8>) -> Result<(), VirtualCompanionError> {
        if message.len() > MAX_INNER_PAYLOAD {
            return Err(VirtualCompanionError::PacketTooLarge {
                max: MAX_INNER_PAYLOAD,
                actual: message.len(),
            });
        }

        let mut state = lock_state(&self.state);
        if state.sync_queue.len() >= state.capacities.sync_messages {
            return Err(VirtualCompanionError::QueueFull {
                queue: "sync_messages",
                capacity: state.capacities.sync_messages,
            });
        }
        state.sync_queue.push_back(message);
        Ok(())
    }

    /// Replaces all queued `SYNC_NEXT_MESSAGE` packets.
    ///
    /// # Errors
    ///
    /// Returns an error if a packet is oversized or the configured sync queue
    /// capacity would be exceeded.
    pub fn set_sync_messages<I>(&self, messages: I) -> Result<(), VirtualCompanionError>
    where
        I: IntoIterator<Item = Vec<u8>>,
    {
        let mut queue = VecDeque::new();
        for message in messages {
            if message.len() > MAX_INNER_PAYLOAD {
                return Err(VirtualCompanionError::PacketTooLarge {
                    max: MAX_INNER_PAYLOAD,
                    actual: message.len(),
                });
            }
            queue.push_back(message);
        }

        let mut state = lock_state(&self.state);
        if queue.len() > state.capacities.sync_messages {
            return Err(VirtualCompanionError::QueueFull {
                queue: "sync_messages",
                capacity: state.capacities.sync_messages,
            });
        }
        state.sync_queue = queue;
        Ok(())
    }

    /// Enqueues unsolicited inbound packets.
    ///
    /// # Errors
    ///
    /// Returns an error if the packet is oversized or the inbound queue cannot
    /// accept it without dropping an existing packet.
    pub fn enqueue_push(&self, packet: Vec<u8>) -> Result<(), VirtualCompanionError> {
        if packet.len() > MAX_INNER_PAYLOAD {
            return Err(VirtualCompanionError::PacketTooLarge {
                max: MAX_INNER_PAYLOAD,
                actual: packet.len(),
            });
        }

        let mut state = lock_state(&self.state);
        state.queue_inbound(&[packet])
    }

    /// Configures the `MSG_SENT` `expected_ack` and optional following `ACK` packet.
    pub fn configure_send_txt_ack(&self, ack: [u8; 4], timeout_ms: u32, emit_ack_packet: bool) {
        let mut state = lock_state(&self.state);
        state.send_txt_ack = ack;
        state.send_txt_timeout_ms = timeout_ms;
        state.emit_send_txt_ack = emit_ack_packet;
    }

    /// Configures how `read` behaves when the inbound queue is empty.
    pub fn set_idle_read_mode(&self, mode: VirtualCompanionIdleReadMode) {
        let mut state = lock_state(&self.state);
        state.idle_read_mode = mode;
    }

    /// Returns and clears all observed outbound command packets.
    #[must_use]
    pub fn drain_outbound(&self) -> Vec<Vec<u8>> {
        let mut state = lock_state(&self.state);
        state.outbound.drain(..).collect()
    }

    /// Returns observed outbound command packets without clearing them.
    #[must_use]
    pub fn outbound_packets(&self) -> Vec<Vec<u8>> {
        let state = lock_state(&self.state);
        state.outbound.iter().cloned().collect()
    }

    /// Number of times `reconnect` has been called.
    #[must_use]
    pub fn reconnect_count(&self) -> usize {
        let state = lock_state(&self.state);
        state.reconnect_count
    }

    /// Schedules one clean disconnect after all currently queued inbound packets are read.
    ///
    /// Unlike [`Self::set_next_read_fault`], this control never interrupts a queued command
    /// response. Repeated calls before the disconnect remain a single bounded one-shot request.
    pub fn disconnect_on_next_idle_read(&self) {
        let mut state = lock_state(&self.state);
        state.idle_disconnects_remaining = 1;
    }

    /// Disconnects before accepting the next direct-text command write.
    ///
    /// The one-shot command is not recorded and no response is queued, so tests can distinguish a
    /// known-unsent draft from an ambiguous failure after transport acceptance.
    pub fn disconnect_before_next_direct_send(&self) {
        let mut state = lock_state(&self.state);
        state.direct_send_write_fault = DirectSendWriteFault::DisconnectBeforeWrite;
    }

    /// Makes exactly `count` subsequent reconnect attempts fail deterministically.
    ///
    /// A later call replaces the remaining failure count. Every attempted reconnect still
    /// increments [`Self::reconnect_count`].
    pub fn fail_next_reconnects(&self, count: usize) {
        let mut state = lock_state(&self.state);
        state.reconnect_failures_remaining = count;
    }

    /// Retains one unsolicited packet for delivery on the next successful reconnect.
    ///
    /// A later call replaces the previously retained packet, keeping this fixture state bounded.
    /// The packet is queued before the reconnecting client's `APP_START` response.
    ///
    /// # Errors
    ///
    /// Returns an error when the packet exceeds the protocol payload bound.
    pub fn set_next_reconnect_push(&self, packet: Vec<u8>) -> Result<(), VirtualCompanionError> {
        if packet.len() > MAX_INNER_PAYLOAD {
            return Err(VirtualCompanionError::PacketTooLarge {
                max: MAX_INNER_PAYLOAD,
                actual: packet.len(),
            });
        }

        let mut state = lock_state(&self.state);
        state.next_reconnect_push = Some(packet);
        Ok(())
    }

    /// Schedules a deterministic fault for the next `read` call.
    pub fn set_next_read_fault(&self, fault: VirtualCompanionFault) {
        let mut state = lock_state(&self.state);
        state.next_fault = Some(fault);
    }

    /// Duplicates the next inbound packet pushed by command handling or push APIs.
    pub fn duplicate_next_inbound_packet(&self) {
        let mut state = lock_state(&self.state);
        state.duplicate_next_inbound = true;
    }

    /// Returns whether the transport is currently connected.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        let state = lock_state(&self.state);
        state.connected
    }

    /// Seeds or clears one authenticated remote session for deterministic CLI scenarios.
    ///
    /// This changes only mock state; it does not bypass authentication in the production client.
    pub fn set_remote_session(&self, public_key: [u8; 32], authenticated: bool) {
        let mut state = lock_state(&self.state);
        if authenticated {
            if state.remote_sessions.contains(&public_key)
                || state.remote_sessions.len() < state.capacities.contacts.max(1)
            {
                state.remote_sessions.insert(public_key);
            }
        } else {
            state.remote_sessions.remove(&public_key);
        }
    }

    fn handle_command(
        state: &mut VirtualCompanionState,
        payload: &[u8],
    ) -> Result<(), VirtualCompanionError> {
        let response_count = Self::command_response_count(state, payload);
        state.ensure_inbound_capacity(response_count)?;

        let responses = match payload.first().copied() {
            Some(command) => match CommandCode::try_from(command) {
                Ok(CommandCode::AppStart) => Self::on_app_start(payload),
                Ok(CommandCode::DeviceQuery) => Self::on_device_query(payload),
                Ok(CommandCode::GetDeviceTime) => Self::on_get_device_time(state, payload),
                Ok(CommandCode::SetDeviceTime) => Self::on_set_device_time(state, payload),
                Ok(CommandCode::SendSelfAdvert) => Self::on_send_self_advert(payload),
                Ok(CommandCode::SendLogin) => Self::on_send_login(state, payload),
                Ok(CommandCode::SendStatusReq) => Self::on_send_status(state, payload),
                Ok(CommandCode::HasConnection) => Self::on_has_connection(state, payload),
                Ok(CommandCode::Logout) => Self::on_logout(state, payload),
                Ok(CommandCode::BinaryReq) => Self::on_binary_request(state, payload),
                Ok(CommandCode::SendAnonReq) => Self::on_send_anon_req(state, payload),
                Ok(CommandCode::Reboot) => Self::on_reboot(payload),
                Ok(CommandCode::GetBattAndStorage) => Self::on_get_battery(state),
                Ok(CommandCode::SendTelemetryReq) => Self::on_send_telemetry_req(payload),
                Ok(CommandCode::GetContactByKey) => Self::on_get_contact_by_key(state, payload),
                Ok(CommandCode::GetAdvertPath) => Self::on_get_advert_path(state, payload),
                Ok(CommandCode::ResetPath) => Self::on_reset_path(state, payload),
                Ok(CommandCode::ExportContact) => Self::on_export_contact(state, payload),
                Ok(CommandCode::ImportContact) => Self::on_import_contact(state, payload),
                Ok(CommandCode::GetContacts) => Self::on_get_contacts(state, payload),
                Ok(CommandCode::SendTxtMsg) => Self::on_send_txt_msg(state, payload),
                Ok(CommandCode::SendChannelTxtMsg) => Self::on_send_channel_txt_msg(payload),
                Ok(CommandCode::SyncNextMessage) => Self::on_sync_next_message(state, payload),
                Ok(CommandCode::GetChannel) => Self::on_get_channel(state, payload),
                Ok(CommandCode::SetChannel) => Self::on_set_channel(state, payload),
                Ok(CommandCode::AddUpdateContact) => Self::on_update_contact(state, payload),
                Ok(CommandCode::RemoveContact) => Self::on_remove_contact(state, payload),
                Ok(CommandCode::ShareContact) => Self::on_share_contact(state, payload),
                Ok(CommandCode::PathDiscovery) => Self::on_path_discovery(state, payload),
                Ok(CommandCode::SendControlData) => Self::on_send_control_data(payload),
                Ok(CommandCode::SetFloodScope) => Self::on_set_flood_scope(payload),
                Ok(CommandCode::SetDefaultFloodScope) => {
                    Self::on_set_default_flood_scope(state, payload)
                }
                Ok(CommandCode::GetDefaultFloodScope) => {
                    Self::on_get_default_flood_scope(state, payload)
                }
                Ok(_) | Err(_) => Vec::new(),
            },
            None => Vec::new(),
        };
        state.queue_inbound(&responses)
    }

    fn command_response_count(state: &VirtualCompanionState, payload: &[u8]) -> usize {
        match payload.first().copied() {
            Some(command) => match CommandCode::try_from(command) {
                Ok(CommandCode::SendLogin) if payload.len() >= 33 => 2,
                Ok(CommandCode::SendStatusReq)
                    if payload.len() == 33 && Self::is_remote_session_active(state, payload) =>
                {
                    2
                }
                Ok(CommandCode::BinaryReq) if payload.len() >= 34 => {
                    if Self::is_remote_session_active(state, payload) {
                        2
                    } else {
                        1
                    }
                }
                Ok(CommandCode::SendAnonReq) if payload.len() >= 35 => 2,
                Ok(CommandCode::GetContacts) if matches!(payload.len(), 1 | 5) => {
                    state.contact_rows.len().saturating_add(2)
                }
                Ok(CommandCode::SendTxtMsg)
                    if payload.len() >= 14 && matches!(payload[1], 0x00 | 0x01) =>
                {
                    usize::from(state.emit_send_txt_ack) + 1
                }
                Ok(CommandCode::PathDiscovery) if payload.len() == 34 => 2,
                Ok(CommandCode::SendControlData) => {
                    if Self::node_discovery_request(payload).is_some() {
                        2
                    } else {
                        1
                    }
                }
                Ok(
                    CommandCode::AppStart
                    | CommandCode::DeviceQuery
                    | CommandCode::GetDeviceTime
                    | CommandCode::SetDeviceTime
                    | CommandCode::SendSelfAdvert
                    | CommandCode::GetBattAndStorage
                    | CommandCode::GetContacts
                    | CommandCode::SendTxtMsg
                    | CommandCode::SendChannelTxtMsg
                    | CommandCode::SyncNextMessage
                    | CommandCode::GetChannel
                    | CommandCode::GetContactByKey
                    | CommandCode::GetAdvertPath
                    | CommandCode::ResetPath
                    | CommandCode::ExportContact
                    | CommandCode::ImportContact
                    | CommandCode::SetChannel
                    | CommandCode::AddUpdateContact
                    | CommandCode::RemoveContact
                    | CommandCode::ShareContact
                    | CommandCode::SendTelemetryReq
                    | CommandCode::PathDiscovery
                    | CommandCode::SetFloodScope
                    | CommandCode::SetDefaultFloodScope
                    | CommandCode::GetDefaultFloodScope
                    | CommandCode::SendLogin
                    | CommandCode::SendStatusReq
                    | CommandCode::HasConnection
                    | CommandCode::Logout
                    | CommandCode::BinaryReq
                    | CommandCode::SendAnonReq,
                ) => 1,
                Ok(_) | Err(_) => 0,
            },
            None => 0,
        }
    }

    fn on_app_start(payload: &[u8]) -> Vec<Vec<u8>> {
        if payload == APP_START_COMMAND {
            vec![Self::self_info_packet()]
        } else {
            vec![Self::error_packet()]
        }
    }

    fn on_device_query(payload: &[u8]) -> Vec<Vec<u8>> {
        if payload == DEVICE_QUERY_COMMAND {
            vec![Self::device_info_packet()]
        } else {
            vec![Self::error_packet_with_code(1)]
        }
    }

    fn on_get_device_time(state: &VirtualCompanionState, payload: &[u8]) -> Vec<Vec<u8>> {
        if !matches!(payload.len(), 1) {
            return vec![Self::error_packet_with_code(1)];
        }
        vec![Self::current_time_packet(state.device_time_seconds)]
    }

    fn on_set_device_time(state: &mut VirtualCompanionState, payload: &[u8]) -> Vec<Vec<u8>> {
        if payload.len() != 5 {
            return vec![Self::error_packet_with_code(1)];
        }
        let requested = u32::from_le_bytes([payload[1], payload[2], payload[3], payload[4]]);
        if requested < state.device_time_seconds {
            return vec![Self::error_packet_with_code(6)];
        }
        state.device_time_seconds = requested;
        vec![Self::ok_packet()]
    }

    fn on_send_self_advert(payload: &[u8]) -> Vec<Vec<u8>> {
        if !matches!(payload.len(), 1 | 2) || matches!(payload.get(1), Some(flag) if *flag != 0x01)
        {
            return vec![Self::error_packet_with_code(1)];
        }
        vec![Self::ok_packet()]
    }

    fn on_send_login(state: &mut VirtualCompanionState, payload: &[u8]) -> Vec<Vec<u8>> {
        if payload.len() < 33 {
            return vec![Self::error_packet_with_code(1)];
        }

        let key = Self::read_key(payload);
        let password = &payload[33..];
        let mut prefix = [0_u8; 6];
        prefix.copy_from_slice(&key[..6]);

        let mut responses = Vec::with_capacity(2);
        responses.push(Self::msg_sent_packet(
            0,
            DEFAULT_ACK_CODE,
            DEFAULT_ACK_TIMEOUT_MS,
        ));

        if password == DEFAULT_REMOTE_PASSWORD.as_bytes()
            && (state.remote_sessions.contains(&key)
                || state.remote_sessions.len() < state.capacities.contacts.max(1))
        {
            state.remote_sessions.insert(key);
            responses.push(Self::login_success_packet(prefix));
        } else {
            responses.push(Self::login_failed_packet(prefix));
        }

        responses
    }

    fn on_has_connection(state: &mut VirtualCompanionState, payload: &[u8]) -> Vec<Vec<u8>> {
        if payload.len() != 33 {
            return vec![Self::error_packet_with_code(1)];
        }

        let key = Self::read_key(payload);
        if state.remote_sessions.contains(&key) {
            vec![Self::ok_packet()]
        } else {
            vec![Self::error_packet_with_code(2)]
        }
    }

    fn on_logout(state: &mut VirtualCompanionState, payload: &[u8]) -> Vec<Vec<u8>> {
        if payload.len() != 33 {
            return vec![Self::error_packet_with_code(1)];
        }

        let key = Self::read_key(payload);
        if state.remote_sessions.remove(&key) {
            vec![Self::ok_packet()]
        } else {
            vec![Self::error_packet_with_code(2)]
        }
    }

    fn on_send_status(state: &mut VirtualCompanionState, payload: &[u8]) -> Vec<Vec<u8>> {
        if payload.len() != 33 {
            return vec![Self::error_packet_with_code(1)];
        }

        let key = Self::read_key(payload);
        if !state.remote_sessions.contains(&key) {
            return vec![Self::error_packet_with_code(2)];
        }

        let mut prefix = [0_u8; 6];
        prefix.copy_from_slice(&key[..6]);

        vec![
            Self::msg_sent_packet(0, DEFAULT_ACK_CODE, DEFAULT_ACK_TIMEOUT_MS),
            Self::status_response_packet(prefix),
        ]
    }

    fn on_binary_request(state: &mut VirtualCompanionState, payload: &[u8]) -> Vec<Vec<u8>> {
        if payload.len() < 34 {
            return vec![Self::error_packet_with_code(1)];
        }

        let key = Self::read_key(payload);
        if !state.remote_sessions.contains(&key) {
            return vec![Self::error_packet_with_code(2)];
        }

        match payload[33] {
            3 => vec![
                Self::msg_sent_packet(0, DEFAULT_ACK_CODE, DEFAULT_ACK_TIMEOUT_MS),
                Self::binary_response_packet(&Self::remote_telemetry_response()),
            ],
            4 => vec![
                Self::msg_sent_packet(0, DEFAULT_ACK_CODE, DEFAULT_ACK_TIMEOUT_MS),
                Self::binary_response_packet(&Self::remote_summary_response()),
            ],
            5 => vec![
                Self::msg_sent_packet(0, DEFAULT_ACK_CODE, DEFAULT_ACK_TIMEOUT_MS),
                Self::binary_response_packet(&Self::remote_acl_response()),
            ],
            6 => match Self::remote_neighbours_response(key, payload) {
                Some(entry) => vec![
                    Self::msg_sent_packet(0, DEFAULT_ACK_CODE, DEFAULT_ACK_TIMEOUT_MS),
                    Self::binary_response_packet(&entry),
                ],
                None => vec![Self::error_packet_with_code(1)],
            },
            _ => vec![Self::error_packet_with_code(1)],
        }
    }

    fn on_send_anon_req(_state: &mut VirtualCompanionState, payload: &[u8]) -> Vec<Vec<u8>> {
        if payload.len() < 35 {
            return vec![Self::error_packet_with_code(1)];
        }

        let request_type = payload[33];
        if request_type == 0 {
            return vec![Self::error_packet_with_code(1)];
        }

        let route_descriptor = payload[34];
        let reply_path = &payload[35..];
        if route_descriptor == u8::MAX || !Self::is_valid_direct_route(route_descriptor, reply_path)
        {
            return vec![Self::error_packet_with_code(1)];
        }

        let response = match request_type {
            1 => Self::remote_regions_response(),
            2 => Self::remote_owner_response(),
            3 => Self::remote_basic_response(),
            _ => return vec![Self::error_packet_with_code(1)],
        };

        vec![
            Self::msg_sent_packet(0, DEFAULT_ACK_CODE, DEFAULT_ACK_TIMEOUT_MS),
            Self::binary_response_packet(&response),
        ]
    }

    fn on_reboot(_payload: &[u8]) -> Vec<Vec<u8>> {
        Vec::new()
    }

    fn on_get_battery(state: &VirtualCompanionState) -> Vec<Vec<u8>> {
        vec![Self::battery_info_packet(state)]
    }

    fn on_get_contacts(state: &VirtualCompanionState, payload: &[u8]) -> Vec<Vec<u8>> {
        if !matches!(payload.len(), 1 | 5) {
            return vec![Self::error_packet_with_code(1)];
        }
        let mut out = Vec::new();
        let count = u32::try_from(state.contact_rows.len()).unwrap_or(u32::MAX);
        out.push(Self::contact_start_packet(count));
        out.extend(state.contact_rows.iter().cloned());
        out.push(Self::contact_end_packet(0));
        out
    }

    fn on_get_contact_by_key(state: &VirtualCompanionState, payload: &[u8]) -> Vec<Vec<u8>> {
        if payload.len() != 33 {
            return vec![Self::error_packet_with_code(1)];
        }
        let key = &payload[1..33];
        match state
            .contact_rows
            .iter()
            .find(|row| row.get(1..33).is_some_and(|stored| stored == key))
        {
            Some(row) => vec![row.clone()],
            None => vec![Self::error_packet_with_code(2)],
        }
    }

    fn on_get_advert_path(state: &VirtualCompanionState, payload: &[u8]) -> Vec<Vec<u8>> {
        if payload.len() != 34 {
            return vec![Self::error_packet_with_code(1)];
        }
        if payload[1] != 0 {
            return vec![Self::error_packet_with_code(6)];
        }
        let key = &payload[2..34];
        for row in &state.contact_rows {
            if row.get(1..33).is_some_and(|stored| stored == key) {
                let route = row[35];
                let path_len = contact_path_bytes_used(route);
                let mut out = vec![PacketCode::AdvertPath.to_u8()];
                out.extend_from_slice(&row[132..136]);
                out.push(route);
                out.extend_from_slice(&row[36..(36 + path_len)]);
                return vec![out];
            }
        }
        vec![Self::error_packet_with_code(2)]
    }

    fn on_reset_path(state: &mut VirtualCompanionState, payload: &[u8]) -> Vec<Vec<u8>> {
        if payload.len() != 33 {
            return vec![Self::error_packet_with_code(1)];
        }
        let key = &payload[1..33];
        for row in &mut state.contact_rows {
            if row.get(1..33).is_some_and(|stored| stored == key) {
                if let Some(route) = row.get_mut(35) {
                    *route = u8::MAX;
                }
                if let Some(path) = row.get_mut(36..(36 + CONTACT_FIXED_PATH_LEN)) {
                    path.fill(0);
                }
                return vec![Self::ok_packet()];
            }
        }
        vec![Self::error_packet_with_code(2)]
    }

    fn on_export_contact(state: &VirtualCompanionState, payload: &[u8]) -> Vec<Vec<u8>> {
        if payload.len() == 1 {
            return vec![Self::contact_uri_packet(&Self::self_contact_card())];
        }
        if payload.len() != 33 {
            return vec![Self::error_packet_with_code(1)];
        }
        let key = &payload[1..33];
        match state
            .contact_rows
            .iter()
            .find(|row| row.get(1..33).is_some_and(|stored| stored == key))
        {
            Some(row) => vec![Self::contact_uri_packet(row)],
            None => vec![Self::error_packet_with_code(2)],
        }
    }

    fn on_import_contact(state: &mut VirtualCompanionState, payload: &[u8]) -> Vec<Vec<u8>> {
        if payload.len() < 99 {
            return vec![Self::error_packet_with_code(1)];
        }

        let card = &payload[1..];
        if card.len() == CONTACT_PACKET_LEN && card[0] == PacketCode::Contact.to_u8() {
            if !valid_contact_row(card) {
                return vec![Self::error_packet_with_code(6)];
            }
            return Self::upsert_contact_row(state, card.to_vec());
        }
        if card == Self::self_contact_card() {
            vec![Self::ok_packet()]
        } else {
            vec![Self::error_packet_with_code(6)]
        }
    }

    fn on_send_telemetry_req(payload: &[u8]) -> Vec<Vec<u8>> {
        if payload != [CommandCode::SendTelemetryReq.to_u8(), 0, 0, 0] {
            return vec![Self::error_packet_with_code(1)];
        }
        vec![Self::telemetry_packet()]
    }

    fn on_send_txt_msg(state: &VirtualCompanionState, payload: &[u8]) -> Vec<Vec<u8>> {
        if payload.len() < 14 || !matches!(payload[1], 0x00 | 0x01) {
            return vec![Self::error_packet_with_code(1)];
        }
        let destination_type = payload[1];
        let mut out = Vec::new();
        out.push(Self::msg_sent_packet(
            destination_type,
            state.send_txt_ack,
            state.send_txt_timeout_ms,
        ));
        if state.emit_send_txt_ack {
            out.push(Self::ack_packet(state.send_txt_ack));
        }
        out
    }

    fn on_send_channel_txt_msg(payload: &[u8]) -> Vec<Vec<u8>> {
        if payload.len() < 7 {
            vec![Self::error_packet_with_code(1)]
        } else {
            vec![vec![PacketCode::Ok.to_u8()]]
        }
    }

    fn on_sync_next_message(state: &mut VirtualCompanionState, payload: &[u8]) -> Vec<Vec<u8>> {
        if payload.len() != 1 {
            return vec![Self::error_packet_with_code(1)];
        }
        if let Some(packet) = state.sync_queue.pop_front() {
            vec![packet]
        } else {
            vec![vec![PacketCode::NoMoreMsgs.to_u8()]]
        }
    }

    fn on_get_channel(state: &VirtualCompanionState, payload: &[u8]) -> Vec<Vec<u8>> {
        if payload.len() != 2 {
            return vec![Self::error_packet_with_code(1)];
        }
        vec![Self::channel_info_packet(state, payload[1])]
    }

    fn on_set_channel(state: &mut VirtualCompanionState, payload: &[u8]) -> Vec<Vec<u8>> {
        if payload.len() != 50 {
            return vec![Self::error_packet_with_code(1)];
        }
        let idx = usize::from(payload[1]);
        state.channels[idx].name.copy_from_slice(&payload[2..34]);
        state.channels[idx].secret.copy_from_slice(&payload[34..50]);
        vec![Self::ok_packet()]
    }

    fn on_update_contact(state: &mut VirtualCompanionState, payload: &[u8]) -> Vec<Vec<u8>> {
        if payload.len() != 144 {
            return vec![Self::error_packet_with_code(1)];
        }

        let route = payload[35];
        let used_path_bytes = contact_path_bytes_used(route);
        let path_field = &payload[36..100];
        if used_path_bytes > CONTACT_FIXED_PATH_LEN
            || path_field[used_path_bytes..].iter().any(|byte| *byte != 0)
            || !valid_padded_name(&payload[100..132])
        {
            return vec![Self::error_packet_with_code(6)];
        }

        let latitude = i32::from_le_bytes([payload[136], payload[137], payload[138], payload[139]]);
        let longitude =
            i32::from_le_bytes([payload[140], payload[141], payload[142], payload[143]]);
        if !(-90_000_000..=90_000_000).contains(&latitude)
            || !(-180_000_000..=180_000_000).contains(&longitude)
        {
            return vec![Self::error_packet_with_code(6)];
        }

        let mut row = Vec::with_capacity(CONTACT_PACKET_LEN);
        row.push(PacketCode::Contact.to_u8());
        row.extend_from_slice(&payload[1..]);
        row.extend_from_slice(&state.device_time_seconds.to_le_bytes());
        Self::upsert_contact_row(state, row)
    }

    fn on_share_contact(state: &VirtualCompanionState, payload: &[u8]) -> Vec<Vec<u8>> {
        if payload.len() != 33 {
            return vec![Self::error_packet_with_code(1)];
        }
        let key = &payload[1..33];
        if state
            .contact_rows
            .iter()
            .any(|row| row.get(1..33).is_some_and(|stored| stored == key))
        {
            vec![Self::ok_packet()]
        } else {
            vec![Self::error_packet_with_code(2)]
        }
    }

    fn on_path_discovery(state: &VirtualCompanionState, payload: &[u8]) -> Vec<Vec<u8>> {
        if payload.len() != 34 || payload[1] != 0 {
            return vec![Self::error_packet_with_code(1)];
        }
        let key = &payload[2..34];
        let Some(row) = state
            .contact_rows
            .iter()
            .find(|row| row.get(1..33).is_some_and(|stored| stored == key))
        else {
            return vec![Self::error_packet_with_code(2)];
        };
        let route = row[35];
        let path_bytes = contact_path_bytes_used(route);
        let mut response = vec![PacketCode::PathDiscoveryResponse.to_u8(), 0];
        response.extend_from_slice(&key[..6]);
        response.push(route);
        response.extend_from_slice(&row[36..36 + path_bytes]);
        response.push(route);
        response.extend_from_slice(&row[36..36 + path_bytes]);
        vec![
            Self::msg_sent_packet(0, DEFAULT_ACK_CODE, DEFAULT_ACK_TIMEOUT_MS),
            response,
        ]
    }

    fn on_send_control_data(payload: &[u8]) -> Vec<Vec<u8>> {
        let Some((prefix_only, tag)) = Self::node_discovery_request(payload) else {
            return vec![Self::error_packet_with_code(1)];
        };
        vec![
            Self::ok_packet(),
            Self::node_discovery_response_packet(prefix_only, tag),
        ]
    }

    fn node_discovery_request(payload: &[u8]) -> Option<(bool, u32)> {
        if !matches!(payload.len(), 7 | 11) {
            return None;
        }
        let prefix_only = match payload[1] {
            0x80 => false,
            0x81 => true,
            _ => return None,
        };
        let tag = u32::from_le_bytes([payload[3], payload[4], payload[5], payload[6]]);
        (tag != 0).then_some((prefix_only, tag))
    }

    fn on_set_flood_scope(payload: &[u8]) -> Vec<Vec<u8>> {
        let valid =
            matches!(payload, [_, 0 | 1]) || (payload.len() == 18 && payload.get(1) == Some(&0));
        if valid {
            vec![Self::ok_packet()]
        } else {
            vec![Self::error_packet_with_code(1)]
        }
    }

    fn on_set_default_flood_scope(
        state: &mut VirtualCompanionState,
        payload: &[u8],
    ) -> Vec<Vec<u8>> {
        if payload.len() == 1 {
            state.default_scope = None;
            return vec![Self::ok_packet()];
        }
        if payload.len() != 48 || !valid_padded_name(&payload[1..32]) {
            return vec![Self::error_packet_with_code(1)];
        }
        let mut name = [0_u8; 31];
        name.copy_from_slice(&payload[1..32]);
        let mut key = [0_u8; 16];
        key.copy_from_slice(&payload[32..48]);
        state.default_scope = Some((name, key));
        vec![Self::ok_packet()]
    }

    fn on_get_default_flood_scope(state: &VirtualCompanionState, payload: &[u8]) -> Vec<Vec<u8>> {
        if payload.len() != 1 {
            return vec![Self::error_packet_with_code(1)];
        }
        let mut response = vec![PacketCode::DefaultFloodScope.to_u8()];
        if let Some((name, key)) = &state.default_scope {
            response.extend_from_slice(name);
            response.extend_from_slice(key);
        }
        vec![response]
    }

    fn upsert_contact_row(state: &mut VirtualCompanionState, row: Vec<u8>) -> Vec<Vec<u8>> {
        let key = &row[1..33];
        if let Some(existing) = state
            .contact_rows
            .iter_mut()
            .find(|existing| existing.get(1..33).is_some_and(|stored| stored == key))
        {
            *existing = row;
            return vec![Self::ok_packet()];
        }
        if state.contact_rows.len() >= state.capacities.contacts {
            return vec![Self::error_packet_with_code(3)];
        }
        state.contact_rows.push(row);
        vec![Self::ok_packet()]
    }

    fn on_remove_contact(state: &mut VirtualCompanionState, payload: &[u8]) -> Vec<Vec<u8>> {
        if payload.len() != 33 {
            return vec![Self::error_packet_with_code(1)];
        }
        let key = &payload[1..33];
        let initial_len = state.contact_rows.len();
        state
            .contact_rows
            .retain(|row| row.get(1..33).is_none_or(|stored| stored != key));
        if state.contact_rows.len() == initial_len {
            vec![Self::error_packet_with_code(2)]
        } else {
            vec![Self::ok_packet()]
        }
    }

    fn is_remote_session_active(state: &VirtualCompanionState, payload: &[u8]) -> bool {
        let Some(key) = Self::parse_key(payload) else {
            return false;
        };
        state.remote_sessions.contains(&key)
    }

    fn parse_key(payload: &[u8]) -> Option<[u8; 32]> {
        if payload.len() < 33 {
            return None;
        }
        let mut key = [0_u8; 32];
        key.copy_from_slice(&payload[1..33]);
        Some(key)
    }

    fn read_key(payload: &[u8]) -> [u8; 32] {
        Self::parse_key(payload).unwrap_or([0_u8; 32])
    }

    fn remote_neighbours_response(key: [u8; 32], payload: &[u8]) -> Option<Vec<u8>> {
        if payload.len() != 44 {
            return None;
        }

        if payload[34] != 0 {
            return None;
        }

        let count = payload[35];
        if count == 0 {
            return None;
        }

        let order = payload[38];
        if order > 3 {
            return None;
        }

        let prefix_length = usize::from(payload[39]);
        if !(1..=32).contains(&prefix_length) {
            return None;
        }

        let nonce = u32::from_le_bytes([payload[40], payload[41], payload[42], payload[43]]);
        if nonce == 0 {
            return None;
        }

        let mut payload_out = Vec::new();
        payload_out.extend_from_slice(&1_i16.to_le_bytes());
        payload_out.extend_from_slice(&1_i16.to_le_bytes());
        payload_out.extend_from_slice(&key[..prefix_length]);
        payload_out.extend_from_slice(&123_u32.to_le_bytes());
        payload_out.push(0x0b);
        Some(payload_out)
    }

    fn is_valid_direct_route(descriptor: u8, path: &[u8]) -> bool {
        let hash_mode = usize::from(descriptor >> 6);
        let hop_count = usize::from(descriptor & 0x3f);
        if hash_mode > 3 || hop_count > 63 {
            return false;
        }

        let Some(expected_path_len) = hop_count.checked_mul(hash_mode + 1) else {
            return false;
        };

        expected_path_len == path.len() && expected_path_len <= 64
    }

    fn login_success_packet(prefix: [u8; 6]) -> Vec<u8> {
        let mut payload = vec![
            PacketCode::LoginSuccess.to_u8(),
            DEFAULT_REMOTE_SESSION_PERMISSIONS,
        ];
        payload.extend_from_slice(&prefix);
        payload.extend_from_slice(&DEFAULT_REMOTE_SESSION_CLOCK.to_le_bytes());
        payload.push(DEFAULT_REMOTE_SESSION_ACL_PERMISSIONS);
        payload.push(DEFAULT_REMOTE_SESSION_FIRMWARE_LEVEL);
        payload
    }

    fn login_failed_packet(prefix: [u8; 6]) -> Vec<u8> {
        let mut payload = vec![PacketCode::LoginFailed.to_u8(), 0];
        payload.extend_from_slice(&prefix);
        payload
    }

    fn status_response_packet(prefix: [u8; 6]) -> Vec<u8> {
        let mut payload = vec![PacketCode::StatusResponse.to_u8(), 0];
        payload.extend_from_slice(&prefix);
        payload.extend_from_slice(&DEFAULT_REMOTE_STATUS_BATTERY_MV.to_le_bytes());
        payload.extend_from_slice(&DEFAULT_REMOTE_STATUS_TX_QUEUE_LEN.to_le_bytes());
        payload.extend_from_slice(&DEFAULT_REMOTE_STATUS_NOISE_FLOOR_DBM.to_le_bytes());
        payload.extend_from_slice(&DEFAULT_REMOTE_STATUS_LAST_RSSI_DBM.to_le_bytes());
        payload.extend_from_slice(&DEFAULT_REMOTE_STATUS_PACKETS_RECEIVED.to_le_bytes());
        payload.extend_from_slice(&DEFAULT_REMOTE_STATUS_PACKETS_SENT.to_le_bytes());
        payload.extend_from_slice(&DEFAULT_REMOTE_STATUS_TX_AIRTIME_SECONDS.to_le_bytes());
        payload.extend_from_slice(&DEFAULT_REMOTE_STATUS_UPTIME_SECONDS.to_le_bytes());
        payload.extend_from_slice(&DEFAULT_REMOTE_STATUS_SENT_FLOOD.to_le_bytes());
        payload.extend_from_slice(&DEFAULT_REMOTE_STATUS_SENT_DIRECT.to_le_bytes());
        payload.extend_from_slice(&DEFAULT_REMOTE_STATUS_RECEIVED_FLOOD.to_le_bytes());
        payload.extend_from_slice(&DEFAULT_REMOTE_STATUS_RECEIVED_DIRECT.to_le_bytes());
        payload.extend_from_slice(&DEFAULT_REMOTE_STATUS_ERROR_EVENTS.to_le_bytes());
        payload.extend_from_slice(&DEFAULT_REMOTE_STATUS_LAST_SNR_QDB.to_le_bytes());
        payload.extend_from_slice(&DEFAULT_REMOTE_STATUS_DIRECT_DUPLICATES.to_le_bytes());
        payload.extend_from_slice(&DEFAULT_REMOTE_STATUS_FLOOD_DUPLICATES.to_le_bytes());
        payload.extend_from_slice(&DEFAULT_REMOTE_STATUS_RX_AIRTIME_SECONDS.to_le_bytes());
        payload.extend_from_slice(&DEFAULT_REMOTE_STATUS_RX_ERRORS.to_le_bytes());
        payload
    }

    fn binary_response_packet(payload: &[u8]) -> Vec<u8> {
        let mut out = vec![PacketCode::BinaryResponse.to_u8(), 0];
        out.extend_from_slice(&DEFAULT_ACK_CODE);
        out.extend_from_slice(payload);
        out
    }

    fn remote_telemetry_response() -> Vec<u8> {
        let mut payload = Vec::new();
        payload.push(1);
        payload.push(REMOTE_BINARY_VOLTAGE_KIND);
        payload.extend_from_slice(&(3_600_u16).to_be_bytes());
        payload.push(2);
        payload.push(REMOTE_BINARY_TEMPERATURE_KIND);
        payload.extend_from_slice(&(250_i16).to_be_bytes());
        payload
    }

    fn remote_summary_response() -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&DEFAULT_BINARY_CLOCK.to_le_bytes());
        payload.push(1);
        payload.push(REMOTE_BINARY_VOLTAGE_KIND);
        payload.extend_from_slice(&(3_300_u16).to_be_bytes());
        payload.extend_from_slice(&(4_200_u16).to_be_bytes());
        payload.extend_from_slice(&(3_800_u16).to_be_bytes());
        payload
    }

    fn remote_acl_response() -> Vec<u8> {
        vec![0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x80]
    }

    fn remote_regions_response() -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&DEFAULT_DEVICE_TIME_SECONDS.to_le_bytes());
        payload.extend_from_slice(b"au,eu");
        payload
    }

    fn remote_owner_response() -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&DEFAULT_DEVICE_TIME_SECONDS.to_le_bytes());
        payload.extend_from_slice(b"meshquill-demo\nMeshquill Test");
        payload
    }

    fn remote_basic_response() -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&DEFAULT_DEVICE_TIME_SECONDS.to_le_bytes());
        payload.push(0x82);
        payload
    }

    fn ok_packet() -> Vec<u8> {
        vec![PacketCode::Ok.to_u8()]
    }

    fn error_packet_with_code(code: u8) -> Vec<u8> {
        vec![PacketCode::Error.to_u8(), code]
    }

    fn node_discovery_response_packet(prefix_only: bool, tag: u32) -> Vec<u8> {
        let key_len = if prefix_only { 8 } else { 32 };
        let mut payload = Vec::with_capacity(4 + 1 + 1 + 4 + key_len);
        payload.push(PacketCode::ControlData.to_u8());
        payload.push(DEFAULT_NODE_DISCOVERY_SNR_QDB.to_le_bytes()[0]);
        payload.push(DEFAULT_NODE_DISCOVERY_RSSI.to_le_bytes()[0]);
        payload.push(0);
        payload.push(0x90 | DEFAULT_NODE_DISCOVERY_TYPE);
        payload.push(DEFAULT_NODE_DISCOVERY_INBOUND_SNR_QDB.to_le_bytes()[0]);
        payload.extend_from_slice(&tag.to_le_bytes());
        payload.extend_from_slice(&DEFAULT_NODE_DISCOVERY_KEY[..key_len]);
        payload
    }

    fn current_time_packet(time: u32) -> Vec<u8> {
        let mut payload = vec![PacketCode::CurrentTime.to_u8()];
        payload.extend_from_slice(&time.to_le_bytes());
        payload
    }

    fn battery_info_packet(state: &VirtualCompanionState) -> Vec<u8> {
        let mut payload = vec![PacketCode::Battery.to_u8()];
        payload.extend_from_slice(&state.battery_level.to_le_bytes());
        payload.extend_from_slice(&state.battery_used_kb.to_le_bytes());
        payload.extend_from_slice(&state.battery_total_kb.to_le_bytes());
        payload
    }

    fn self_contact_card() -> [u8; 98] {
        let mut card = [0_u8; 98];
        card[..DEFAULT_SELF_KEY.len()].copy_from_slice(&DEFAULT_SELF_KEY);
        card
    }

    fn contact_uri_packet(card: &[u8]) -> Vec<u8> {
        let mut payload = vec![PacketCode::ContactUri.to_u8()];
        payload.extend_from_slice(card);
        payload
    }

    fn telemetry_packet() -> Vec<u8> {
        let mut payload = vec![PacketCode::TelemetryResponse.to_u8(), 0];
        payload.extend_from_slice(&DEFAULT_TELEMETRY_PREFIX);
        payload.extend_from_slice(&DEFAULT_TELEMETRY_PAYLOAD);
        payload
    }

    fn channel_info_packet(state: &VirtualCompanionState, idx: u8) -> Vec<u8> {
        let mut payload = vec![PacketCode::ChannelInfo.to_u8(), idx];
        let idx = usize::from(idx);
        payload.extend_from_slice(&state.channels[idx].name);
        payload.extend_from_slice(&state.channels[idx].secret);
        payload
    }

    fn error_packet() -> Vec<u8> {
        vec![PacketCode::Error.to_u8(), 0x01]
    }

    fn contact_start_packet(count: u32) -> Vec<u8> {
        let mut payload = vec![PacketCode::ContactStart.to_u8()];
        payload.extend_from_slice(&count.to_le_bytes());
        payload
    }

    fn contact_end_packet(lastmod: u32) -> Vec<u8> {
        let mut payload = vec![PacketCode::ContactEnd.to_u8()];
        payload.extend_from_slice(&lastmod.to_le_bytes());
        payload
    }

    fn msg_sent_packet(destination_type: u8, ack: [u8; 4], timeout_ms: u32) -> Vec<u8> {
        let mut payload = vec![PacketCode::MsgSent.to_u8()];
        payload.push(destination_type);
        payload.extend_from_slice(&ack);
        payload.extend_from_slice(&timeout_ms.to_le_bytes());
        payload
    }

    fn ack_packet(ack: [u8; 4]) -> Vec<u8> {
        let mut payload = vec![PacketCode::Ack.to_u8()];
        payload.extend_from_slice(&ack);
        payload.extend_from_slice(&DEFAULT_ACK_PACKET_TIMEOUT_MS.to_le_bytes());
        payload
    }

    fn device_info_packet() -> Vec<u8> {
        let mut payload = vec![PacketCode::DeviceInfo.to_u8()];
        payload.push(0x0a); // protocol version.
        payload.push(18); // max_contacts/2 (x2).
        payload.push(3); // max_channels.
        payload.extend_from_slice(&0x0100_0000u32.to_le_bytes());
        payload.extend_from_slice(&pad_string("meshquill", 12));
        payload.extend_from_slice(&pad_string("meshlink-virtual", 40));
        payload.extend_from_slice(&pad_string("fw-virtual-1.0", 20));
        payload.push(1); // repeat enabled.
        payload.push(2); // four-byte path-hash mode (mode 3 is reserved by firmware).
        payload
    }

    fn self_info_packet() -> Vec<u8> {
        let mut payload = vec![PacketCode::SelfInfo.to_u8()];
        payload.push(0x01);
        payload.push(0xf0);
        payload.push(0x80);
        payload.extend_from_slice(&DEFAULT_SELF_KEY);
        payload.extend_from_slice(&0i32.to_le_bytes());
        payload.extend_from_slice(&0i32.to_le_bytes());
        payload.push(0);
        payload.push(0);
        payload.push(0b01_00_00_00);
        payload.push(0x00);
        payload.extend_from_slice(&2_440_000u32.to_le_bytes());
        payload.extend_from_slice(&125_000u32.to_le_bytes());
        payload.push(7);
        payload.push(5);
        payload.extend_from_slice(b"Meshquill Demo");
        payload
    }
}

#[async_trait]
impl Transport for VirtualCompanion {
    fn kind(&self) -> TransportKind {
        TransportKind::Scripted
    }

    async fn connect(&mut self) -> Result<(), TransportError> {
        let mut state = lock_state(&self.state);
        state.connected = true;
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<(), TransportError> {
        let mut state = lock_state(&self.state);
        state.connected = false;
        // Transport-session packets cannot be consumed after a physical disconnect. Clearing them
        // also prevents a failed handshake response from poisoning a later explicit reconnect.
        state.inbound.clear();
        Ok(())
    }

    async fn write(&mut self, payload: &[u8]) -> Result<(), TransportError> {
        let mut state = lock_state(&self.state);
        if !state.connected {
            return Err(TransportError::NotConnected);
        }

        if payload.len() > MAX_INNER_PAYLOAD {
            return Err(TransportError::PayloadTooLarge {
                maximum: MAX_INNER_PAYLOAD,
                actual: payload.len(),
            });
        }

        let is_direct_send = payload.first().copied().is_some_and(|command| {
            matches!(CommandCode::try_from(command), Ok(CommandCode::SendTxtMsg))
        });
        if state.direct_send_write_fault == DirectSendWriteFault::DisconnectBeforeWrite
            && is_direct_send
        {
            state.direct_send_write_fault = DirectSendWriteFault::None;
            state.connected = false;
            return Err(TransportError::Closed);
        }

        state
            .ensure_outbound_capacity()
            .map_err(transport_queue_error)?;
        Self::handle_command(&mut state, payload).map_err(transport_queue_error)?;
        state
            .queue_outbound(payload.to_vec())
            .map_err(transport_queue_error)?;
        Ok(())
    }

    async fn read(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
        let idle_read_mode = {
            let mut state = lock_state(&self.state);
            if !state.connected {
                return Err(TransportError::NotConnected);
            }

            if let Some(fault) = state.next_fault.take() {
                return match fault {
                    VirtualCompanionFault::Timeout => Err(TransportError::Timeout),
                    VirtualCompanionFault::CleanDisconnect => {
                        state.connected = false;
                        Ok(None)
                    }
                };
            }

            if let Some(payload) = state.inbound.pop_front() {
                return Ok(Some(payload));
            }

            if state.idle_disconnects_remaining > 0 {
                state.idle_disconnects_remaining -= 1;
                state.connected = false;
                return Ok(None);
            }

            state.idle_read_mode
        };

        match idle_read_mode {
            VirtualCompanionIdleReadMode::Timeout => Err(TransportError::Timeout),
            VirtualCompanionIdleReadMode::Pending => std::future::pending().await,
        }
    }

    fn try_read(&mut self) -> Result<ReadyRead, TransportError> {
        let mut state = lock_state(&self.state);
        if !state.connected {
            return Err(TransportError::NotConnected);
        }
        if let Some(fault) = state.next_fault.take() {
            return match fault {
                VirtualCompanionFault::Timeout => Err(TransportError::Timeout),
                VirtualCompanionFault::CleanDisconnect => {
                    state.connected = false;
                    Ok(ReadyRead::Closed)
                }
            };
        }
        if let Some(payload) = state.inbound.pop_front() {
            return Ok(ReadyRead::Packet(payload));
        }
        if state.idle_disconnects_remaining > 0 {
            state.idle_disconnects_remaining -= 1;
            state.connected = false;
            return Ok(ReadyRead::Closed);
        }
        Ok(ReadyRead::Pending)
    }
}

#[async_trait]
impl ReconnectableTransport for VirtualCompanion {
    async fn reconnect(&mut self) -> Result<(), TransportError> {
        let should_fail = {
            let mut state = lock_state(&self.state);
            state.reconnect_count = state.reconnect_count.saturating_add(1);
            if state.reconnect_failures_remaining == 0 {
                false
            } else {
                state.reconnect_failures_remaining -= 1;
                true
            }
        };
        self.disconnect().await?;
        if should_fail {
            return Err(TransportError::ReconnectFailed {
                message: "deterministic virtual companion reconnect failure",
            });
        }

        self.connect().await?;
        let queue_result = {
            let mut state = lock_state(&self.state);
            let packet = state.next_reconnect_push.take();
            let result = packet.map_or(Ok(()), |packet| state.queue_inbound(&[packet]));
            if result.is_err() {
                // The retained packet belonged to this reconnect attempt. Do not let a failed
                // enqueue arm duplication for the following APP_START handshake.
                state.duplicate_next_inbound = false;
            }
            result
        };
        if let Err(error) = queue_result {
            self.disconnect().await?;
            return Err(transport_queue_error(error));
        }
        Ok(())
    }
}

fn transport_queue_error(error: VirtualCompanionError) -> TransportError {
    match error {
        VirtualCompanionError::QueueFull { queue, capacity } => {
            TransportError::Backpressure { queue, capacity }
        }
        VirtualCompanionError::PacketTooLarge { max, actual } => TransportError::PayloadTooLarge {
            maximum: max,
            actual,
        },
        VirtualCompanionError::InvalidContactPacket { .. }
        | VirtualCompanionError::InvalidContactPathLength { .. }
        | VirtualCompanionError::InvalidCoordinate { .. } => TransportError::Io(
            std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string()),
        ),
    }
}

fn lock_state(state: &Mutex<VirtualCompanionState>) -> MutexGuard<'_, VirtualCompanionState> {
    match state.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn pad_string(value: &str, len: usize) -> Vec<u8> {
    let mut out = value.as_bytes().to_vec();
    if out.len() > len {
        out.truncate(len);
    } else {
        out.resize(len, 0);
    }
    out
}

fn contact_path_bytes_used(route: u8) -> usize {
    if route == u8::MAX {
        return 0;
    }
    let hop_count = usize::from(route & 0x3f);
    let hash_mode = usize::from(route >> 6);
    hop_count.saturating_mul(hash_mode.saturating_add(1))
}

fn valid_padded_name(field: &[u8]) -> bool {
    let Some(end) = field.iter().position(|byte| *byte == 0) else {
        return false;
    };
    end > 0
        && field[end..].iter().all(|byte| *byte == 0)
        && std::str::from_utf8(&field[..end]).is_ok()
}

fn valid_contact_row(row: &[u8]) -> bool {
    if row.len() != CONTACT_PACKET_LEN || row[0] != PacketCode::Contact.to_u8() {
        return false;
    }
    let used_path_bytes = contact_path_bytes_used(row[35]);
    if used_path_bytes > CONTACT_FIXED_PATH_LEN
        || row[36 + used_path_bytes..100].iter().any(|byte| *byte != 0)
    {
        return false;
    }
    let name = &row[100..132];
    let name_end = name
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(name.len());
    if name[name_end..].iter().any(|byte| *byte != 0)
        || std::str::from_utf8(&name[..name_end]).is_err()
    {
        return false;
    }
    let latitude = i32::from_le_bytes([row[136], row[137], row[138], row[139]]);
    let longitude = i32::from_le_bytes([row[140], row[141], row[142], row[143]]);
    (-90_000_000..=90_000_000).contains(&latitude)
        && (-180_000_000..=180_000_000).contains(&longitude)
}

/// Inputs for one fixed-width contact fixture.
#[derive(Debug, Clone, Copy)]
pub struct ContactFixture<'a> {
    /// Contact public key.
    pub public_key: [u8; 32],
    /// Firmware contact-type byte.
    pub contact_type: u8,
    /// Packed route descriptor.
    pub route: u8,
    /// Exact path bytes described by `route`.
    pub path: &'a [u8],
    /// Advertised display name.
    pub adv_name: &'a str,
    /// Last advertisement timestamp.
    pub last_advert: u32,
    /// Advertised latitude in degrees.
    pub adv_lat: f64,
    /// Advertised longitude in degrees.
    pub adv_lon: f64,
    /// Contact record modification marker.
    pub lastmod: u32,
}

/// Builds a fixed-width contact payload row (148 bytes, including packet code).
///
/// # Errors
///
/// Returns [`VirtualCompanionError::InvalidContactPathLength`] when `path` does
/// not exactly match the route descriptor, or
/// [`VirtualCompanionError::InvalidCoordinate`] for non-finite or out-of-range
/// latitude/longitude values.
pub fn make_contact_row(fixture: &ContactFixture<'_>) -> Result<Vec<u8>, VirtualCompanionError> {
    let used = contact_path_bytes_used(fixture.route);
    if used > CONTACT_FIXED_PATH_LEN || fixture.path.len() != used {
        return Err(VirtualCompanionError::InvalidContactPathLength {
            expected: used,
            actual: fixture.path.len(),
        });
    }
    let lat_scaled = scale_coordinate(fixture.adv_lat, -90.0, 90.0, "adv_lat")?;
    let lon_scaled = scale_coordinate(fixture.adv_lon, -180.0, 180.0, "adv_lon")?;

    let mut payload = vec![PacketCode::Contact.to_u8()];
    payload.extend_from_slice(&fixture.public_key);
    payload.push(fixture.contact_type);
    payload.push(0x00);
    payload.push(fixture.route);
    let mut fixed_path = [0u8; CONTACT_FIXED_PATH_LEN];
    fixed_path[..used].copy_from_slice(fixture.path);
    payload.extend_from_slice(&fixed_path);
    payload.extend_from_slice(&pad_string(fixture.adv_name, 32));
    payload.extend_from_slice(&fixture.last_advert.to_le_bytes());
    payload.extend_from_slice(&lat_scaled.to_le_bytes());
    payload.extend_from_slice(&lon_scaled.to_le_bytes());
    payload.extend_from_slice(&fixture.lastmod.to_le_bytes());
    Ok(payload)
}

/// Builds a minimal direct text packet for sync queue tests.
///
/// # Errors
///
/// Returns [`VirtualCompanionError::PacketTooLarge`] if the encoded frame would
/// exceed the protocol payload limit.
pub fn make_direct_message_packet(route: u8, text: &str) -> Result<Vec<u8>, VirtualCompanionError> {
    let mut payload = vec![PacketCode::ContactMsgRecvV3.to_u8()];
    payload.extend_from_slice(&[0x00, 0x00, 0x00]);
    payload.extend_from_slice(&[0xaa; 6]);
    payload.push(route);
    payload.push(0x00);
    payload.extend_from_slice(&0u32.to_le_bytes());
    payload.extend_from_slice(text.as_bytes());
    validate_fixture_packet(payload)
}

/// Builds a minimal channel text packet for sync queue tests.
///
/// # Errors
///
/// Returns [`VirtualCompanionError::PacketTooLarge`] if the encoded frame would
/// exceed the protocol payload limit.
pub fn make_channel_message_packet(
    channel: u8,
    text: &str,
) -> Result<Vec<u8>, VirtualCompanionError> {
    let mut payload = vec![PacketCode::ChannelMsgRecvV3.to_u8()];
    payload.extend_from_slice(&[0x00, 0x00, 0x00]);
    payload.push(channel);
    payload.push(0xff);
    payload.push(0x00);
    payload.extend_from_slice(&0u32.to_le_bytes());
    payload.extend_from_slice(text.as_bytes());
    validate_fixture_packet(payload)
}

fn validate_fixture_packet(payload: Vec<u8>) -> Result<Vec<u8>, VirtualCompanionError> {
    if payload.len() > MAX_INNER_PAYLOAD {
        return Err(VirtualCompanionError::PacketTooLarge {
            max: MAX_INNER_PAYLOAD,
            actual: payload.len(),
        });
    }
    Ok(payload)
}

fn scale_coordinate(
    value: f64,
    minimum: f64,
    maximum: f64,
    field: &'static str,
) -> Result<i32, VirtualCompanionError> {
    if !value.is_finite() || !(minimum..=maximum).contains(&value) {
        return Err(VirtualCompanionError::InvalidCoordinate { field });
    }
    let scaled = (value * 1_000_000.0).round();
    if scaled < f64::from(i32::MIN) || scaled > f64::from(i32::MAX) {
        return Err(VirtualCompanionError::InvalidCoordinate { field });
    }
    format!("{scaled:.0}")
        .parse::<i32>()
        .map_err(|_| VirtualCompanionError::InvalidCoordinate { field })
}

#[cfg(test)]
mod tests {
    use super::*;
    use meshquill_core::{
        Client, CoreError, Event,
        domain::Path,
        protocol::Packet as CorePacket,
        remote::{
            parse_basic_response, parse_owner_response, parse_regions_response,
            parse_telemetry_payload,
        },
    };

    fn must_ok<T, E: std::fmt::Display>(result: Result<T, E>, context: &str) -> T {
        match result {
            Ok(value) => value,
            Err(error) => panic!("{context}: {error}"),
        }
    }

    fn must_some<T>(value: Option<T>, context: &str) -> T {
        match value {
            Some(value) => value,
            None => panic!("{context}"),
        }
    }

    async fn raw_round_trip(
        companion: &mut VirtualCompanion,
        payload: Vec<u8>,
        context: &str,
    ) -> CorePacket {
        must_ok(companion.write(&payload).await, context);
        let raw = must_some(
            must_ok(companion.read().await, context),
            "virtual companion stream closed",
        );
        must_ok(CorePacket::parse(&raw), context)
    }

    #[tokio::test]
    async fn handshake_uses_expected_app_start_and_returns_self_info() {
        let companion = VirtualCompanion::new();
        let mut client = Client::new(companion.clone());

        let info = must_ok(client.connect().await, "handshake failed");
        assert_eq!(info.public_key.as_bytes()[0], 1);
        assert_eq!(info.name, "Meshquill Demo");

        let outbound = companion.outbound_packets();
        assert_eq!(outbound.len(), 1);
        assert_eq!(outbound[0], APP_START_COMMAND.to_vec());
        assert!(format!("{companion:?}").contains("reconnects"));
    }

    #[tokio::test]
    async fn default_idle_read_returns_timeout_immediately() {
        let mut companion = VirtualCompanion::new();
        must_ok(companion.connect().await, "connect failed");

        let read = tokio::time::timeout(std::time::Duration::from_millis(20), companion.read())
            .await
            .expect("default idle read remained pending");
        assert!(matches!(read, Err(TransportError::Timeout)));
    }

    #[tokio::test]
    async fn configured_pending_idle_read_is_cancellation_safe() {
        let mut companion = VirtualCompanion::new();
        companion.set_idle_read_mode(VirtualCompanionIdleReadMode::Pending);
        must_ok(companion.connect().await, "connect failed");

        let read =
            tokio::time::timeout(std::time::Duration::from_millis(10), companion.read()).await;
        assert!(read.is_err(), "configured idle read unexpectedly completed");
        assert!(companion.is_connected());

        let expected = must_ok(
            make_direct_message_packet(u8::MAX, "after cancelled idle read"),
            "direct fixture failed",
        );
        must_ok(
            companion.enqueue_push(expected.clone()),
            "inbound enqueue failed",
        );
        let read = tokio::time::timeout(std::time::Duration::from_millis(20), companion.read())
            .await
            .expect("fresh read remained pending after enqueue");
        assert_eq!(
            must_some(must_ok(read, "fresh read failed"), "stream closed"),
            expected
        );
    }

    #[tokio::test]
    async fn queued_packet_wins_over_pending_idle_read_mode() {
        let mut companion = VirtualCompanion::new();
        companion.set_idle_read_mode(VirtualCompanionIdleReadMode::Pending);
        let expected = must_ok(
            make_direct_message_packet(u8::MAX, "queued before pending"),
            "direct fixture failed",
        );
        must_ok(
            companion.enqueue_push(expected.clone()),
            "inbound enqueue failed",
        );
        must_ok(companion.connect().await, "connect failed");

        let read = tokio::time::timeout(std::time::Duration::from_millis(20), companion.read())
            .await
            .expect("queued packet was blocked by idle mode");
        assert_eq!(
            must_some(must_ok(read, "queued read failed"), "stream closed"),
            expected
        );
    }

    #[tokio::test]
    async fn idle_disconnect_waits_until_after_the_handshake_response() {
        let companion = VirtualCompanion::new();
        companion.disconnect_on_next_idle_read();
        let mut client = Client::new(companion.clone());

        let info = must_ok(client.connect().await, "handshake failed");
        assert_eq!(info.public_key.as_bytes()[0], 1);
        assert!(companion.is_connected());

        assert!(matches!(
            client.next_event().await,
            Err(CoreError::Disconnected)
        ));
        assert!(!companion.is_connected());
        assert_eq!(companion.outbound_packets(), [APP_START_COMMAND.to_vec()]);
    }

    #[tokio::test]
    async fn direct_send_disconnect_is_one_shot_and_happens_before_acceptance() {
        let companion = VirtualCompanion::new();
        let mut client = Client::new(companion.clone());
        let _ = must_ok(client.connect().await, "handshake failed");
        companion.disconnect_before_next_direct_send();

        assert!(matches!(
            client.send_direct_text(&[0x22; 6], 0, "known unsent").await,
            Err(CoreError::Transport(TransportError::Closed))
        ));
        assert_eq!(companion.outbound_packets(), [APP_START_COMMAND.to_vec()]);

        let _ = must_ok(client.reconnect().await, "reconnect failed");
        let _ = must_ok(
            client
                .send_direct_text(&[0x22; 6], 0, "deliberate retry")
                .await,
            "retry failed",
        );
        let direct_sends = companion
            .outbound_packets()
            .into_iter()
            .filter(|packet| {
                packet.first().copied().is_some_and(|command| {
                    matches!(CommandCode::try_from(command), Ok(CommandCode::SendTxtMsg))
                })
            })
            .count();
        assert_eq!(direct_sends, 1);
    }

    #[tokio::test]
    async fn configured_reconnect_failures_are_exact_and_count_every_attempt() {
        let companion = VirtualCompanion::new();
        let mut client = Client::new(companion.clone());
        let _ = must_ok(client.connect().await, "handshake failed");
        companion.fail_next_reconnects(2);

        for expected_count in 1..=2 {
            let error = client
                .reconnect()
                .await
                .expect_err("configured reconnect should fail");
            assert!(matches!(
                error,
                CoreError::Transport(TransportError::ReconnectFailed { .. })
            ));
            assert_eq!(companion.reconnect_count(), expected_count);
            assert!(!companion.is_connected());
        }

        let info = must_ok(client.reconnect().await, "third reconnect should succeed");
        assert_eq!(info.public_key.as_bytes()[0], 1);
        assert_eq!(companion.reconnect_count(), 3);
        assert!(companion.is_connected());
    }

    #[tokio::test]
    async fn successful_reconnect_delivers_the_retained_push_before_self_info() {
        const RECONNECTED_MESSAGE: &str = "live message after deterministic reconnect";

        let companion = VirtualCompanion::new();
        companion.disconnect_on_next_idle_read();
        let mut client = Client::new(companion.clone());
        let _ = must_ok(client.connect().await, "handshake failed");
        assert!(matches!(
            client.next_event().await,
            Err(CoreError::Disconnected)
        ));

        assert!(matches!(
            companion.set_next_reconnect_push(vec![0; MAX_INNER_PAYLOAD + 1]),
            Err(VirtualCompanionError::PacketTooLarge { .. })
        ));
        must_ok(
            companion.set_next_reconnect_push(must_ok(
                make_direct_message_packet(u8::MAX, "superseded reconnect message"),
                "superseded direct fixture failed",
            )),
            "initial reconnect push failed",
        );
        must_ok(
            companion.set_next_reconnect_push(must_ok(
                make_direct_message_packet(u8::MAX, RECONNECTED_MESSAGE),
                "direct fixture failed",
            )),
            "replacement reconnect push failed",
        );
        companion.fail_next_reconnects(1);

        let mut events = client.subscribe();
        assert!(matches!(
            client.reconnect().await,
            Err(CoreError::Transport(TransportError::ReconnectFailed { .. }))
        ));
        let _ = must_ok(client.reconnect().await, "second reconnect should succeed");

        let pushed = must_ok(events.try_recv(), "missing reconnect push event");
        let Event::Message(message) = pushed else {
            panic!("reconnect push was not published before SELF_INFO");
        };
        assert_eq!(message.text, RECONNECTED_MESSAGE);
        assert!(matches!(
            must_ok(events.try_recv(), "missing reconnect SELF_INFO event"),
            Event::SelfInfo(_)
        ));
    }

    #[tokio::test]
    async fn reconnect_push_uses_the_normal_duplicate_queue_path() {
        let companion = VirtualCompanion::new();
        companion.disconnect_on_next_idle_read();
        let mut client = Client::new(companion.clone());
        let _ = must_ok(client.connect().await, "handshake failed");
        assert!(matches!(
            client.next_event().await,
            Err(CoreError::Disconnected)
        ));

        must_ok(
            companion.set_next_reconnect_push(must_ok(
                make_direct_message_packet(u8::MAX, "duplicated reconnect push"),
                "direct fixture failed",
            )),
            "reconnect push setup failed",
        );
        companion.duplicate_next_inbound_packet();
        let mut events = client.subscribe();
        let _ = must_ok(client.reconnect().await, "reconnect failed");

        for _ in 0..2 {
            assert!(matches!(
                must_ok(events.try_recv(), "missing duplicated reconnect push"),
                Event::Message(_)
            ));
        }
        assert!(matches!(
            must_ok(events.try_recv(), "missing reconnect SELF_INFO event"),
            Event::SelfInfo(_)
        ));
    }

    #[tokio::test]
    async fn failed_reconnect_handshake_does_not_poison_the_next_attempt() {
        let limits = VirtualCompanionCapacities::new(8, 1, 1, 1);
        let companion = VirtualCompanion::with_capacities(limits);
        companion.disconnect_on_next_idle_read();
        let mut client = Client::new(companion.clone());
        let _ = must_ok(client.connect().await, "handshake failed");
        assert!(matches!(
            client.next_event().await,
            Err(CoreError::Disconnected)
        ));
        must_ok(
            companion.set_next_reconnect_push(must_ok(
                make_direct_message_packet(u8::MAX, "fills the one-slot queue"),
                "direct fixture failed",
            )),
            "reconnect push setup failed",
        );

        assert!(matches!(
            client.reconnect().await,
            Err(CoreError::Transport(TransportError::Backpressure {
                queue: "inbound_queue",
                capacity: 1
            }))
        ));
        assert!(!companion.is_connected());

        let _ = must_ok(
            client.reconnect().await,
            "clean reconnect after failed handshake should succeed",
        );
        assert!(companion.is_connected());
    }

    #[tokio::test]
    async fn contact_rows_preserve_fixed_path_layout() {
        let companion = VirtualCompanion::new();
        let public_key = [0x22u8; 32];
        let contact = must_ok(
            make_contact_row(&ContactFixture {
                public_key,
                contact_type: 0,
                route: 0x03,
                path: &[0x10, 0x11, 0x12],
                adv_name: "alice",
                last_advert: 7,
                adv_lat: 1.2,
                adv_lon: -2.4,
                lastmod: 99,
            }),
            "contact fixture failed",
        );
        assert_eq!(contact.len(), CONTACT_PACKET_LEN);
        must_ok(companion.set_contacts(vec![contact]), "set contacts failed");

        let mut client = Client::new(companion.clone());
        let _ = must_ok(client.connect().await, "handshake failed");
        let contacts = must_ok(client.list_contacts(None).await, "contact list failed");

        assert_eq!(contacts.len(), 1);
        assert_eq!(contacts[0].public_key.as_bytes(), &public_key);
        assert_eq!(contacts[0].adv_name, "alice");
        assert_eq!(contacts[0].lastmod, 99);
        match contacts[0].route {
            meshquill_core::domain::ContactRoute::Path {
                hash_mode,
                hop_count,
            } => {
                assert_eq!(hash_mode, 0);
                assert_eq!(hop_count, 3);
            }
            meshquill_core::domain::ContactRoute::Flood => panic!("expected path route"),
        }
    }

    #[tokio::test]
    async fn direct_send_returns_msg_sent_and_configured_ack() {
        let companion = VirtualCompanion::new();
        companion.configure_send_txt_ack([0x90, 0x91, 0x92, 0x93], 2_500, true);

        let mut client = Client::new(companion.clone());
        let _ = must_ok(client.connect().await, "handshake failed");
        let tracking = must_ok(
            client
                .send_direct_text(&[0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff], 1, "test")
                .await,
            "direct send failed",
        );

        let mut companion_for_read = companion.clone();
        let ack_raw = must_some(
            must_ok(companion_for_read.read().await, "ACK read failed"),
            "ACK stream closed",
        );
        let ack = must_ok(CorePacket::parse(&ack_raw), "ACK parse failed");

        assert_eq!(tracking.ack_code, [0x90, 0x91, 0x92, 0x93]);
        assert!(matches!(ack, CorePacket::Ack(_)));
    }

    #[tokio::test]
    async fn remote_login_success_failure_and_connection_state() {
        let companion = VirtualCompanion::new();
        let key = [0x11_u8; 32];

        let mut client = Client::new(companion.clone());
        let _ = must_ok(client.connect().await, "connect failed");

        assert!(!must_ok(
            client.has_connection(&key).await,
            "has_connection failed"
        ));

        let failed = client.login(&key, "wrong-password").await;
        assert!(matches!(
            failed,
            Err(meshquill_core::CoreError::AuthenticationFailed)
        ));

        let session = must_ok(
            client.login(&key, DEFAULT_REMOTE_PASSWORD).await,
            "login failed",
        );
        assert!(session.is_admin());
        assert_eq!(session.pubkey_prefix, [0x11; 6]);
        assert_eq!(session.server_timestamp, Some(DEFAULT_REMOTE_SESSION_CLOCK));
        assert_eq!(
            session.acl_permissions,
            Some(DEFAULT_REMOTE_SESSION_ACL_PERMISSIONS)
        );
        assert_eq!(
            session.firmware_version_level,
            Some(DEFAULT_REMOTE_SESSION_FIRMWARE_LEVEL)
        );

        assert!(must_ok(
            client.has_connection(&key).await,
            "has_connection check failed"
        ));

        must_ok(client.logout(&key).await, "logout failed");
        assert!(!must_ok(
            client.has_connection(&key).await,
            "post-logout check failed"
        ));
        assert!(matches!(
            client.logout(&key).await,
            Err(meshquill_core::CoreError::DeviceRejected { code: Some(2), .. })
        ));
    }

    #[tokio::test]
    async fn remote_status_is_reported_when_authenticated() {
        let companion = VirtualCompanion::new();
        let key = [0x22_u8; 32];

        let mut client = Client::new(companion.clone());
        let _ = must_ok(client.connect().await, "connect failed");
        assert!(matches!(
            client.remote_status(&key).await,
            Err(meshquill_core::CoreError::DeviceRejected { code: Some(2), .. })
        ));

        must_ok(
            client.login(&key, DEFAULT_REMOTE_PASSWORD).await,
            "login failed",
        );
        let status = must_ok(
            client.remote_status(&key).await,
            "remote status request failed",
        );
        assert_eq!(status.pubkey_prefix, [0x22; 6]);
        assert_eq!(status.battery_mv, DEFAULT_REMOTE_STATUS_BATTERY_MV);
    }

    #[tokio::test]
    async fn binary_and_anonymous_requests_return_correlated_responses() {
        let companion = VirtualCompanion::new();
        let key = [0x33_u8; 32];

        let mut client = Client::new(companion.clone());
        let _ = must_ok(client.connect().await, "connect failed");
        let empty_path = Path::try_from_bytes(&[]).expect("empty path fixture");
        let regions = must_ok(
            client
                .anonymous_request(
                    &key,
                    1,
                    meshquill_core::domain::ContactRoute::Path {
                        hash_mode: 0,
                        hop_count: 0,
                    },
                    &empty_path,
                )
                .await,
            "anonymous unauthenticated request failed",
        );
        assert!(parse_regions_response(&regions.payload).is_ok());

        must_ok(
            client.login(&key, DEFAULT_REMOTE_PASSWORD).await,
            "login failed",
        );

        let telemetry = must_ok(
            client.binary_request(&key, 3, &[]).await,
            "binary telemetry request failed",
        );
        assert_eq!(telemetry.tag, DEFAULT_ACK_CODE);
        assert_eq!(
            parse_telemetry_payload(&telemetry.payload)
                .expect("parse telemetry")
                .len(),
            2
        );

        let summary = must_ok(
            client.binary_request(&key, 4, &[]).await,
            "binary summary request failed",
        );
        assert_eq!(summary.tag, DEFAULT_ACK_CODE);

        let reply_path = Path::try_from_bytes(&[0x99]).expect("path fixture");
        let basic = must_ok(
            client
                .anonymous_request(
                    &key,
                    3,
                    meshquill_core::domain::ContactRoute::Path {
                        hash_mode: 0,
                        hop_count: 1,
                    },
                    &reply_path,
                )
                .await,
            "anonymous request failed",
        );
        assert_eq!(basic.tag, DEFAULT_ACK_CODE);
        assert!(parse_basic_response(&basic.payload).is_ok());

        let owner_payload = must_ok(
            client
                .anonymous_request(
                    &key,
                    2,
                    meshquill_core::domain::ContactRoute::Path {
                        hash_mode: 0,
                        hop_count: 1,
                    },
                    &reply_path,
                )
                .await,
            "owner request failed",
        );
        assert!(parse_owner_response(&owner_payload.payload).is_ok());
        assert_eq!(owner_payload.tag, DEFAULT_ACK_CODE);
    }

    #[tokio::test]
    async fn channel_send_returns_ok() {
        let companion = VirtualCompanion::new();
        let mut client = Client::new(companion.clone());
        let _ = must_ok(client.connect().await, "handshake failed");

        must_ok(
            client.send_channel_message(3, 0x01, "hello").await,
            "channel send failed",
        );

        let outbound = companion.outbound_packets();
        let sent = must_some(
            outbound
                .into_iter()
                .find(|payload| payload.first() == Some(&CommandCode::SendChannelTxtMsg.to_u8())),
            "missing send channel command",
        );
        assert_eq!(sent[0], CommandCode::SendChannelTxtMsg.to_u8());
    }

    #[tokio::test]
    async fn sync_message_queue_exhaustion_is_reported() {
        let limits = VirtualCompanionCapacities::new(8, 4, 1, 1);
        let companion = VirtualCompanion::with_capacities(limits);
        let first = must_ok(
            make_direct_message_packet(0xff, "first"),
            "direct fixture failed",
        );
        must_ok(
            companion.push_sync_message(first),
            "initial sync enqueue failed",
        );
        let second = must_ok(
            make_direct_message_packet(0xff, "second"),
            "direct fixture failed",
        );
        let result = companion.push_sync_message(second);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn duplicate_inbound_packets_and_reconnect_count() {
        let companion = VirtualCompanion::new();
        let mut client = Client::new(companion.clone());
        let _ = must_ok(client.connect().await, "handshake failed");

        companion.set_next_read_fault(VirtualCompanionFault::CleanDisconnect);
        assert!(client.query_device_info().await.is_err());

        let before = companion.reconnect_count();
        let _ = must_ok(client.reconnect().await, "reconnect failed");
        assert_eq!(companion.reconnect_count(), before + 1);

        companion.duplicate_next_inbound_packet();
        let duplicate_fixture = must_ok(
            make_channel_message_packet(1, "dupe"),
            "channel fixture failed",
        );
        must_ok(
            companion.enqueue_push(duplicate_fixture),
            "duplicate push failed",
        );
        let mut companion_for_read = companion.clone();
        let duplicated = must_some(
            must_ok(
                companion_for_read.read().await,
                "first duplicate read failed",
            ),
            "first duplicate stream closed",
        );
        let parsed = must_ok(
            CorePacket::parse(&duplicated),
            "first duplicate parse failed",
        );
        assert!(matches!(parsed, CorePacket::ChannelMsg(_)));

        // confirm duplicated frame type again.
        let duplicated2 = must_some(
            must_ok(
                companion_for_read.read().await,
                "second duplicate read failed",
            ),
            "second duplicate stream closed",
        );
        let parsed2 = must_ok(
            CorePacket::parse(&duplicated2),
            "second duplicate parse failed",
        );
        assert!(matches!(parsed2, CorePacket::ChannelMsg(_)));
    }

    #[tokio::test]
    async fn direct_sync_queue_delivers_messages_then_no_more() {
        let companion = VirtualCompanion::new();
        let direct = must_ok(
            make_direct_message_packet(0xff, "direct-1"),
            "direct fixture failed",
        );
        let channel = must_ok(
            make_channel_message_packet(2, "chan-1"),
            "channel fixture failed",
        );
        must_ok(
            companion.set_sync_messages(vec![direct, channel]),
            "sync queue setup failed",
        );

        let mut client = Client::new(companion.clone());
        let _ = must_ok(client.connect().await, "handshake failed");

        let first = must_ok(client.sync_next_message().await, "first sync failed");
        assert!(first.is_some());
        let second = must_ok(client.sync_next_message().await, "second sync failed");
        assert!(second.is_some());
        let third = must_ok(client.sync_next_message().await, "third sync failed");
        assert!(third.is_none());
    }

    #[tokio::test]
    async fn sync_drains_preexisting_notification_before_writing_and_returns_its_response() {
        let companion = VirtualCompanion::new();
        let queued = must_ok(
            make_channel_message_packet(2, "requested queue response"),
            "queued fixture failed",
        );

        let mut client = Client::new(companion.clone());
        let _ = must_ok(client.connect().await, "handshake failed");
        let mut events = client.subscribe();
        must_ok(
            companion.enqueue_push(vec![PacketCode::MessagesWaiting.to_u8()]),
            "message-waiting setup failed",
        );
        must_ok(
            companion.push_sync_message(queued),
            "sync response setup failed",
        );

        let returned = must_some(
            must_ok(client.sync_next_message().await, "sync failed"),
            "sync response missing",
        );
        assert_eq!(returned.text, "requested queue response");

        let first = must_ok(events.try_recv(), "preexisting event missing");
        let second = must_ok(events.try_recv(), "sync event missing");
        assert!(matches!(first, Event::MessagesWaiting));
        let Event::Message(queued) = second else {
            panic!("sync response was not published as a message");
        };
        assert_eq!(queued.observation_id, returned.observation_id);
        assert_eq!(
            companion
                .outbound_packets()
                .iter()
                .filter(|packet| {
                    packet.first().copied() == Some(CommandCode::SyncNextMessage.to_u8())
                })
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn set_and_get_device_time() {
        let companion = VirtualCompanion::new();
        let mut client = Client::new(companion.clone());
        let _ = must_ok(client.connect().await, "handshake failed");

        let target_time = 1_727_000_123_u32;
        must_ok(client.set_time(target_time).await, "set time failed");
        assert_eq!(
            must_ok(client.get_time().await, "get time failed"),
            target_time
        );
    }

    #[tokio::test]
    async fn reboot_and_flood_scope_operations_have_stateful_responses() {
        let companion = VirtualCompanion::new();
        let mut client = Client::new(companion);
        let _ = must_ok(client.connect().await, "handshake failed");

        must_ok(client.reboot().await, "reboot failed");
        let _ = must_ok(client.reconnect().await, "post-reboot reconnect failed");
        assert!(matches!(
            must_ok(
                client.get_default_flood_scope().await,
                "empty default-scope query failed"
            ),
            meshquill_core::DefaultFloodScope::Unconfigured
        ));

        let key = [0x44_u8; 16];
        must_ok(
            client.set_default_flood_scope("field", key).await,
            "default-scope update failed",
        );
        let configured = must_ok(
            client.get_default_flood_scope().await,
            "configured default-scope query failed",
        );
        match configured {
            meshquill_core::DefaultFloodScope::Configured(scope) => {
                assert_eq!(scope.name(), Some("field"));
                assert_eq!(scope.key, key);
            }
            meshquill_core::DefaultFloodScope::Unconfigured => {
                panic!("configured scope was lost")
            }
        }
        must_ok(
            client
                .set_flood_scope(&meshquill_core::FloodScope::Key(key))
                .await,
            "named flood-scope selection failed",
        );
        must_ok(
            client
                .set_flood_scope(&meshquill_core::FloodScope::Unscoped)
                .await,
            "unscoped flood selection failed",
        );
        must_ok(
            client.clear_default_flood_scope().await,
            "default-scope clear failed",
        );
        assert!(matches!(
            must_ok(
                client.get_default_flood_scope().await,
                "cleared default-scope query failed"
            ),
            meshquill_core::DefaultFloodScope::Unconfigured
        ));
    }

    #[tokio::test]
    async fn path_discovery_returns_the_selected_contact_route() {
        let companion = VirtualCompanion::new();
        let public_key = [0x45_u8; 32];
        let row = must_ok(
            make_contact_row(&ContactFixture {
                public_key,
                contact_type: 0,
                route: 0x02,
                path: &[0x12, 0x34],
                adv_name: "path-target",
                last_advert: 1,
                adv_lat: 0.0,
                adv_lon: 0.0,
                lastmod: 1,
            }),
            "contact fixture failed",
        );
        must_ok(companion.set_contacts([row]), "contact setup failed");
        let mut client = Client::new(companion);
        let _ = must_ok(client.connect().await, "handshake failed");

        let discovered = must_ok(
            client.discover_path(&public_key).await,
            "path discovery failed",
        );
        assert_eq!(discovered.pubkey_prefix, public_key[..6]);
        assert_eq!(discovered.outbound_path.as_bytes(), &[0x12, 0x34]);
        assert_eq!(discovered.inbound_path.as_bytes(), &[0x12, 0x34]);
    }

    #[tokio::test]
    async fn node_discovery_returns_ok_then_one_correlated_control_event() {
        let companion = VirtualCompanion::new();
        let mut client = Client::new(companion.clone());
        let _ = must_ok(client.connect().await, "handshake failed");
        let tag = 0x1234_5678;

        must_ok(
            client
                .send_node_discovery(0x04, true, tag, Some(0xa1b2_c3d4))
                .await,
            "node discovery request failed",
        );
        let event = must_some(
            must_ok(client.next_event().await, "node discovery event failed"),
            "node discovery did not emit an event",
        );
        let meshquill_core::Event::ControlData(data) = event else {
            panic!("unexpected node discovery event: {event:?}");
        };
        assert_eq!(data.snr_qdb, DEFAULT_NODE_DISCOVERY_SNR_QDB);
        assert_eq!(data.rssi, DEFAULT_NODE_DISCOVERY_RSSI);
        let response = match data.node_discovery_response() {
            Ok(Some(response)) => response,
            Ok(None) => panic!("control event was not a node discovery response"),
            Err(error) => panic!("node discovery response was malformed: {error}"),
        };
        assert_eq!(response.tag, tag);
        assert_eq!(response.node_type, DEFAULT_NODE_DISCOVERY_TYPE);
        assert_eq!(
            response.inbound_snr_qdb,
            DEFAULT_NODE_DISCOVERY_INBOUND_SNR_QDB
        );
        assert_eq!(response.public_key, DEFAULT_NODE_DISCOVERY_KEY[..8]);

        let outbound = companion.outbound_packets();
        assert_eq!(outbound.len(), 2);
        assert_eq!(
            outbound[1],
            vec![
                0x37, 0x81, 0x04, 0x78, 0x56, 0x34, 0x12, 0xd4, 0xc3, 0xb2, 0xa1,
            ]
        );
    }

    #[tokio::test]
    async fn malformed_command_shapes_are_reported_as_error_packets() {
        let mut companion = VirtualCompanion::new();
        let mut client = Client::new(companion.clone());
        let _ = must_ok(client.connect().await, "handshake failed");

        must_ok(
            companion
                .write(&[CommandCode::GetDeviceTime.to_u8(), 0x00])
                .await,
            "bad time read write failed",
        );
        let malformed_time = must_some(
            must_ok(companion.read().await, "bad time read failed"),
            "bad time read stream closed",
        );
        assert!(matches!(
            must_ok(CorePacket::parse(&malformed_time), "bad time parse failed"),
            CorePacket::Error(Some(1))
        ));

        must_ok(
            companion
                .write(&[CommandCode::SetDeviceTime.to_u8(), 0x00, 0x00, 0x00])
                .await,
            "bad time write failed",
        );
        let malformed_set_time = must_some(
            must_ok(companion.read().await, "bad set-time read failed"),
            "bad set-time read stream closed",
        );
        assert!(matches!(
            must_ok(
                CorePacket::parse(&malformed_set_time),
                "bad set-time parse failed"
            ),
            CorePacket::Error(Some(1))
        ));

        must_ok(
            companion
                .write(&[CommandCode::SendSelfAdvert.to_u8(), 0x02])
                .await,
            "bad advert write failed",
        );
        let malformed_self_advert = must_some(
            must_ok(companion.read().await, "bad advert read failed"),
            "bad advert read stream closed",
        );
        assert!(matches!(
            must_ok(
                CorePacket::parse(&malformed_self_advert),
                "bad advert parse failed"
            ),
            CorePacket::Error(Some(1))
        ));

        must_ok(
            companion.write(&[CommandCode::SetChannel.to_u8(), 3]).await,
            "bad set-channel write failed",
        );
        let malformed_set_channel = must_some(
            must_ok(companion.read().await, "bad set-channel read failed"),
            "bad set-channel read stream closed",
        );
        assert!(matches!(
            must_ok(
                CorePacket::parse(&malformed_set_channel),
                "bad set-channel parse failed"
            ),
            CorePacket::Error(Some(1))
        ));

        must_ok(
            companion
                .write(&[CommandCode::SendTelemetryReq.to_u8(), 1, 0, 0])
                .await,
            "bad telemetry write failed",
        );
        let malformed_telemetry = must_some(
            must_ok(companion.read().await, "bad telemetry read failed"),
            "bad telemetry read stream closed",
        );
        assert!(matches!(
            must_ok(
                CorePacket::parse(&malformed_telemetry),
                "bad telemetry parse failed"
            ),
            CorePacket::Error(Some(1))
        ));

        must_ok(
            companion
                .write(&[CommandCode::SendControlData.to_u8(), 0x80, 0xff, 0, 0, 0, 0])
                .await,
            "bad node-discovery write failed",
        );
        let malformed_discovery = must_some(
            must_ok(companion.read().await, "bad node-discovery read failed"),
            "bad node-discovery stream closed",
        );
        assert!(matches!(
            must_ok(
                CorePacket::parse(&malformed_discovery),
                "bad node-discovery parse failed"
            ),
            CorePacket::Error(Some(1))
        ));
    }

    #[tokio::test]
    async fn send_self_advert_can_be_flood_or_nonflood() {
        let companion = VirtualCompanion::new();
        let mut client = Client::new(companion.clone());
        let _ = must_ok(client.connect().await, "handshake failed");

        must_ok(
            client.send_self_advert(false).await,
            "non-flood advert failed",
        );
        must_ok(client.send_self_advert(true).await, "flood advert failed");
    }

    #[tokio::test]
    async fn telemetry_and_battery_queries_return_deterministic_payloads() {
        let companion = VirtualCompanion::new();
        let mut client = Client::new(companion.clone());
        let _ = must_ok(client.connect().await, "handshake failed");

        let battery = must_ok(client.get_battery().await, "battery query failed");
        assert_eq!(battery.level, DEFAULT_BATTERY_LEVEL);
        assert_eq!(battery.used_kb, Some(DEFAULT_BATTERY_USED_KB));
        assert_eq!(battery.total_kb, Some(DEFAULT_BATTERY_TOTAL_KB));

        let telemetry = must_ok(client.get_self_telemetry().await, "telemetry query failed");
        assert_eq!(telemetry.pubkey_prefix, DEFAULT_TELEMETRY_PREFIX);
        assert_eq!(telemetry.payload, DEFAULT_TELEMETRY_PAYLOAD.to_vec());
    }

    #[tokio::test]
    async fn channel_queries_reflect_updated_slot_state() {
        let companion = VirtualCompanion::new();
        let mut client = Client::new(companion.clone());
        let _ = must_ok(client.connect().await, "handshake failed");

        let default = must_ok(client.get_channel(0).await, "default channel query failed");
        assert_eq!(default.name, "meshquill-channel");
        assert_eq!(default.secret(), Some(&DEFAULT_CHANNEL_SECRET));

        let new_secret = [0x55_u8; 16];
        must_ok(
            client.set_channel(0, "team", new_secret).await,
            "channel update failed",
        );
        let changed = must_ok(client.get_channel(0).await, "changed channel query failed");
        assert_eq!(changed.name, "team");
        assert_eq!(changed.secret(), Some(&new_secret));

        must_ok(client.clear_channel(0).await, "channel clear failed");
        let cleared = must_ok(client.get_channel(0).await, "cleared channel query failed");
        assert_eq!(cleared.name, "");
        assert_eq!(cleared.secret(), Some(&[0_u8; 16]));
    }

    #[tokio::test]
    async fn contact_query_export_path_reset_and_removal_follow_fixtures() {
        let mut companion = VirtualCompanion::new();
        let public_key = [0x11_u8; 32];
        let expected_contact = must_ok(
            make_contact_row(&ContactFixture {
                public_key,
                contact_type: 0,
                route: 0x03,
                path: &[0x12, 0x34, 0x56],
                adv_name: "bridge",
                last_advert: 7,
                adv_lat: 51.5014,
                adv_lon: -0.1419,
                lastmod: 3,
            }),
            "contact fixture failed",
        );
        must_ok(
            companion.set_contacts(vec![expected_contact.clone()]),
            "contact set failed",
        );
        let mut client = Client::new(companion.clone());
        let _ = must_ok(client.connect().await, "handshake failed");

        let by_key = must_ok(
            client.get_contact(&public_key).await,
            "contact query by key failed",
        );
        assert_eq!(by_key.public_key.as_bytes(), &public_key);
        assert_eq!(by_key.adv_name, "bridge");

        let mut payload = vec![CommandCode::GetContactByKey.to_u8()];
        payload.extend_from_slice(&[0x99_u8; 32]);
        let missing_lookup =
            raw_round_trip(&mut companion, payload, "missing contact lookup").await;
        assert!(matches!(missing_lookup, CorePacket::Error(Some(2))));

        let path = must_ok(
            client.get_advert_path(&public_key).await,
            "advert path query failed",
        );
        match path.route {
            meshquill_core::domain::ContactRoute::Path {
                hash_mode,
                hop_count,
            } => {
                assert_eq!(hash_mode, 0);
                assert_eq!(hop_count, 3);
            }
            meshquill_core::domain::ContactRoute::Flood => {
                panic!("expected non-flood route")
            }
        }
        assert_eq!(path.path.as_bytes(), &[0x12, 0x34, 0x56]);

        let exported_self = must_ok(
            client.export_contact(None).await,
            "self contact export failed",
        );
        assert_eq!(exported_self.card.len(), 98);
        assert_eq!(&exported_self.card[..32], &DEFAULT_SELF_KEY);
        let exported = must_ok(
            client.export_contact(Some(&public_key)).await,
            "contact export failed",
        );
        assert_eq!(exported.card, expected_contact);

        let mut payload = vec![CommandCode::ResetPath.to_u8()];
        payload.extend_from_slice(&public_key);
        assert!(matches!(
            raw_round_trip(&mut companion, payload, "reset path").await,
            CorePacket::Ok(None)
        ));

        let path = must_ok(
            client.get_advert_path(&public_key).await,
            "post-reset advert path query failed",
        );
        assert!(matches!(
            path.route,
            meshquill_core::domain::ContactRoute::Flood
        ));

        let mut payload = vec![CommandCode::RemoveContact.to_u8()];
        payload.extend_from_slice(&public_key);
        assert!(matches!(
            raw_round_trip(&mut companion, payload, "remove contact").await,
            CorePacket::Ok(None)
        ));

        let post_remove = must_ok(client.list_contacts(None).await, "post-remove list failed");
        assert!(post_remove.is_empty());

        let mut payload = vec![CommandCode::GetContactByKey.to_u8()];
        payload.extend_from_slice(&public_key);
        assert!(matches!(
            raw_round_trip(&mut companion, payload, "removed contact lookup").await,
            CorePacket::Error(Some(2))
        ));
    }

    #[tokio::test]
    async fn contact_update_and_share_mutate_only_the_selected_record() {
        let companion = VirtualCompanion::new();
        let public_key = [0x21_u8; 32];
        let row = must_ok(
            make_contact_row(&ContactFixture {
                public_key,
                contact_type: 0,
                route: 0x02,
                path: &[0x10, 0x20],
                adv_name: "before",
                last_advert: 4,
                adv_lat: 1.0,
                adv_lon: 2.0,
                lastmod: 3,
            }),
            "contact fixture failed",
        );
        must_ok(companion.set_contacts(vec![row]), "contact set failed");
        let mut client = Client::new(companion);
        let _ = must_ok(client.connect().await, "handshake failed");

        let mut contact = must_ok(
            client.get_contact(&public_key).await,
            "contact query failed",
        );
        contact.adv_name = "after".to_owned();
        contact.flags = 1;
        contact.route = meshquill_core::ContactRoute::Flood;
        contact.out_path = must_ok(
            meshquill_core::Path::try_from_bytes(&[]),
            "empty path construction failed",
        );
        contact.adv_lat = 12.5;
        contact.adv_lon = -45.25;
        must_ok(
            client.update_contact(&contact).await,
            "contact update failed",
        );

        let changed = must_ok(
            client.get_contact(&public_key).await,
            "updated query failed",
        );
        assert_eq!(changed.adv_name, "after");
        assert_eq!(changed.flags, 1);
        assert!(matches!(changed.route, meshquill_core::ContactRoute::Flood));
        assert!(changed.out_path.as_bytes().is_empty());
        assert_eq!(changed.lastmod, DEFAULT_DEVICE_TIME_SECONDS);
        must_ok(
            client.share_contact(&public_key).await,
            "contact share failed",
        );
        assert!(matches!(
            client.share_contact(&[0x99_u8; 32]).await,
            Err(meshquill_core::CoreError::DeviceRejected { code: Some(2), .. })
        ));
    }

    #[tokio::test]
    async fn import_contact_rejects_invalid_cards_and_restores_mock_exports() {
        let mut companion = VirtualCompanion::new();
        let mut client = Client::new(companion.clone());
        let _ = must_ok(client.connect().await, "handshake failed");

        must_ok(
            companion
                .write(&[CommandCode::ImportContact.to_u8(), 0x00])
                .await,
            "short import write failed",
        );
        let short_import = must_some(
            must_ok(companion.read().await, "short import read failed"),
            "short import stream closed",
        );
        assert!(matches!(
            must_ok(
                CorePacket::parse(&short_import),
                "short import parse failed"
            ),
            CorePacket::Error(Some(1))
        ));

        let mut payload = vec![CommandCode::ImportContact.to_u8()];
        payload.extend_from_slice(&[0x51_u8; 98]);
        must_ok(companion.write(&payload).await, "valid import write failed");
        let valid_import = must_some(
            must_ok(companion.read().await, "valid import read failed"),
            "valid import stream closed",
        );
        assert!(matches!(
            must_ok(
                CorePacket::parse(&valid_import),
                "valid import parse failed"
            ),
            CorePacket::Error(Some(6))
        ));

        let public_key = [0x31_u8; 32];
        let row = must_ok(
            make_contact_row(&ContactFixture {
                public_key,
                contact_type: 0,
                route: u8::MAX,
                path: &[],
                adv_name: "round-trip",
                last_advert: 9,
                adv_lat: 0.0,
                adv_lon: 0.0,
                lastmod: 8,
            }),
            "round-trip fixture failed",
        );
        must_ok(companion.set_contacts(vec![row]), "contact set failed");
        let exported = must_ok(
            client.export_contact(Some(&public_key)).await,
            "contact export failed",
        );
        must_ok(
            client.remove_contact(&public_key).await,
            "contact removal failed",
        );
        must_ok(
            client.import_contact(&exported.card).await,
            "contact reimport failed",
        );
        let restored = must_ok(
            client.get_contact(&public_key).await,
            "restored query failed",
        );
        assert_eq!(restored.adv_name, "round-trip");
    }

    #[tokio::test]
    async fn bounded_transport_reports_backpressure_without_evicting_packets() {
        let limits = VirtualCompanionCapacities::new(1, 1, 1, 1);
        let mut companion = VirtualCompanion::with_capacities(limits);
        must_ok(companion.connect().await, "transport connect failed");
        must_ok(
            companion.enqueue_push(vec![PacketCode::NoMoreMsgs.to_u8()]),
            "inbound setup failed",
        );

        let inbound_full = companion.write(DEVICE_QUERY_COMMAND).await;
        assert!(matches!(
            inbound_full,
            Err(TransportError::Backpressure {
                queue: "inbound_queue",
                capacity: 1
            })
        ));
        assert!(companion.outbound_packets().is_empty());
        let preserved = must_some(
            must_ok(companion.read().await, "preserved packet read failed"),
            "preserved packet stream closed",
        );
        assert_eq!(preserved, vec![PacketCode::NoMoreMsgs.to_u8()]);

        must_ok(companion.write(&[0xff]).await, "first write failed");
        let outbound_full = companion.write(&[0xfe]).await;
        assert!(matches!(
            outbound_full,
            Err(TransportError::Backpressure {
                queue: "outbound_queue",
                capacity: 1
            })
        ));
        assert_eq!(companion.outbound_packets(), vec![vec![0xff]]);
    }

    #[tokio::test]
    async fn node_discovery_preflights_ok_and_control_response_capacity() {
        let limits = VirtualCompanionCapacities::new(1, 1, 1, 1);
        let mut companion = VirtualCompanion::with_capacities(limits);
        must_ok(companion.connect().await, "transport connect failed");
        let request = [0x37, 0x81, 0x04, 0x78, 0x56, 0x34, 0x12];

        assert!(matches!(
            companion.write(&request).await,
            Err(TransportError::Backpressure {
                queue: "inbound_queue",
                capacity: 1
            })
        ));
        assert!(companion.outbound_packets().is_empty());
        assert!(matches!(
            companion.read().await,
            Err(TransportError::Timeout)
        ));
    }

    #[test]
    fn fixture_builders_reject_inconsistent_or_oversized_data() {
        let invalid_path = ContactFixture {
            public_key: [0x33; 32],
            contact_type: 0,
            route: 0x03,
            path: &[0x10, 0x11],
            adv_name: "invalid",
            last_advert: 0,
            adv_lat: 0.0,
            adv_lon: 0.0,
            lastmod: 0,
        };
        assert!(matches!(
            make_contact_row(&invalid_path),
            Err(VirtualCompanionError::InvalidContactPathLength {
                expected: 3,
                actual: 2
            })
        ));

        let invalid_coordinate = ContactFixture {
            path: &[0x10, 0x11, 0x12],
            adv_lat: f64::NAN,
            ..invalid_path
        };
        assert!(matches!(
            make_contact_row(&invalid_coordinate),
            Err(VirtualCompanionError::InvalidCoordinate { field: "adv_lat" })
        ));

        let oversized_text = "x".repeat(MAX_INNER_PAYLOAD);
        assert!(matches!(
            make_direct_message_packet(0xff, &oversized_text),
            Err(VirtualCompanionError::PacketTooLarge { .. })
        ));
    }
}

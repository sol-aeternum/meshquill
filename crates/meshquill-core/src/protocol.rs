use std::convert::TryFrom;
use std::fmt;

use serde::{Deserialize, Serialize};
use zeroize::Zeroize;

use crate::domain::{
    Ack, AdvertPath, AutoAddConfig, BatteryInfo, BinaryResponse, ChannelInfo, Contact,
    ContactRoute, ContactType, ContactUri, ControlData, CustomVariable, CustomVariables,
    DefaultFloodScope, DeviceInfo, DeviceStats, Event, FloodScope, FrequencyRange, LoginSession,
    Message, MessageRoute, MessageSource, MessageStatus, Path, PathDiscovery, PrivateKeyMaterial,
    PublicKey, RadioParams, RemoteStatus, Scope, SelfInfo, Signature, StatsType, TelemetryResponse,
    TuningParams,
};
use crate::error::{CoreError, PacketDisplay, ParseError};

/// Maximum payload size for one companion-protocol packet accepted by current firmware.
///
/// This is intentionally smaller than the defensive serial/TCP declared-frame bound. Current
/// `MeshCore` firmware queues at most 176 bytes for one logical companion frame on every transport.
pub const MAX_INNER_PAYLOAD: usize = 176;

/// Command selector sent to companion.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum CommandCode {
    /// Start companion session handshaking.
    AppStart = 0x01,
    /// Send direct text.
    SendTxtMsg = 0x02,
    /// Send direct channel text.
    SendChannelTxtMsg = 0x03,
    /// Fetch contact list.
    GetContacts = 0x04,
    /// Query current device time.
    GetDeviceTime = 0x05,
    /// Set device time.
    SetDeviceTime = 0x06,
    /// Send self-advertisement.
    SendSelfAdvert = 0x07,
    /// Set advertised name.
    SetAdvertName = 0x08,
    /// Add or update a contact.
    AddUpdateContact = 0x09,
    /// Synchronize the next pending message.
    SyncNextMessage = 0x0a,
    /// Update radio parameters.
    SetRadioParams = 0x0b,
    /// Update tx power.
    SetTxPower = 0x0c,
    /// Reset routing/path state.
    ResetPath = 0x0d,
    /// Set advertised latitude and longitude.
    SetAdvertLatLon = 0x0e,
    /// Remove a stored contact.
    RemoveContact = 0x0f,
    /// Share contact blob.
    ShareContact = 0x10,
    /// Export contact blob.
    ExportContact = 0x11,
    /// Import contact blob.
    ImportContact = 0x12,
    /// Reboot companion.
    Reboot = 0x13,
    /// Read battery and storage metrics.
    GetBattAndStorage = 0x14,
    /// Update tuning parameters.
    SetTuningParams = 0x15,
    /// Query a device by index or id.
    DeviceQuery = 0x16,
    /// Export private key material.
    ExportPrivateKey = 0x17,
    /// Import private key material.
    ImportPrivateKey = 0x18,
    /// Send raw payload.
    SendRawData = 0x19,
    /// Send login packet.
    SendLogin = 0x1a,
    /// Send status request.
    SendStatusReq = 0x1b,
    /// Query whether connection exists.
    HasConnection = 0x1c,
    /// Logout command.
    Logout = 0x1d,
    /// Query contact by public key.
    GetContactByKey = 0x1e,
    /// Read channel details.
    GetChannel = 0x1f,
    /// Set active channel.
    SetChannel = 0x20,
    /// Start signature session.
    SignStart = 0x21,
    /// Add signature data.
    SignData = 0x22,
    /// Finish signature session.
    SignFinish = 0x23,
    /// Send trace path payload.
    SendTracePath = 0x24,
    /// Configure device PIN.
    SetDevicePin = 0x25,
    /// Set miscellaneous params.
    SetOtherParams = 0x26,
    /// Request telemetry.
    SendTelemetryReq = 0x27,
    /// Read custom variables.
    GetCustomVars = 0x28,
    /// Set a custom variable.
    SetCustomVar = 0x29,
    /// Read advert path.
    GetAdvertPath = 0x2a,
    /// Read tuning parameters.
    GetTuningParams = 0x2b,
    /// Binary request style.
    BinaryReq = 0x32,
    /// Factory reset.
    FactoryReset = 0x33,
    /// Discover route path.
    PathDiscovery = 0x34,
    /// Set flood scope.
    SetFloodScope = 0x36,
    /// Send control-plane data.
    SendControlData = 0x37,
    /// Fetch stats packet.
    GetStats = 0x38,
    /// Send anonymous request.
    SendAnonReq = 0x39,
    /// Set auto-add configuration.
    SetAutoAddConfig = 0x3a,
    /// Get auto-add configuration.
    GetAutoAddConfig = 0x3b,
    /// Get allowed repeat frequency.
    GetAllowedRepeatFreq = 0x3c,
    /// Set path hash mode.
    SetPathHashMode = 0x3d,
    /// Send channel binary data.
    SendChannelData = 0x3e,
    /// Set default flood scope.
    SetDefaultFloodScope = 0x3f,
    /// Read default flood scope.
    GetDefaultFloodScope = 0x40,
    /// Unknown command code.
    Unknown(u8),
}

impl CommandCode {
    /// Converts to byte code.
    #[must_use]
    pub const fn to_u8(self) -> u8 {
        match self {
            Self::AppStart => 0x01,
            Self::SendTxtMsg => 0x02,
            Self::SendChannelTxtMsg => 0x03,
            Self::GetContacts => 0x04,
            Self::GetDeviceTime => 0x05,
            Self::SetDeviceTime => 0x06,
            Self::SendSelfAdvert => 0x07,
            Self::SetAdvertName => 0x08,
            Self::AddUpdateContact => 0x09,
            Self::SyncNextMessage => 0x0a,
            Self::SetRadioParams => 0x0b,
            Self::SetTxPower => 0x0c,
            Self::ResetPath => 0x0d,
            Self::SetAdvertLatLon => 0x0e,
            Self::RemoveContact => 0x0f,
            Self::ShareContact => 0x10,
            Self::ExportContact => 0x11,
            Self::ImportContact => 0x12,
            Self::Reboot => 0x13,
            Self::GetBattAndStorage => 0x14,
            Self::SetTuningParams => 0x15,
            Self::DeviceQuery => 0x16,
            Self::ExportPrivateKey => 0x17,
            Self::ImportPrivateKey => 0x18,
            Self::SendRawData => 0x19,
            Self::SendLogin => 0x1a,
            Self::SendStatusReq => 0x1b,
            Self::HasConnection => 0x1c,
            Self::Logout => 0x1d,
            Self::GetContactByKey => 0x1e,
            Self::GetChannel => 0x1f,
            Self::SetChannel => 0x20,
            Self::SignStart => 0x21,
            Self::SignData => 0x22,
            Self::SignFinish => 0x23,
            Self::SendTracePath => 0x24,
            Self::SetDevicePin => 0x25,
            Self::SetOtherParams => 0x26,
            Self::SendTelemetryReq => 0x27,
            Self::GetCustomVars => 0x28,
            Self::SetCustomVar => 0x29,
            Self::GetAdvertPath => 0x2a,
            Self::GetTuningParams => 0x2b,
            Self::BinaryReq => 0x32,
            Self::FactoryReset => 0x33,
            Self::PathDiscovery => 0x34,
            Self::SetFloodScope => 0x36,
            Self::SendControlData => 0x37,
            Self::GetStats => 0x38,
            Self::SendAnonReq => 0x39,
            Self::SetAutoAddConfig => 0x3a,
            Self::GetAutoAddConfig => 0x3b,
            Self::GetAllowedRepeatFreq => 0x3c,
            Self::SetPathHashMode => 0x3d,
            Self::SendChannelData => 0x3e,
            Self::SetDefaultFloodScope => 0x3f,
            Self::GetDefaultFloodScope => 0x40,
            Self::Unknown(value) => value,
        }
    }
}

impl From<CommandCode> for u8 {
    fn from(value: CommandCode) -> Self {
        value.to_u8()
    }
}

impl TryFrom<u8> for CommandCode {
    type Error = ParseError;

    fn try_from(code: u8) -> Result<Self, Self::Error> {
        Ok(match code {
            0x01 => Self::AppStart,
            0x02 => Self::SendTxtMsg,
            0x03 => Self::SendChannelTxtMsg,
            0x04 => Self::GetContacts,
            0x05 => Self::GetDeviceTime,
            0x06 => Self::SetDeviceTime,
            0x07 => Self::SendSelfAdvert,
            0x08 => Self::SetAdvertName,
            0x09 => Self::AddUpdateContact,
            0x0a => Self::SyncNextMessage,
            0x0b => Self::SetRadioParams,
            0x0c => Self::SetTxPower,
            0x0d => Self::ResetPath,
            0x0e => Self::SetAdvertLatLon,
            0x0f => Self::RemoveContact,
            0x10 => Self::ShareContact,
            0x11 => Self::ExportContact,
            0x12 => Self::ImportContact,
            0x13 => Self::Reboot,
            0x14 => Self::GetBattAndStorage,
            0x15 => Self::SetTuningParams,
            0x16 => Self::DeviceQuery,
            0x17 => Self::ExportPrivateKey,
            0x18 => Self::ImportPrivateKey,
            0x19 => Self::SendRawData,
            0x1a => Self::SendLogin,
            0x1b => Self::SendStatusReq,
            0x1c => Self::HasConnection,
            0x1d => Self::Logout,
            0x1e => Self::GetContactByKey,
            0x1f => Self::GetChannel,
            0x20 => Self::SetChannel,
            0x21 => Self::SignStart,
            0x22 => Self::SignData,
            0x23 => Self::SignFinish,
            0x24 => Self::SendTracePath,
            0x25 => Self::SetDevicePin,
            0x26 => Self::SetOtherParams,
            0x27 => Self::SendTelemetryReq,
            0x28 => Self::GetCustomVars,
            0x29 => Self::SetCustomVar,
            0x2a => Self::GetAdvertPath,
            0x2b => Self::GetTuningParams,
            0x32 => Self::BinaryReq,
            0x33 => Self::FactoryReset,
            0x34 => Self::PathDiscovery,
            0x36 => Self::SetFloodScope,
            0x37 => Self::SendControlData,
            0x38 => Self::GetStats,
            0x39 => Self::SendAnonReq,
            0x3a => Self::SetAutoAddConfig,
            0x3b => Self::GetAutoAddConfig,
            0x3c => Self::GetAllowedRepeatFreq,
            0x3d => Self::SetPathHashMode,
            0x3e => Self::SendChannelData,
            0x3f => Self::SetDefaultFloodScope,
            0x40 => Self::GetDefaultFloodScope,
            _ => Self::Unknown(code),
        })
    }
}

/// Fully encoded companion command packet.
#[derive(Clone)]
pub struct Command {
    /// Packet selector.
    pub code: CommandCode,
    /// Packet payload bytes.
    pub payload: Vec<u8>,
}

impl fmt::Debug for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Command")
            .field("code", &self.code)
            .field("payload_len", &self.payload.len())
            .finish()
    }
}

impl Drop for Command {
    fn drop(&mut self) {
        self.payload.zeroize();
    }
}

impl Command {
    /// Constructs the startup packet.
    /// Encodes `[command][payload]`.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + self.payload.len());
        out.push(self.code.to_u8());
        out.extend_from_slice(&self.payload);
        out
    }

    /// Consumes this command and returns its encoded bytes without retaining a second payload copy.
    #[must_use]
    pub fn into_encoded(mut self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + self.payload.len());
        out.push(self.code.to_u8());
        out.append(&mut self.payload);
        out
    }

    /// Constructs a startup packet.
    #[must_use]
    pub fn app_start() -> Self {
        Self {
            code: CommandCode::AppStart,
            payload: b"\x03      mccli".to_vec(),
        }
    }

    /// Constructs a device query packet.
    #[must_use]
    pub fn device_query() -> Self {
        Self {
            code: CommandCode::DeviceQuery,
            payload: vec![0x03],
        }
    }

    /// Constructs a get-contacts request.
    #[must_use]
    pub fn get_contacts(lastmod: Option<u32>) -> Self {
        let mut payload = Vec::new();
        if let Some(lastmod) = lastmod {
            payload.extend_from_slice(&lastmod.to_le_bytes());
        }
        Self {
            code: CommandCode::GetContacts,
            payload,
        }
    }

    /// Constructs a sync-next-message request.
    #[must_use]
    pub fn sync_next_message() -> Self {
        Self {
            code: CommandCode::SyncNextMessage,
            payload: Vec::new(),
        }
    }

    /// Constructs a device-time query.
    #[must_use]
    pub fn get_time() -> Self {
        Self {
            code: CommandCode::GetDeviceTime,
            payload: Vec::new(),
        }
    }

    /// Constructs a device-time set request.
    #[must_use]
    pub fn set_time(value: u32) -> Self {
        Self {
            code: CommandCode::SetDeviceTime,
            payload: value.to_le_bytes().to_vec(),
        }
    }

    /// Constructs a battery and storage query.
    #[must_use]
    pub fn get_battery() -> Self {
        Self::without_payload(CommandCode::GetBattAndStorage)
    }

    /// Constructs a self-advertisement request.
    #[must_use]
    pub fn send_self_advert(flood: bool) -> Self {
        Self {
            code: CommandCode::SendSelfAdvert,
            payload: if flood { vec![1] } else { Vec::new() },
        }
    }

    /// Constructs an advertised-name update.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidArgument`] when the name is empty, contains a NUL byte, or is
    /// longer than the firmware's 31-byte UTF-8 field.
    pub fn set_advert_name(name: &str) -> Result<Self, CoreError> {
        validate_text_field(name, 31, "name")?;
        if name.as_bytes().contains(&0) {
            return Err(invalid_argument("name", "must not contain a NUL byte"));
        }
        Self::checked(CommandCode::SetAdvertName, name.as_bytes().to_vec())
    }

    /// Constructs an advertised coordinate update.
    ///
    /// Coordinates are encoded as signed millionths of a degree, matching companion firmware.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidArgument`] for non-finite or out-of-range coordinates.
    pub fn set_coordinates(latitude: f64, longitude: f64) -> Result<Self, CoreError> {
        let latitude = scaled_coordinate(latitude, -90.0, 90.0, "latitude")?;
        let longitude = scaled_coordinate(longitude, -180.0, 180.0, "longitude")?;
        let mut payload = Vec::with_capacity(12);
        payload.extend_from_slice(&latitude.to_le_bytes());
        payload.extend_from_slice(&longitude.to_le_bytes());
        payload.extend_from_slice(&0_i32.to_le_bytes());
        Self::checked(CommandCode::SetAdvertLatLon, payload)
    }

    /// Constructs a transmit-power update.
    ///
    /// The portable lower limit is -9 dBm. The upper limit is board-specific and is therefore
    /// confirmed by the companion response rather than guessed by the host.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidArgument`] when `power_dbm` is below -9 dBm.
    pub fn set_tx_power(power_dbm: i8) -> Result<Self, CoreError> {
        if power_dbm < -9 {
            return Err(invalid_argument("power_dbm", "must be at least -9 dBm"));
        }
        Self::checked(CommandCode::SetTxPower, vec![power_dbm.cast_unsigned()])
    }

    /// Constructs a radio-parameter update.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidArgument`] when a firmware-defined radio range is violated.
    pub fn set_radio_params(params: &RadioParams) -> Result<Self, CoreError> {
        validate_range(params.frequency_khz, 150_000, 2_500_000, "frequency_khz")?;
        validate_range(params.bandwidth_hz, 7_000, 500_000, "bandwidth_hz")?;
        validate_range(params.spreading_factor, 5, 12, "spreading_factor")?;
        validate_range(params.coding_rate, 5, 8, "coding_rate")?;

        let mut payload = Vec::with_capacity(if params.repeat.is_some() { 11 } else { 10 });
        payload.extend_from_slice(&params.frequency_khz.to_le_bytes());
        payload.extend_from_slice(&params.bandwidth_hz.to_le_bytes());
        payload.push(params.spreading_factor);
        payload.push(params.coding_rate);
        if let Some(repeat) = params.repeat {
            payload.push(u8::from(repeat));
        }
        Self::checked(CommandCode::SetRadioParams, payload)
    }

    /// Constructs a tuning-parameter update.
    #[must_use]
    pub fn set_tuning(params: TuningParams) -> Self {
        let mut payload = Vec::with_capacity(10);
        payload.extend_from_slice(&params.rx_delay.to_le_bytes());
        payload.extend_from_slice(&params.airtime_factor.to_le_bytes());
        payload.extend_from_slice(&[0, 0]);
        Self {
            code: CommandCode::SetTuningParams,
            payload,
        }
    }

    /// Constructs a tuning-parameter query.
    #[must_use]
    pub fn get_tuning() -> Self {
        Self::without_payload(CommandCode::GetTuningParams)
    }

    /// Constructs a path reset for one exact public key.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidArgument`] unless `public_key` is exactly 32 bytes.
    pub fn reset_path(public_key: &[u8]) -> Result<Self, CoreError> {
        Self::command_with_key(CommandCode::ResetPath, public_key, Vec::new())
    }

    /// Constructs a complete add/update contact frame from one validated domain record.
    ///
    /// The firmware update command replaces the contact metadata supplied in this frame. Callers
    /// should therefore start from a freshly queried contact and change only intentional fields.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidArgument`] for an invalid name, route/path mismatch, path above
    /// the firmware's 64-byte contact limit, or unrepresentable coordinates.
    pub fn update_contact(contact: &Contact) -> Result<Self, CoreError> {
        validate_text_field(&contact.adv_name, 31, "name")?;
        if contact.adv_name.as_bytes().contains(&0) {
            return Err(invalid_argument("name", "must not contain a NUL byte"));
        }
        let descriptor = encode_contact_route(contact.route, &contact.out_path)?;
        let latitude = scaled_coordinate(contact.adv_lat, -90.0, 90.0, "latitude")?;
        let longitude = scaled_coordinate(contact.adv_lon, -180.0, 180.0, "longitude")?;

        let mut payload = Vec::with_capacity(143);
        payload.extend_from_slice(contact.public_key.as_bytes());
        payload.push(contact_type_to_u8(contact.contact_type));
        payload.push(contact.flags);
        payload.push(descriptor);
        payload.extend_from_slice(contact.out_path.as_bytes());
        payload.resize(32 + 1 + 1 + 1 + 64, 0);
        payload.extend_from_slice(contact.adv_name.as_bytes());
        payload.resize(32 + 1 + 1 + 1 + 64 + 32, 0);
        payload.extend_from_slice(&contact.last_advert.to_le_bytes());
        payload.extend_from_slice(&latitude.to_le_bytes());
        payload.extend_from_slice(&longitude.to_le_bytes());
        Self::checked(CommandCode::AddUpdateContact, payload)
    }

    /// Constructs an exact-key contact removal.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidArgument`] unless `public_key` is exactly 32 bytes.
    pub fn remove_contact(public_key: &[u8]) -> Result<Self, CoreError> {
        Self::command_with_key(CommandCode::RemoveContact, public_key, Vec::new())
    }

    /// Constructs a contact share request for one exact public key.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidArgument`] unless `public_key` is exactly 32 bytes.
    pub fn share_contact(public_key: &[u8]) -> Result<Self, CoreError> {
        Self::command_with_key(CommandCode::ShareContact, public_key, Vec::new())
    }

    /// Constructs a contact export request, or a self-export when no key is supplied.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidArgument`] unless a supplied key is exactly 32 bytes.
    pub fn export_contact(public_key: Option<&[u8]>) -> Result<Self, CoreError> {
        match public_key {
            Some(public_key) => {
                Self::command_with_key(CommandCode::ExportContact, public_key, Vec::new())
            }
            None => Ok(Self::without_payload(CommandCode::ExportContact)),
        }
    }

    /// Constructs a contact-card import.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidArgument`] when the card is shorter than the minimum firmware
    /// advert card or would make the outbound companion packet exceed 176 bytes.
    pub fn import_contact(card: &[u8]) -> Result<Self, CoreError> {
        const MIN_CARD_BYTES: usize = 98;
        if card.len() < MIN_CARD_BYTES {
            return Err(invalid_argument(
                "card",
                format!("must contain at least {MIN_CARD_BYTES} bytes"),
            ));
        }
        Self::checked(CommandCode::ImportContact, card.to_vec())
    }

    /// Constructs an exact-key contact query.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidArgument`] unless `public_key` is exactly 32 bytes.
    pub fn get_contact(public_key: &[u8]) -> Result<Self, CoreError> {
        Self::command_with_key(CommandCode::GetContactByKey, public_key, Vec::new())
    }

    /// Constructs an exact-key advert-path query.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidArgument`] unless `public_key` is exactly 32 bytes.
    pub fn get_advert_path(public_key: &[u8]) -> Result<Self, CoreError> {
        Self::command_with_key(CommandCode::GetAdvertPath, public_key, vec![0])
    }

    /// Constructs an auto-add configuration update.
    ///
    /// All `u8` configuration values are preserved. The optional maximum path length is bounded
    /// to the firmware limit of 64 hops rather than relying on firmware clamping.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidArgument`] when `max_hops` exceeds 64.
    pub fn set_auto_add_config(config: AutoAddConfig) -> Result<Self, CoreError> {
        if config.max_hops.is_some_and(|max_hops| max_hops > 64) {
            return Err(invalid_argument("max_hops", "must be at most 64"));
        }
        let mut payload = vec![config.config];
        if let Some(max_hops) = config.max_hops {
            payload.push(max_hops);
        }
        Self::checked(CommandCode::SetAutoAddConfig, payload)
    }

    /// Constructs an auto-add configuration query.
    #[must_use]
    pub fn get_auto_add_config() -> Self {
        Self::without_payload(CommandCode::GetAutoAddConfig)
    }

    /// Constructs a custom-variable query.
    #[must_use]
    pub fn get_custom_vars() -> Self {
        Self::without_payload(CommandCode::GetCustomVars)
    }

    /// Constructs a strict `key:value` custom-variable update.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidArgument`] when either UTF-8 field is empty, contains a wire
    /// separator or NUL, or when the encoded packet would exceed 176 bytes.
    pub fn set_custom_var(key: &str, value: &str) -> Result<Self, CoreError> {
        validate_custom_component(key, "key")?;
        validate_custom_component(value, "value")?;
        let mut payload = Vec::with_capacity(key.len() + value.len() + 1);
        payload.extend_from_slice(key.as_bytes());
        payload.push(b':');
        payload.extend_from_slice(value.as_bytes());
        Self::checked(CommandCode::SetCustomVar, payload)
    }

    /// Constructs a typed statistics query.
    #[must_use]
    pub fn get_stats(stats_type: StatsType) -> Self {
        let stats_type = match stats_type {
            StatsType::Core => 0,
            StatsType::Radio => 1,
            StatsType::Packets => 2,
        };
        Self {
            code: CommandCode::GetStats,
            payload: vec![stats_type],
        }
    }

    /// Constructs an allowed repeat-frequency query.
    #[must_use]
    pub fn get_allowed_repeat_frequencies() -> Self {
        Self::without_payload(CommandCode::GetAllowedRepeatFreq)
    }

    /// Constructs a path-hash mode update.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidArgument`] because current firmware accepts modes 0, 1 and 2;
    /// mode 3 is reserved.
    pub fn set_path_hash_mode(mode: u8) -> Result<Self, CoreError> {
        if mode > 2 {
            return Err(invalid_argument("mode", "must be 0, 1, or 2"));
        }
        Self::checked(CommandCode::SetPathHashMode, vec![0, mode])
    }

    /// Constructs an outbound flood-scope selection.
    #[must_use]
    pub fn set_flood_scope(scope: &FloodScope) -> Self {
        let payload = match scope {
            FloodScope::Default => vec![0],
            FloodScope::Unscoped => vec![1],
            FloodScope::Key(key) => {
                let mut payload = Vec::with_capacity(17);
                payload.push(0);
                payload.extend_from_slice(key);
                payload
            }
        };
        Self {
            code: CommandCode::SetFloodScope,
            payload,
        }
    }

    /// Constructs a configured default flood-scope update using the caller's exact key bytes.
    ///
    /// The 31-byte name field reserves one byte for the NUL terminator, so at most 30 UTF-8 bytes
    /// are accepted.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidArgument`] for an empty, NUL-containing, or overlong name.
    pub fn set_default_flood_scope(name: &str, key: [u8; 16]) -> Result<Self, CoreError> {
        validate_text_field(name, 30, "name")?;
        if name.as_bytes().contains(&0) {
            return Err(invalid_argument("name", "must not contain a NUL byte"));
        }
        let mut payload = vec![0_u8; 31 + 16];
        payload[..name.len()].copy_from_slice(name.as_bytes());
        payload[31..].copy_from_slice(&key);
        Self::checked(CommandCode::SetDefaultFloodScope, payload)
    }

    /// Constructs a request to clear the configured default flood scope.
    #[must_use]
    pub fn clear_default_flood_scope() -> Self {
        Self::without_payload(CommandCode::SetDefaultFloodScope)
    }

    /// Constructs a default flood-scope query.
    #[must_use]
    pub fn get_default_flood_scope() -> Self {
        Self::without_payload(CommandCode::GetDefaultFloodScope)
    }

    /// Constructs a direct text send command.
    #[must_use]
    pub fn send_direct_text(
        destination_prefix: &[u8],
        timestamp: u32,
        attempt: u8,
        text: &str,
    ) -> Self {
        let mut payload = Vec::with_capacity(1 + 1 + 4 + destination_prefix.len() + text.len());
        payload.push(0x00);
        payload.push(attempt);
        payload.extend_from_slice(&timestamp.to_le_bytes());
        payload.extend_from_slice(destination_prefix);
        payload.extend_from_slice(text.as_bytes());
        Self {
            code: CommandCode::SendTxtMsg,
            payload,
        }
    }

    /// Constructs a direct command send command.
    #[must_use]
    pub fn send_direct_command(
        destination_prefix: &[u8],
        timestamp: u32,
        attempt: u8,
        command: &str,
    ) -> Self {
        let mut payload = Vec::with_capacity(1 + 1 + 4 + destination_prefix.len() + command.len());
        payload.push(0x01);
        payload.push(attempt);
        payload.extend_from_slice(&timestamp.to_le_bytes());
        payload.extend_from_slice(destination_prefix);
        payload.extend_from_slice(command.as_bytes());
        Self {
            code: CommandCode::SendTxtMsg,
            payload,
        }
    }

    /// Constructs a channel text send command.
    #[must_use]
    pub fn send_channel(chan: u8, txt_type: u8, timestamp: u32, text: &str) -> Self {
        let mut payload = Vec::with_capacity(1 + 1 + 4 + text.len());
        payload.push(txt_type);
        payload.push(chan);
        payload.extend_from_slice(&timestamp.to_le_bytes());
        payload.extend_from_slice(text.as_bytes());
        Self {
            code: CommandCode::SendChannelTxtMsg,
            payload,
        }
    }

    /// Constructs a channel query packet.
    #[must_use]
    pub fn get_channel(idx: u8) -> Self {
        Self {
            code: CommandCode::GetChannel,
            payload: vec![idx],
        }
    }

    /// Constructs a channel update with an exact 16-byte secret.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidArgument`] for an empty, NUL-containing, or overlong name.
    pub fn set_channel(idx: u8, name: &str, secret: [u8; 16]) -> Result<Self, CoreError> {
        validate_text_field(name, 31, "name")?;
        if name.as_bytes().contains(&0) {
            return Err(invalid_argument("name", "must not contain a NUL byte"));
        }
        let mut payload = vec![0_u8; 1 + 32 + 16];
        payload[0] = idx;
        payload[1..=name.len()].copy_from_slice(name.as_bytes());
        payload[33..].copy_from_slice(&secret);
        Self::checked(CommandCode::SetChannel, payload)
    }

    /// Constructs a channel removal by replacing the slot with an empty name and zero key.
    #[must_use]
    pub fn clear_channel(idx: u8) -> Self {
        let mut payload = vec![0_u8; 1 + 32 + 16];
        payload[0] = idx;
        Self {
            code: CommandCode::SetChannel,
            payload,
        }
    }

    /// Constructs a remote login request.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidArgument`] for a non-32-byte key, a NUL-containing password,
    /// or an encoded packet above the companion maximum.
    pub fn send_login(public_key: &[u8], password: &str) -> Result<Self, CoreError> {
        if password.as_bytes().contains(&0) {
            return Err(invalid_argument("password", "must not contain a NUL byte"));
        }
        Self::command_with_key_suffix(CommandCode::SendLogin, public_key, password.as_bytes())
    }

    /// Constructs a remote status request.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidArgument`] unless `public_key` is exactly 32 bytes.
    pub fn send_status_request(public_key: &[u8]) -> Result<Self, CoreError> {
        Self::command_with_key(CommandCode::SendStatusReq, public_key, Vec::new())
    }

    /// Constructs a remote connection-state query.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidArgument`] unless `public_key` is exactly 32 bytes.
    pub fn has_connection(public_key: &[u8]) -> Result<Self, CoreError> {
        Self::command_with_key(CommandCode::HasConnection, public_key, Vec::new())
    }

    /// Constructs a remote logout request.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidArgument`] unless `public_key` is exactly 32 bytes.
    pub fn logout(public_key: &[u8]) -> Result<Self, CoreError> {
        Self::command_with_key(CommandCode::Logout, public_key, Vec::new())
    }

    /// Constructs a local telemetry query.
    #[must_use]
    pub fn get_self_telemetry() -> Self {
        Self {
            code: CommandCode::SendTelemetryReq,
            payload: vec![0, 0, 0],
        }
    }

    /// Constructs a correlated remote binary request.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidArgument`] for an invalid key or oversized request.
    pub fn send_binary_request(
        public_key: &[u8],
        request_type: u8,
        data: &[u8],
    ) -> Result<Self, CoreError> {
        let mut suffix = Vec::with_capacity(1 + data.len());
        suffix.push(request_type);
        suffix.extend_from_slice(data);
        Self::command_with_key_suffix(CommandCode::BinaryReq, public_key, &suffix)
    }

    /// Constructs an anonymous metadata request with an explicit reply route.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidArgument`] for an invalid key, zero request type, or a reply
    /// route/path mismatch. Flood reply routes are rejected because firmware accepts these
    /// metadata requests only over a direct route.
    pub fn send_anonymous_request(
        public_key: &[u8],
        request_type: u8,
        reply_route: ContactRoute,
        reply_path: &Path,
    ) -> Result<Self, CoreError> {
        if request_type == 0 {
            return Err(invalid_argument("request_type", "must be non-zero"));
        }
        if reply_route == ContactRoute::Flood {
            return Err(invalid_argument(
                "reply_route",
                "anonymous requests require a direct reply route",
            ));
        }
        let descriptor = encode_contact_route(reply_route, reply_path)?;
        let mut suffix = Vec::with_capacity(2 + reply_path.as_bytes().len());
        suffix.push(request_type);
        suffix.push(descriptor);
        suffix.extend_from_slice(reply_path.as_bytes());
        Self::command_with_key_suffix(CommandCode::SendAnonReq, public_key, &suffix)
    }

    /// Constructs a path-discovery request for one exact contact key.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidArgument`] unless `public_key` is exactly 32 bytes.
    pub fn discover_path(public_key: &[u8]) -> Result<Self, CoreError> {
        Self::command_with_key(CommandCode::PathDiscovery, public_key, vec![0])
    }

    /// Constructs a correlated node-discovery control request.
    ///
    /// The optional `since` timestamp selects the extended request layout. `prefix_only` requests
    /// 8-byte public-key prefixes instead of full 32-byte keys in subsequent control-data pushes.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidArgument`] when `tag` is zero.
    pub fn send_node_discovery(
        filter: u8,
        prefix_only: bool,
        tag: u32,
        since: Option<u32>,
    ) -> Result<Self, CoreError> {
        if tag == 0 {
            return Err(invalid_argument("tag", "must be non-zero"));
        }
        let mut payload = Vec::with_capacity(if since.is_some() { 10 } else { 6 });
        payload.push(0x80 | u8::from(prefix_only));
        payload.push(filter);
        payload.extend_from_slice(&tag.to_le_bytes());
        if let Some(since) = since {
            payload.extend_from_slice(&since.to_le_bytes());
        }
        Self::checked(CommandCode::SendControlData, payload)
    }

    /// Constructs the first packet in a device signing session.
    #[must_use]
    pub fn sign_start() -> Self {
        Self::without_payload(CommandCode::SignStart)
    }

    /// Constructs one bounded signing-data chunk.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidArgument`] for an empty or oversized chunk.
    pub fn sign_data(chunk: &[u8]) -> Result<Self, CoreError> {
        if chunk.is_empty() {
            return Err(invalid_argument("chunk", "must not be empty"));
        }
        Self::checked(CommandCode::SignData, chunk.to_vec())
    }

    /// Constructs the final packet in a device signing session.
    #[must_use]
    pub fn sign_finish() -> Self {
        Self::without_payload(CommandCode::SignFinish)
    }

    /// Constructs a private-key export request.
    #[must_use]
    pub fn export_private_key() -> Self {
        Self::without_payload(CommandCode::ExportPrivateKey)
    }

    /// Constructs a private-key import request.
    #[must_use]
    pub fn import_private_key(key: &PrivateKeyMaterial) -> Self {
        Self {
            code: CommandCode::ImportPrivateKey,
            payload: key.expose_secret().to_vec(),
        }
    }

    /// Constructs a device-PIN update.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidArgument`] when `pin` is not a six-digit-or-shorter value.
    pub fn set_device_pin(pin: u32) -> Result<Self, CoreError> {
        if pin > 999_999 {
            return Err(invalid_argument("pin", "must be between 0 and 999999"));
        }
        Self::checked(CommandCode::SetDevicePin, pin.to_le_bytes().to_vec())
    }

    /// Constructs a reboot request using the firmware's exact safety token.
    #[must_use]
    pub fn reboot() -> Self {
        Self {
            code: CommandCode::Reboot,
            payload: b"reboot".to_vec(),
        }
    }

    /// Constructs a factory-reset request using the firmware's exact safety token.
    #[must_use]
    pub fn factory_reset() -> Self {
        Self {
            code: CommandCode::FactoryReset,
            payload: b"reset".to_vec(),
        }
    }

    fn without_payload(code: CommandCode) -> Self {
        Self {
            code,
            payload: Vec::new(),
        }
    }

    fn checked(code: CommandCode, payload: Vec<u8>) -> Result<Self, CoreError> {
        let total = payload.len().saturating_add(1);
        if total > MAX_INNER_PAYLOAD {
            return Err(invalid_argument(
                "payload",
                format!("encoded packet is {total} bytes; maximum is {MAX_INNER_PAYLOAD}"),
            ));
        }
        Ok(Self { code, payload })
    }

    fn command_with_key(
        code: CommandCode,
        public_key: &[u8],
        mut prefix: Vec<u8>,
    ) -> Result<Self, CoreError> {
        if public_key.len() != 32 {
            return Err(invalid_argument(
                "public_key",
                format!("expected exactly 32 bytes, got {}", public_key.len()),
            ));
        }
        prefix.extend_from_slice(public_key);
        Self::checked(code, prefix)
    }

    fn command_with_key_suffix(
        code: CommandCode,
        public_key: &[u8],
        suffix: &[u8],
    ) -> Result<Self, CoreError> {
        if public_key.len() != 32 {
            return Err(invalid_argument(
                "public_key",
                format!("expected exactly 32 bytes, got {}", public_key.len()),
            ));
        }
        let mut payload = Vec::with_capacity(public_key.len().saturating_add(suffix.len()));
        payload.extend_from_slice(public_key);
        payload.extend_from_slice(suffix);
        Self::checked(code, payload)
    }
}

fn invalid_argument(field: &'static str, message: impl Into<String>) -> CoreError {
    CoreError::InvalidArgument {
        field,
        message: message.into(),
    }
}

fn contact_type_to_u8(contact_type: ContactType) -> u8 {
    match contact_type {
        ContactType::Chat => 0,
        ContactType::Repeater => 1,
        ContactType::Room => 2,
        ContactType::Sensor => 3,
        ContactType::Unknown(value) => value,
    }
}

fn encode_contact_route(route: ContactRoute, path: &Path) -> Result<u8, CoreError> {
    match route {
        ContactRoute::Flood => {
            if !path.as_bytes().is_empty() {
                return Err(invalid_argument(
                    "path",
                    "a flood route must not contain path bytes",
                ));
            }
            Ok(u8::MAX)
        }
        ContactRoute::Path {
            hash_mode,
            hop_count,
        } => {
            if hash_mode > 2 || hop_count > 63 {
                return Err(invalid_argument(
                    "route",
                    "hash mode must be at most 2 and hop count at most 63",
                ));
            }
            let expected = usize::from(hop_count) * (usize::from(hash_mode) + 1);
            if path.as_bytes().len() != expected {
                return Err(invalid_argument(
                    "path",
                    format!(
                        "route descriptor requires {expected} bytes, got {}",
                        path.as_bytes().len()
                    ),
                ));
            }
            if expected > 64 {
                return Err(invalid_argument(
                    "path",
                    "contact paths are limited to 64 bytes",
                ));
            }
            Ok((hash_mode << 6) | hop_count)
        }
    }
}

fn validate_text_field(
    value: &str,
    maximum_bytes: usize,
    field: &'static str,
) -> Result<(), CoreError> {
    if value.is_empty() {
        return Err(invalid_argument(field, "must not be empty"));
    }
    if value.len() > maximum_bytes {
        return Err(invalid_argument(
            field,
            format!(
                "UTF-8 value is {} bytes; maximum is {maximum_bytes}",
                value.len()
            ),
        ));
    }
    Ok(())
}

fn validate_custom_component(value: &str, field: &'static str) -> Result<(), CoreError> {
    validate_text_field(value, MAX_INNER_PAYLOAD - 2, field)?;
    if value.bytes().any(|byte| matches!(byte, 0 | b':' | b',')) {
        return Err(invalid_argument(
            field,
            "must not contain NUL, ':' or ',' wire separators",
        ));
    }
    Ok(())
}

fn validate_range<T>(value: T, minimum: T, maximum: T, field: &'static str) -> Result<(), CoreError>
where
    T: Copy + fmt::Display + PartialOrd,
{
    if value < minimum || value > maximum {
        return Err(invalid_argument(
            field,
            format!("must be between {minimum} and {maximum}, got {value}"),
        ));
    }
    Ok(())
}

#[allow(clippy::cast_possible_truncation)]
fn scaled_coordinate(
    value: f64,
    minimum: f64,
    maximum: f64,
    field: &'static str,
) -> Result<i32, CoreError> {
    if !value.is_finite() || value < minimum || value > maximum {
        return Err(invalid_argument(
            field,
            format!("must be finite and between {minimum} and {maximum}"),
        ));
    }
    Ok((value * 1_000_000.0) as i32)
}

/// Companion response/notification tag.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum PacketCode {
    /// Command completed successfully.
    Ok = 0x00,
    /// Command failed with a reason code.
    Error = 0x01,
    /// Beginning of contact list stream.
    ContactStart = 0x02,
    /// Contact entry packet.
    Contact = 0x03,
    /// End of contact list stream.
    ContactEnd = 0x04,
    /// Self info packet.
    SelfInfo = 0x05,
    /// Message sent confirmation.
    MsgSent = 0x06,
    /// Legacy contact message.
    ContactMsgRecv = 0x07,
    /// Legacy channel message.
    ChannelMsgRecv = 0x08,
    /// Current time packet.
    CurrentTime = 0x09,
    /// No further pending messages.
    NoMoreMsgs = 0x0a,
    /// Contact URI response.
    ContactUri = 0x0b,
    /// Battery and storage packet.
    Battery = 0x0c,
    /// Device information packet.
    DeviceInfo = 0x0d,
    /// Private key packet.
    PrivateKey = 0x0e,
    /// Disabled or placeholder packet.
    Disabled = 0x0f,
    /// V3 contact message.
    ContactMsgRecvV3 = 0x10,
    /// V3 channel message.
    ChannelMsgRecvV3 = 0x11,
    /// Channel information packet.
    ChannelInfo = 0x12,
    /// Signature session start packet.
    SignStart = 0x13,
    /// Signature blob packet.
    Signature = 0x14,
    /// Custom variables packet.
    CustomVars = 0x15,
    /// Advert path packet.
    AdvertPath = 0x16,
    /// Tuning parameters packet.
    TuningParams = 0x17,
    /// Statistics packet.
    Stats = 0x18,
    /// Auto-add config packet.
    AutoaddConfig = 0x19,
    /// Allowed repeat frequency packet.
    AllowedRepeatFreq = 0x1a,
    /// Channel data packet.
    ChannelDataRecv = 0x1b,
    /// Default flood scope packet.
    DefaultFloodScope = 0x1c,
    /// Advertisement packet.
    Advertisement = 0x80,
    /// Path update packet.
    PathUpdate = 0x81,
    /// Acknowledgement packet.
    Ack = 0x82,
    /// Message queue not empty packet.
    MessagesWaiting = 0x83,
    /// Raw data packet.
    RawData = 0x84,
    /// Login success packet.
    LoginSuccess = 0x85,
    /// Login failed packet.
    LoginFailed = 0x86,
    /// Status response packet.
    StatusResponse = 0x87,
    /// Logged event packet.
    LogData = 0x88,
    /// Trace path packet.
    TraceData = 0x89,
    /// Advertisement packet.
    NewAdvert = 0x8a,
    /// Telemetry response packet.
    TelemetryResponse = 0x8b,
    /// Binary response packet.
    BinaryResponse = 0x8c,
    /// Path discovery response packet.
    PathDiscoveryResponse = 0x8d,
    /// Control-data packet.
    ControlData = 0x8e,
    /// Contact removed packet.
    ContactDeleted = 0x8f,
    /// Contact store full packet.
    ContactsFull = 0x90,
    /// Unknown packet type.
    Unknown(u8),
}

impl PacketCode {
    /// Converts to byte code.
    #[must_use]
    pub const fn to_u8(self) -> u8 {
        match self {
            Self::Ok => 0x00,
            Self::Error => 0x01,
            Self::ContactStart => 0x02,
            Self::Contact => 0x03,
            Self::ContactEnd => 0x04,
            Self::SelfInfo => 0x05,
            Self::MsgSent => 0x06,
            Self::ContactMsgRecv => 0x07,
            Self::ChannelMsgRecv => 0x08,
            Self::CurrentTime => 0x09,
            Self::NoMoreMsgs => 0x0a,
            Self::ContactUri => 0x0b,
            Self::Battery => 0x0c,
            Self::DeviceInfo => 0x0d,
            Self::PrivateKey => 0x0e,
            Self::Disabled => 0x0f,
            Self::ContactMsgRecvV3 => 0x10,
            Self::ChannelMsgRecvV3 => 0x11,
            Self::ChannelInfo => 0x12,
            Self::SignStart => 0x13,
            Self::Signature => 0x14,
            Self::CustomVars => 0x15,
            Self::AdvertPath => 0x16,
            Self::TuningParams => 0x17,
            Self::Stats => 0x18,
            Self::AutoaddConfig => 0x19,
            Self::AllowedRepeatFreq => 0x1a,
            Self::ChannelDataRecv => 0x1b,
            Self::DefaultFloodScope => 0x1c,
            Self::Advertisement => 0x80,
            Self::PathUpdate => 0x81,
            Self::Ack => 0x82,
            Self::MessagesWaiting => 0x83,
            Self::RawData => 0x84,
            Self::LoginSuccess => 0x85,
            Self::LoginFailed => 0x86,
            Self::StatusResponse => 0x87,
            Self::LogData => 0x88,
            Self::TraceData => 0x89,
            Self::NewAdvert => 0x8a,
            Self::TelemetryResponse => 0x8b,
            Self::BinaryResponse => 0x8c,
            Self::PathDiscoveryResponse => 0x8d,
            Self::ControlData => 0x8e,
            Self::ContactDeleted => 0x8f,
            Self::ContactsFull => 0x90,
            Self::Unknown(code) => code,
        }
    }
}

impl PacketCode {
    /// Converts a raw code into packet code enum.
    #[must_use]
    pub const fn from_u8(code: u8) -> Self {
        match code {
            0x00 => Self::Ok,
            0x01 => Self::Error,
            0x02 => Self::ContactStart,
            0x03 => Self::Contact,
            0x04 => Self::ContactEnd,
            0x05 => Self::SelfInfo,
            0x06 => Self::MsgSent,
            0x07 => Self::ContactMsgRecv,
            0x08 => Self::ChannelMsgRecv,
            0x09 => Self::CurrentTime,
            0x0a => Self::NoMoreMsgs,
            0x0b => Self::ContactUri,
            0x0c => Self::Battery,
            0x0d => Self::DeviceInfo,
            0x0e => Self::PrivateKey,
            0x0f => Self::Disabled,
            0x10 => Self::ContactMsgRecvV3,
            0x11 => Self::ChannelMsgRecvV3,
            0x12 => Self::ChannelInfo,
            0x13 => Self::SignStart,
            0x14 => Self::Signature,
            0x15 => Self::CustomVars,
            0x16 => Self::AdvertPath,
            0x17 => Self::TuningParams,
            0x18 => Self::Stats,
            0x19 => Self::AutoaddConfig,
            0x1a => Self::AllowedRepeatFreq,
            0x1b => Self::ChannelDataRecv,
            0x1c => Self::DefaultFloodScope,
            0x80 => Self::Advertisement,
            0x81 => Self::PathUpdate,
            0x82 => Self::Ack,
            0x83 => Self::MessagesWaiting,
            0x84 => Self::RawData,
            0x85 => Self::LoginSuccess,
            0x86 => Self::LoginFailed,
            0x87 => Self::StatusResponse,
            0x88 => Self::LogData,
            0x89 => Self::TraceData,
            0x8a => Self::NewAdvert,
            0x8b => Self::TelemetryResponse,
            0x8c => Self::BinaryResponse,
            0x8d => Self::PathDiscoveryResponse,
            0x8e => Self::ControlData,
            0x8f => Self::ContactDeleted,
            0x90 => Self::ContactsFull,
            other => Self::Unknown(other),
        }
    }
}

/// Parsed protocol packet from one companion frame.
#[derive(Clone, Serialize, Deserialize)]
pub enum Packet {
    /// Operation completed status.
    Ok(Option<u32>),
    /// Error status.
    Error(Option<u8>),
    /// Contact stream has started.
    ContactStart {
        /// Number of contacts in stream.
        count: u32,
    },
    /// Single contact packet.
    Contact(Contact),
    /// Contact stream has ended.
    ContactEnd {
        /// Last-modified marker for contacts.
        lastmod: u32,
    },
    /// Self info payload.
    SelfInfo(SelfInfo),
    /// Message sent acknowledgement.
    MsgSent {
        /// Destination kind for send attempt.
        destination_type: u8,
        /// Expected ack token.
        expected_ack: [u8; 4],
        /// Suggested retry timeout.
        suggested_timeout_ms: u32,
    },
    /// Received contact message.
    ContactMsg(Message),
    /// Received channel message.
    ChannelMsg(Message),
    /// Current device time.
    CurrentTime(u32),
    /// No more messages available.
    NoMoreMsgs,
    /// Contact URI payload.
    ContactUri(ContactUri),
    /// Battery and storage telemetry.
    Battery(BatteryInfo),
    /// Device info payload.
    DeviceInfo(DeviceInfo),
    /// Privileged device private-key response.
    ///
    /// This variant is never serialized or converted into an event. A privileged export operation
    /// must consume it explicitly and write it to a secure destination.
    #[serde(skip)]
    PrivateKey(PrivateKeyMaterial),
    /// Firmware feature is deliberately disabled in this build.
    Disabled,
    /// Device signing session capacity response.
    SignStart {
        /// Maximum bytes accepted by the signing session.
        max_data_bytes: u32,
    },
    /// Device-generated signature.
    Signature(Signature),
    /// Channel metadata.
    ChannelInfo(ChannelInfo),
    /// Tuning parameters.
    TuningParams(TuningParams),
    /// Strictly decoded custom variables with the raw payload retained.
    CustomVariables(CustomVariables),
    /// Last observed advert path for a contact.
    AdvertPath(AdvertPath),
    /// Typed device statistics.
    DeviceStats(DeviceStats),
    /// Auto-add configuration.
    AutoAddConfig(AutoAddConfig),
    /// Allowed repeat-frequency ranges.
    AllowedRepeatFrequencies(Vec<FrequencyRange>),
    /// Configured default flood scope, or its explicit unset sentinel.
    DefaultFloodScope(DefaultFloodScope),
    /// Generic ack packet.
    Ack(Ack),
    /// Message queue state marker.
    MessagesWaiting,
    /// Successful remote login response.
    LoginSuccess(LoginSession),
    /// Failed remote login response.
    LoginFailed {
        /// Six-byte remote public-key prefix.
        pubkey_prefix: [u8; 6],
    },
    /// Remote status response.
    RemoteStatus(RemoteStatus),
    /// Local or remote telemetry response.
    TelemetryResponse(TelemetryResponse),
    /// Correlated binary response.
    BinaryResponse(BinaryResponse),
    /// Received control-plane payload and radio metadata.
    ControlData(ControlData),
    /// Path discovery response.
    PathDiscovery(PathDiscovery),
    /// Unknown packet payload.
    Unknown {
        /// Unrecognized packet code.
        code: u8,
        /// Raw packet payload bytes.
        payload: Vec<u8>,
    },
}

impl fmt::Debug for Packet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ok(value) => f.debug_tuple("Ok").field(value).finish(),
            Self::Error(code) => f.debug_tuple("Error").field(code).finish(),
            Self::ContactStart { count } => f
                .debug_struct("ContactStart")
                .field("count", count)
                .finish(),
            Self::Contact(_) => write!(f, "Contact(<redacted>)"),
            Self::ContactEnd { lastmod } => f
                .debug_struct("ContactEnd")
                .field("lastmod", lastmod)
                .finish(),
            Self::SelfInfo(_) => write!(f, "SelfInfo(<redacted>)"),
            Self::MsgSent {
                destination_type,
                suggested_timeout_ms,
                ..
            } => write!(
                f,
                "MsgSent {{ destination_type: {destination_type}, expected_ack: <redacted>, suggested_timeout_ms: {suggested_timeout_ms} }}"
            ),
            Self::ContactMsg(message) => write!(f, "ContactMsg({message:?})"),
            Self::ChannelMsg(message) => write!(f, "ChannelMsg({message:?})"),
            Self::CurrentTime(timestamp) => f.debug_tuple("CurrentTime").field(timestamp).finish(),
            Self::NoMoreMsgs => write!(f, "NoMoreMsgs"),
            Self::ContactUri(uri) => write!(f, "ContactUri({uri:?})"),
            Self::Battery(info) => f.debug_tuple("Battery").field(info).finish(),
            Self::DeviceInfo(info) => write!(f, "DeviceInfo({info:?})"),
            Self::PrivateKey(_) => write!(f, "PrivateKey(<redacted>)"),
            Self::Disabled => write!(f, "Disabled"),
            Self::SignStart { max_data_bytes } => f
                .debug_struct("SignStart")
                .field("max_data_bytes", max_data_bytes)
                .finish(),
            Self::Signature(signature) => write!(f, "Signature({signature:?})"),
            Self::ChannelInfo(info) => write!(f, "ChannelInfo({info:?})"),
            Self::TuningParams(params) => f.debug_tuple("TuningParams").field(params).finish(),
            Self::CustomVariables(vars) => write!(f, "CustomVariables({vars:?})"),
            Self::AdvertPath(path) => write!(f, "AdvertPath({path:?})"),
            Self::DeviceStats(stats) => f.debug_tuple("DeviceStats").field(stats).finish(),
            Self::AutoAddConfig(config) => f.debug_tuple("AutoAddConfig").field(config).finish(),
            Self::AllowedRepeatFrequencies(ranges) => {
                write!(f, "AllowedRepeatFrequencies(count={})", ranges.len())
            }
            Self::DefaultFloodScope(scope) => write!(f, "DefaultFloodScope({scope:?})"),
            Self::Ack(ack) => write!(f, "Ack({ack:?})"),
            Self::MessagesWaiting => write!(f, "MessagesWaiting"),
            Self::LoginSuccess(session) => write!(f, "LoginSuccess({session:?})"),
            Self::LoginFailed { .. } => {
                write!(f, "LoginFailed {{ pubkey_prefix: <redacted> }}")
            }
            Self::RemoteStatus(status) => write!(f, "RemoteStatus({status:?})"),
            Self::TelemetryResponse(response) => write!(f, "TelemetryResponse({response:?})"),
            Self::BinaryResponse(response) => write!(f, "BinaryResponse({response:?})"),
            Self::ControlData(data) => write!(f, "ControlData({data:?})"),
            Self::PathDiscovery(path) => write!(f, "PathDiscovery({path:?})"),
            Self::Unknown { code, payload } => write!(
                f,
                "Unknown {{ code: {code:#04x}, payload_len: {} }}",
                payload.len()
            ),
        }
    }
}

impl Packet {
    /// Parses a complete inner companion frame.
    ///
    /// # Errors
    ///
    /// Returns `ParseError` when packet framing or payload decoding is invalid.
    pub fn parse(raw: &[u8]) -> Result<Self, ParseError> {
        if raw.is_empty() {
            return Err(ParseError::Malformed {
                reason: "empty packet",
            });
        }
        if raw.len() > MAX_INNER_PAYLOAD {
            return Err(ParseError::OversizedPacketPayload {
                actual: raw.len(),
                maximum: MAX_INNER_PAYLOAD,
            });
        }

        match PacketCode::from_u8(raw[0]) {
            PacketCode::Ok => parse_ok(raw),
            PacketCode::Error => Ok(parse_error(raw)),
            PacketCode::ContactStart => parse_contact_start(raw),
            PacketCode::Contact => parse_contact(raw),
            PacketCode::ContactEnd => parse_contact_end(raw),
            PacketCode::SelfInfo => parse_self_info(raw),
            PacketCode::MsgSent => parse_msg_sent(raw),
            PacketCode::ContactMsgRecv => parse_contact_msg(raw, false),
            PacketCode::ContactMsgRecvV3 => parse_contact_msg(raw, true),
            PacketCode::ChannelMsgRecv => parse_channel_msg(raw, false),
            PacketCode::ChannelMsgRecvV3 => parse_channel_msg(raw, true),
            PacketCode::CurrentTime => parse_current_time(raw),
            PacketCode::NoMoreMsgs => Ok(Packet::NoMoreMsgs),
            PacketCode::ContactUri => parse_contact_uri(raw),
            PacketCode::Battery => parse_battery(raw),
            PacketCode::DeviceInfo => parse_device_info(raw),
            PacketCode::PrivateKey => parse_private_key(raw),
            PacketCode::Disabled => parse_disabled(raw),
            PacketCode::ChannelInfo => parse_channel_info(raw),
            PacketCode::SignStart => parse_sign_start(raw),
            PacketCode::Signature => parse_signature(raw),
            PacketCode::CustomVars => parse_custom_vars(raw),
            PacketCode::AdvertPath => parse_advert_path(raw),
            PacketCode::TuningParams => parse_tuning_params(raw),
            PacketCode::Stats => parse_stats(raw),
            PacketCode::AutoaddConfig => parse_auto_add_config(raw),
            PacketCode::AllowedRepeatFreq => parse_allowed_repeat_frequencies(raw),
            PacketCode::DefaultFloodScope => parse_default_flood_scope(raw),
            PacketCode::Ack => parse_ack(raw),
            PacketCode::MessagesWaiting => Ok(Packet::MessagesWaiting),
            PacketCode::LoginSuccess => parse_login_success(raw),
            PacketCode::LoginFailed => parse_login_failed(raw),
            PacketCode::StatusResponse => parse_remote_status(raw),
            PacketCode::TelemetryResponse => parse_telemetry_response(raw),
            PacketCode::BinaryResponse => parse_binary_response(raw),
            PacketCode::PathDiscoveryResponse => parse_path_discovery(raw),
            PacketCode::ControlData => parse_control_data(raw),
            PacketCode::Advertisement
            | PacketCode::PathUpdate
            | PacketCode::RawData
            | PacketCode::LogData
            | PacketCode::TraceData
            | PacketCode::NewAdvert
            | PacketCode::ContactDeleted
            | PacketCode::ContactsFull
            | PacketCode::ChannelDataRecv => Ok(Packet::Unknown {
                code: raw[0],
                payload: raw[1..].to_vec(),
            }),
            PacketCode::Unknown(code) => Ok(Packet::Unknown {
                code,
                payload: raw[1..].to_vec(),
            }),
        }
    }

    /// Maps packets to events for watchers.
    #[must_use]
    pub fn into_event(self) -> Option<Event> {
        match self {
            Self::Ok(_)
            | Self::Error(_)
            | Self::ContactStart { .. }
            | Self::PrivateKey(_)
            | Self::Disabled
            | Self::SignStart { .. } => None,
            Self::Contact(contact) => Some(Event::Contacts {
                contacts: vec![contact],
                lastmod: 0,
            }),
            Self::ContactEnd { lastmod } => Some(Event::Contacts {
                contacts: Vec::new(),
                lastmod,
            }),
            Self::SelfInfo(info) => Some(Event::SelfInfo(info)),
            Self::MsgSent {
                destination_type,
                expected_ack,
                suggested_timeout_ms,
            } => Some(Event::MessageSent {
                destination_type,
                ack_code: expected_ack,
                suggested_timeout_ms,
            }),
            Self::ContactMsg(message) | Self::ChannelMsg(message) => Some(Event::Message(message)),
            Self::CurrentTime(timestamp) => Some(Event::CurrentTime(timestamp)),
            Self::ContactUri(uri) => Some(Event::ContactUri(uri)),
            Self::Battery(info) => Some(Event::Battery {
                level: info.level,
                used_kb: info.used_kb,
                total_kb: info.total_kb,
            }),
            Self::DeviceInfo(info) => Some(Event::DeviceInfo(info)),
            Self::ChannelInfo(info) => Some(Event::ChannelInfo {
                idx: info.idx,
                name: info.name,
                secret_hash: info.secret_hash,
            }),
            Self::TuningParams(params) => Some(Event::TuningParams(params)),
            Self::CustomVariables(vars) => Some(Event::CustomVariables(vars)),
            Self::AdvertPath(path) => Some(Event::AdvertPath(path)),
            Self::DeviceStats(stats) => Some(Event::DeviceStats(stats)),
            Self::AutoAddConfig(config) => Some(Event::AutoAddConfig(config)),
            Self::AllowedRepeatFrequencies(ranges) => Some(Event::AllowedRepeatFrequencies(ranges)),
            Self::DefaultFloodScope(scope) => Some(Event::DefaultFloodScope(scope)),
            Self::LoginSuccess(session) => Some(Event::LoginSucceeded(session)),
            Self::LoginFailed { pubkey_prefix } => Some(Event::LoginFailed { pubkey_prefix }),
            Self::RemoteStatus(status) => Some(Event::RemoteStatus(status)),
            Self::TelemetryResponse(response) => Some(Event::Telemetry(response)),
            Self::BinaryResponse(response) => Some(Event::BinaryResponse(response)),
            Self::ControlData(data) => Some(Event::ControlData(data)),
            Self::PathDiscovery(path) => Some(Event::PathDiscovery(path)),
            Self::Signature(signature) => Some(Event::Signature(signature)),
            Self::Ack(ack) => Some(Event::Ack(ack)),
            Self::NoMoreMsgs => Some(Event::InboxEmpty),
            Self::MessagesWaiting => Some(Event::MessagesWaiting),
            Self::Unknown { code, payload } => Some(Event::UnknownPacket { code, payload }),
        }
    }

    /// Returns the variant code.
    #[must_use]
    pub const fn code(&self) -> PacketCode {
        match self {
            Self::Ok(_) => PacketCode::Ok,
            Self::Error(_) => PacketCode::Error,
            Self::ContactStart { .. } => PacketCode::ContactStart,
            Self::Contact(_) => PacketCode::Contact,
            Self::ContactEnd { .. } => PacketCode::ContactEnd,
            Self::SelfInfo(_) => PacketCode::SelfInfo,
            Self::MsgSent { .. } => PacketCode::MsgSent,
            Self::ContactMsg(_) => PacketCode::ContactMsgRecv,
            Self::ChannelMsg(_) => PacketCode::ChannelMsgRecv,
            Self::CurrentTime(_) => PacketCode::CurrentTime,
            Self::NoMoreMsgs => PacketCode::NoMoreMsgs,
            Self::ContactUri(_) => PacketCode::ContactUri,
            Self::Battery(_) => PacketCode::Battery,
            Self::DeviceInfo(_) => PacketCode::DeviceInfo,
            Self::PrivateKey(_) => PacketCode::PrivateKey,
            Self::Disabled => PacketCode::Disabled,
            Self::SignStart { .. } => PacketCode::SignStart,
            Self::Signature(_) => PacketCode::Signature,
            Self::ChannelInfo(_) => PacketCode::ChannelInfo,
            Self::TuningParams(_) => PacketCode::TuningParams,
            Self::CustomVariables(_) => PacketCode::CustomVars,
            Self::AdvertPath(_) => PacketCode::AdvertPath,
            Self::DeviceStats(_) => PacketCode::Stats,
            Self::AutoAddConfig(_) => PacketCode::AutoaddConfig,
            Self::AllowedRepeatFrequencies(_) => PacketCode::AllowedRepeatFreq,
            Self::DefaultFloodScope(_) => PacketCode::DefaultFloodScope,
            Self::Ack(_) => PacketCode::Ack,
            Self::MessagesWaiting => PacketCode::MessagesWaiting,
            Self::LoginSuccess(_) => PacketCode::LoginSuccess,
            Self::LoginFailed { .. } => PacketCode::LoginFailed,
            Self::RemoteStatus(_) => PacketCode::StatusResponse,
            Self::TelemetryResponse(_) => PacketCode::TelemetryResponse,
            Self::BinaryResponse(_) => PacketCode::BinaryResponse,
            Self::ControlData(_) => PacketCode::ControlData,
            Self::PathDiscovery(_) => PacketCode::PathDiscoveryResponse,
            Self::Unknown { code, .. } => PacketCode::from_u8(*code),
        }
    }
}

fn parse_ok(raw: &[u8]) -> Result<Packet, ParseError> {
    match raw {
        [_] => Ok(Packet::Ok(None)),
        [_, one, two, three, four] => Ok(Packet::Ok(Some(u32::from_le_bytes([
            *one, *two, *three, *four,
        ])))),
        _ => Err(ParseError::Malformed {
            reason: "OK packet must contain either no value or one complete u32 value",
        }),
    }
}

fn parse_error(raw: &[u8]) -> Packet {
    Packet::Error(raw.get(1).copied())
}

fn parse_private_key(raw: &[u8]) -> Result<Packet, ParseError> {
    require_exact_len(raw, 65, PacketCode::PrivateKey)?;
    let private_key =
        PrivateKeyMaterial::try_from_bytes(&raw[1..]).map_err(|_| ParseError::Malformed {
            reason: "invalid private-key payload",
        })?;
    Ok(Packet::PrivateKey(private_key))
}

fn parse_disabled(raw: &[u8]) -> Result<Packet, ParseError> {
    require_exact_len(raw, 1, PacketCode::Disabled)?;
    Ok(Packet::Disabled)
}

fn parse_sign_start(raw: &[u8]) -> Result<Packet, ParseError> {
    require_exact_len(raw, 6, PacketCode::SignStart)?;
    if raw[1] != 0 {
        return Err(ParseError::Malformed {
            reason: "sign-start reserved byte must be zero",
        });
    }
    Ok(Packet::SignStart {
        max_data_bytes: read_u32(raw, 2),
    })
}

fn parse_signature(raw: &[u8]) -> Result<Packet, ParseError> {
    require_exact_len(raw, 65, PacketCode::Signature)?;
    let signature = Signature::try_from_bytes(&raw[1..]).map_err(|_| ParseError::Malformed {
        reason: "invalid signature payload",
    })?;
    Ok(Packet::Signature(signature))
}

fn parse_login_success(raw: &[u8]) -> Result<Packet, ParseError> {
    if !matches!(raw.len(), 8 | 12 | 13 | 14) {
        require_len(raw, 8, PacketCode::LoginSuccess)?;
        return Err(ParseError::Malformed {
            reason: "login-success packet has a partial or unexpected extension",
        });
    }
    let mut pubkey_prefix = [0_u8; 6];
    pubkey_prefix.copy_from_slice(&raw[2..8]);
    Ok(Packet::LoginSuccess(LoginSession {
        permissions: raw[1],
        pubkey_prefix,
        server_timestamp: (raw.len() >= 12).then(|| read_u32(raw, 8)),
        acl_permissions: raw.get(12).copied(),
        firmware_version_level: raw.get(13).copied(),
    }))
}

fn parse_login_failed(raw: &[u8]) -> Result<Packet, ParseError> {
    require_exact_len(raw, 8, PacketCode::LoginFailed)?;
    if raw[1] != 0 {
        return Err(ParseError::Malformed {
            reason: "login-failed reserved byte must be zero",
        });
    }
    let mut pubkey_prefix = [0_u8; 6];
    pubkey_prefix.copy_from_slice(&raw[2..8]);
    Ok(Packet::LoginFailed { pubkey_prefix })
}

fn parse_remote_status(raw: &[u8]) -> Result<Packet, ParseError> {
    if !matches!(raw.len(), 60 | 64) {
        require_len(raw, 60, PacketCode::StatusResponse)?;
        return Err(ParseError::Malformed {
            reason: "status response must contain the legacy or current complete layout",
        });
    }
    if raw[1] != 0 {
        return Err(ParseError::Malformed {
            reason: "status-response reserved byte must be zero",
        });
    }
    let mut pubkey_prefix = [0_u8; 6];
    pubkey_prefix.copy_from_slice(&raw[2..8]);
    let offset = 8;
    Ok(Packet::RemoteStatus(RemoteStatus {
        pubkey_prefix,
        battery_mv: read_u16(raw, offset),
        tx_queue_length: read_u16(raw, offset + 2),
        noise_floor: read_i16(raw, offset + 4),
        last_rssi: read_i16(raw, offset + 6),
        packets_received: read_u32(raw, offset + 8),
        packets_sent: read_u32(raw, offset + 12),
        tx_airtime_seconds: read_u32(raw, offset + 16),
        uptime_seconds: read_u32(raw, offset + 20),
        sent_flood: read_u32(raw, offset + 24),
        sent_direct: read_u32(raw, offset + 28),
        received_flood: read_u32(raw, offset + 32),
        received_direct: read_u32(raw, offset + 36),
        error_events: read_u16(raw, offset + 40),
        last_snr: f32::from(read_i16(raw, offset + 42)) / 4.0,
        direct_duplicates: read_u16(raw, offset + 44),
        flood_duplicates: read_u16(raw, offset + 46),
        rx_airtime_seconds: read_u32(raw, offset + 48),
        receive_errors: (raw.len() == 64).then(|| read_u32(raw, offset + 52)),
    }))
}

fn parse_telemetry_response(raw: &[u8]) -> Result<Packet, ParseError> {
    require_len(raw, 8, PacketCode::TelemetryResponse)?;
    if raw[1] != 0 {
        return Err(ParseError::Malformed {
            reason: "telemetry-response reserved byte must be zero",
        });
    }
    let mut pubkey_prefix = [0_u8; 6];
    pubkey_prefix.copy_from_slice(&raw[2..8]);
    Ok(Packet::TelemetryResponse(TelemetryResponse {
        pubkey_prefix,
        payload: raw[8..].to_vec(),
    }))
}

fn parse_binary_response(raw: &[u8]) -> Result<Packet, ParseError> {
    require_len(raw, 6, PacketCode::BinaryResponse)?;
    if raw[1] != 0 {
        return Err(ParseError::Malformed {
            reason: "binary-response reserved byte must be zero",
        });
    }
    let mut tag = [0_u8; 4];
    tag.copy_from_slice(&raw[2..6]);
    Ok(Packet::BinaryResponse(BinaryResponse {
        tag,
        payload: raw[6..].to_vec(),
    }))
}

fn parse_control_data(raw: &[u8]) -> Result<Packet, ParseError> {
    require_len(raw, 5, PacketCode::ControlData)?;
    Ok(Packet::ControlData(ControlData {
        snr_qdb: i8::from_le_bytes([raw[1]]),
        rssi: i8::from_le_bytes([raw[2]]),
        path_len: raw[3],
        payload: raw[4..].to_vec(),
    }))
}

fn parse_path_discovery(raw: &[u8]) -> Result<Packet, ParseError> {
    require_len(raw, 10, PacketCode::PathDiscoveryResponse)?;
    if raw[1] != 0 {
        return Err(ParseError::Malformed {
            reason: "path-discovery reserved byte must be zero",
        });
    }
    let mut pubkey_prefix = [0_u8; 6];
    pubkey_prefix.copy_from_slice(&raw[2..8]);
    let mut cursor = 8;
    let (outbound_route, outbound_path) = parse_discovered_path(raw, &mut cursor)?;
    let (inbound_route, inbound_path) = parse_discovered_path(raw, &mut cursor)?;
    if cursor != raw.len() {
        return Err(ParseError::Malformed {
            reason: "path-discovery response contains trailing bytes",
        });
    }
    Ok(Packet::PathDiscovery(PathDiscovery {
        pubkey_prefix,
        outbound_route,
        outbound_path,
        inbound_route,
        inbound_path,
    }))
}

fn parse_discovered_path(
    raw: &[u8],
    cursor: &mut usize,
) -> Result<(ContactRoute, Path), ParseError> {
    let descriptor = *raw.get(*cursor).ok_or(ParseError::Malformed {
        reason: "path-discovery response is missing a route descriptor",
    })?;
    *cursor = (*cursor).saturating_add(1);
    let (route, byte_len) = if descriptor == u8::MAX {
        (ContactRoute::Flood, 0)
    } else {
        let hash_mode = descriptor >> 6;
        let hop_count = descriptor & 0x3f;
        (
            ContactRoute::Path {
                hash_mode,
                hop_count,
            },
            usize::from(hop_count)
                .checked_mul(usize::from(hash_mode) + 1)
                .ok_or(ParseError::Malformed {
                    reason: "path-discovery path length overflowed",
                })?,
        )
    };
    let end = (*cursor)
        .checked_add(byte_len)
        .ok_or(ParseError::Malformed {
            reason: "path-discovery cursor overflowed",
        })?;
    let bytes = raw.get(*cursor..end).ok_or(ParseError::Malformed {
        reason: "path-discovery response contains a truncated path",
    })?;
    let path = Path::try_from_bytes(bytes).map_err(|_| ParseError::Malformed {
        reason: "path-discovery response exceeds supported path bytes",
    })?;
    *cursor = end;
    Ok((route, path))
}

fn require_len(raw: &[u8], minimum: usize, code: PacketCode) -> Result<(), ParseError> {
    if raw.len() < minimum {
        Err(ParseError::InvalidPacketLength {
            code: PacketDisplay::Raw(code.to_u8()),
            minimum,
            actual: raw.len(),
        })
    } else {
        Ok(())
    }
}

fn require_exact_len(raw: &[u8], expected: usize, code: PacketCode) -> Result<(), ParseError> {
    require_len(raw, expected, code)?;
    if raw.len() != expected {
        return Err(ParseError::Malformed {
            reason: "packet contains unexpected trailing bytes",
        });
    }
    Ok(())
}

fn parse_contact_start(raw: &[u8]) -> Result<Packet, ParseError> {
    require_len(raw, 5, PacketCode::ContactStart)?;
    Ok(Packet::ContactStart {
        count: u32::from_le_bytes([raw[1], raw[2], raw[3], raw[4]]),
    })
}

fn parse_contact(raw: &[u8]) -> Result<Packet, ParseError> {
    const CONTACT_PACKET_LEN: usize = 148;
    const FIXED_PATH_LEN: usize = 64;
    require_len(raw, CONTACT_PACKET_LEN, PacketCode::Contact)?;

    let public_key = PublicKey::try_from_bytes(&raw[1..33]).map_err(|_| ParseError::Malformed {
        reason: "invalid contact public key",
    })?;
    let contact_type = ContactType::from_u8(raw[33]);
    let flags = raw[34];
    let route_byte = raw[35];
    let (route, used_path_bytes) = if route_byte == u8::MAX {
        (ContactRoute::Flood, 0)
    } else {
        let hash_mode = route_byte >> 6;
        let hop_count = route_byte & 0x3f;
        let used = usize::from(hop_count) * (usize::from(hash_mode) + 1);
        if used > FIXED_PATH_LEN {
            return Err(ParseError::Malformed {
                reason: "contact path descriptor exceeds fixed path field",
            });
        }
        (
            ContactRoute::Path {
                hash_mode,
                hop_count,
            },
            used,
        )
    };

    let out_path = Path::try_from_bytes(&raw[36..36 + used_path_bytes]).map_err(|_| {
        ParseError::Malformed {
            reason: "invalid contact path",
        }
    })?;
    let mut cursor = 36 + FIXED_PATH_LEN;
    let adv_name = decode_padded_string(&raw[cursor..cursor + 32], "Contact.adv_name")?;
    cursor += 32;
    let last_advert = u32::from_le_bytes([
        raw[cursor],
        raw[cursor + 1],
        raw[cursor + 2],
        raw[cursor + 3],
    ]);
    cursor += 4;
    let adv_lat = parse_scaled_i32([
        raw[cursor],
        raw[cursor + 1],
        raw[cursor + 2],
        raw[cursor + 3],
    ]);
    let adv_lon = parse_scaled_i32([
        raw[cursor + 4],
        raw[cursor + 5],
        raw[cursor + 6],
        raw[cursor + 7],
    ]);
    let lastmod = u32::from_le_bytes([
        raw[cursor + 8],
        raw[cursor + 9],
        raw[cursor + 10],
        raw[cursor + 11],
    ]);

    Ok(Packet::Contact(Contact {
        public_key,
        contact_type,
        flags,
        route,
        out_path,
        adv_name,
        last_advert,
        adv_lat,
        adv_lon,
        lastmod,
    }))
}

fn parse_contact_end(raw: &[u8]) -> Result<Packet, ParseError> {
    require_len(raw, 5, PacketCode::ContactEnd)?;
    Ok(Packet::ContactEnd {
        lastmod: u32::from_le_bytes([raw[1], raw[2], raw[3], raw[4]]),
    })
}

fn parse_self_info(raw: &[u8]) -> Result<Packet, ParseError> {
    require_len(raw, 58, PacketCode::SelfInfo)?;
    let public_key = PublicKey::try_from_bytes(&raw[4..36]).map_err(|_| ParseError::Malformed {
        reason: "invalid self-info public key",
    })?;
    let modes = raw[46];
    Ok(Packet::SelfInfo(SelfInfo {
        advertising_type: raw[1],
        tx_power: raw[2],
        max_tx_power: raw[3],
        public_key,
        adv_lat: parse_scaled_i32([raw[36], raw[37], raw[38], raw[39]]),
        adv_lon: parse_scaled_i32([raw[40], raw[41], raw[42], raw[43]]),
        multi_acks: raw[44],
        advert_loc_policy: raw[45],
        telemetry_mode_env: modes >> 4 & 0b11,
        telemetry_mode_loc: modes >> 2 & 0b11,
        telemetry_mode_base: modes & 0b11,
        manual_add_contacts: raw[47] != 0,
        radio_frequency_mhz: f64::from(u32::from_le_bytes([raw[48], raw[49], raw[50], raw[51]]))
            / 1000.0,
        radio_bandwidth_khz: f64::from(u32::from_le_bytes([raw[52], raw[53], raw[54], raw[55]]))
            / 1000.0,
        radio_spreading_factor: raw[56],
        radio_coding_rate: raw[57],
        name: decode_padded_string(&raw[58..], "SelfInfo.name")?,
    }))
}

fn parse_device_info(raw: &[u8]) -> Result<Packet, ParseError> {
    require_len(raw, 2, PacketCode::DeviceInfo)?;
    let protocol_version = raw[1];
    let mut info = DeviceInfo {
        protocol_version,
        max_contacts: None,
        max_channels: None,
        ble_pin: None,
        firmware_build: None,
        model: None,
        firmware_version: None,
        repeat_enabled: None,
        path_hash_mode: None,
    };

    if protocol_version >= 3 {
        require_len(raw, 80, PacketCode::DeviceInfo)?;
        info.max_contacts = Some(u16::from(raw[2]) * 2);
        info.max_channels = Some(raw[3]);
        info.ble_pin = Some(u32::from_le_bytes([raw[4], raw[5], raw[6], raw[7]]));
        info.firmware_build = Some(decode_padded_string(
            &raw[8..20],
            "DeviceInfo.firmware_build",
        )?);
        info.model = Some(decode_padded_string(&raw[20..60], "DeviceInfo.model")?);
        info.firmware_version = Some(decode_padded_string(&raw[60..80], "DeviceInfo.version")?);
    }
    if protocol_version >= 9 {
        require_len(raw, 81, PacketCode::DeviceInfo)?;
        info.repeat_enabled = Some(raw[80] != 0);
    }
    if protocol_version >= 10 {
        require_len(raw, 82, PacketCode::DeviceInfo)?;
        info.path_hash_mode = Some(raw[81]);
    }

    Ok(Packet::DeviceInfo(info))
}

fn parse_msg_sent(raw: &[u8]) -> Result<Packet, ParseError> {
    require_len(raw, 10, PacketCode::MsgSent)?;
    let mut expected_ack = [0u8; 4];
    expected_ack.copy_from_slice(&raw[2..6]);
    Ok(Packet::MsgSent {
        destination_type: raw[1],
        expected_ack,
        suggested_timeout_ms: u32::from_le_bytes([raw[6], raw[7], raw[8], raw[9]]),
    })
}

fn parse_contact_msg(raw: &[u8], is_v3: bool) -> Result<Packet, ParseError> {
    let code = if is_v3 {
        PacketCode::ContactMsgRecvV3
    } else {
        PacketCode::ContactMsgRecv
    };
    require_len(raw, if is_v3 { 16 } else { 13 }, code)?;
    let mut cursor = 1;
    let snr = if is_v3 {
        let value = Some(f32::from(i8::from_le_bytes([raw[cursor]])) / 4.0);
        cursor += 3;
        value
    } else {
        None
    };

    let prefix_start = cursor;
    let route = message_route(raw[prefix_start + 6]);
    let pubkey_prefix = hex::encode(&raw[prefix_start..prefix_start + 6]);

    cursor = prefix_start + 7;
    let txt_type = raw[cursor];
    let sender_timestamp = u32::from_le_bytes([
        raw[cursor + 1],
        raw[cursor + 2],
        raw[cursor + 3],
        raw[cursor + 4],
    ]);
    cursor += 5;

    let mut signature = None;
    if txt_type == 0x02 {
        require_len(raw, cursor + 4, code)?;
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&raw[cursor..cursor + 4]);
        cursor += 4;
        signature = Some(bytes);
    }

    let text = decode_text(
        &raw[cursor..],
        if is_v3 {
            "ContactMsg.text_v3"
        } else {
            "ContactMsg.text"
        },
    )?;
    Ok(Packet::ContactMsg(Message {
        observation_id: None,
        source: MessageSource::Direct { pubkey_prefix },
        route,
        txt_type,
        sender_timestamp,
        signature,
        text,
        snr,
        status: MessageStatus::Received,
    }))
}

fn parse_channel_msg(raw: &[u8], is_v3: bool) -> Result<Packet, ParseError> {
    let code = if is_v3 {
        PacketCode::ChannelMsgRecvV3
    } else {
        PacketCode::ChannelMsgRecv
    };
    require_len(raw, if is_v3 { 11 } else { 8 }, code)?;
    let mut cursor = 1;
    let snr = if is_v3 {
        let value = Some(f32::from(i8::from_le_bytes([raw[cursor]])) / 4.0);
        cursor += 3;
        value
    } else {
        None
    };

    let channel_idx = raw[cursor];
    cursor += 1;
    let route = message_route(raw[cursor]);
    let txt_type = raw[cursor + 1];
    let sender_timestamp = u32::from_le_bytes([
        raw[cursor + 2],
        raw[cursor + 3],
        raw[cursor + 4],
        raw[cursor + 5],
    ]);
    let text = decode_text(
        &raw[cursor + 6..],
        if is_v3 {
            "ChannelMsg.text_v3"
        } else {
            "ChannelMsg.text"
        },
    )?;

    Ok(Packet::ChannelMsg(Message {
        observation_id: None,
        source: MessageSource::Channel { channel_idx },
        route,
        txt_type,
        sender_timestamp,
        signature: None,
        text,
        snr,
        status: MessageStatus::Received,
    }))
}

fn message_route(route_byte: u8) -> MessageRoute {
    if route_byte == u8::MAX {
        MessageRoute::Direct
    } else {
        MessageRoute::Path {
            hash_mode: route_byte >> 6,
            hop_count: route_byte & 0x3f,
        }
    }
}

fn parse_current_time(raw: &[u8]) -> Result<Packet, ParseError> {
    require_exact_len(raw, 5, PacketCode::CurrentTime)?;
    Ok(Packet::CurrentTime(u32::from_le_bytes([
        raw[1], raw[2], raw[3], raw[4],
    ])))
}

fn parse_contact_uri(raw: &[u8]) -> Result<Packet, ParseError> {
    require_len(raw, 2, PacketCode::ContactUri)?;
    let card = raw[1..].to_vec();
    Ok(Packet::ContactUri(ContactUri {
        uri: format!("meshcore://{}", hex::encode(&card)),
        card,
    }))
}

fn parse_battery(raw: &[u8]) -> Result<Packet, ParseError> {
    require_len(raw, 3, PacketCode::Battery)?;
    let level = u16::from_le_bytes([raw[1], raw[2]]);
    // Compatibility: meshcore/reader.py:399-418 retains the 3-byte battery-only layout while
    // accepting the current 11-byte battery-and-storage layout.
    let (used_kb, total_kb) = if raw.len() == 3 {
        (None, None)
    } else if raw.len() == 11 {
        (
            Some(u32::from_le_bytes([raw[3], raw[4], raw[5], raw[6]])),
            Some(u32::from_le_bytes([raw[7], raw[8], raw[9], raw[10]])),
        )
    } else {
        return if raw.len() < 11 {
            Err(ParseError::InvalidPacketLength {
                code: PacketDisplay::Raw(PacketCode::Battery.to_u8()),
                minimum: 11,
                actual: raw.len(),
            })
        } else {
            Err(ParseError::Malformed {
                reason: "battery packet contains unexpected trailing bytes",
            })
        };
    };
    Ok(Packet::Battery(BatteryInfo {
        level,
        used_kb,
        total_kb,
    }))
}

fn parse_tuning_params(raw: &[u8]) -> Result<Packet, ParseError> {
    require_exact_len(raw, 9, PacketCode::TuningParams)?;
    Ok(Packet::TuningParams(TuningParams {
        rx_delay: read_u32(raw, 1),
        airtime_factor: read_u32(raw, 5),
    }))
}

fn parse_custom_vars(raw: &[u8]) -> Result<Packet, ParseError> {
    const MAX_CUSTOM_VARS_BYTES: usize = 140;
    if raw.len().saturating_sub(1) > MAX_CUSTOM_VARS_BYTES {
        return Err(ParseError::Malformed {
            reason: "custom-variable payload exceeds firmware response bound",
        });
    }
    let text = std::str::from_utf8(&raw[1..]).map_err(|_| ParseError::InvalidUtf8Payload {
        context: "CustomVariables",
    })?;
    let mut entries = Vec::new();
    if !text.is_empty() {
        for pair in text.split(',') {
            let Some((key, value)) = pair.split_once(':') else {
                return Err(ParseError::Malformed {
                    reason: "custom-variable entry is missing its separator",
                });
            };
            if key.is_empty()
                || value.is_empty()
                || value.contains(':')
                || key.contains('\0')
                || value.contains('\0')
            {
                return Err(ParseError::Malformed {
                    reason: "custom-variable entry is not one strict non-empty key:value pair",
                });
            }
            if entries
                .iter()
                .any(|entry: &CustomVariable| entry.key == key)
            {
                return Err(ParseError::Malformed {
                    reason: "custom-variable payload contains a duplicate key",
                });
            }
            entries.push(CustomVariable {
                key: key.to_owned(),
                value: value.to_owned(),
            });
        }
    }
    Ok(Packet::CustomVariables(CustomVariables {
        raw: raw[1..].to_vec(),
        entries,
    }))
}

fn parse_advert_path(raw: &[u8]) -> Result<Packet, ParseError> {
    require_len(raw, 6, PacketCode::AdvertPath)?;
    let descriptor = raw[5];
    let (route, used_path_bytes) = if descriptor == u8::MAX {
        (ContactRoute::Flood, 0)
    } else {
        let hash_mode = descriptor >> 6;
        let hop_count = descriptor & 0x3f;
        (
            ContactRoute::Path {
                hash_mode,
                hop_count,
            },
            usize::from(hop_count) * (usize::from(hash_mode) + 1),
        )
    };
    require_exact_len(raw, 6 + used_path_bytes, PacketCode::AdvertPath)?;
    let path = Path::try_from_bytes(&raw[6..]).map_err(|_| ParseError::Malformed {
        reason: "advert path exceeds supported path bytes",
    })?;
    Ok(Packet::AdvertPath(AdvertPath {
        received_at: read_u32(raw, 1),
        route,
        path,
    }))
}

fn parse_stats(raw: &[u8]) -> Result<Packet, ParseError> {
    require_len(raw, 2, PacketCode::Stats)?;
    let stats = match raw[1] {
        0 => {
            require_exact_len(raw, 11, PacketCode::Stats)?;
            DeviceStats::Core {
                battery_mv: u16::from_le_bytes([raw[2], raw[3]]),
                uptime_seconds: read_u32(raw, 4),
                errors: u16::from_le_bytes([raw[8], raw[9]]),
                queue_length: raw[10],
            }
        }
        1 => {
            require_exact_len(raw, 14, PacketCode::Stats)?;
            DeviceStats::Radio {
                noise_floor: i16::from_le_bytes([raw[2], raw[3]]),
                last_rssi: i8::from_le_bytes([raw[4]]),
                last_snr: f32::from(i8::from_le_bytes([raw[5]])) / 4.0,
                tx_airtime_seconds: read_u32(raw, 6),
                rx_airtime_seconds: read_u32(raw, 10),
            }
        }
        2 => {
            // Compatibility: meshcore/reader.py:506-530 supports the legacy 26-byte counters
            // packet and the current 30-byte packet with `recv_errors`.
            if !matches!(raw.len(), 26 | 30) {
                require_len(raw, 26, PacketCode::Stats)?;
                return Err(ParseError::Malformed {
                    reason: "packet statistics must use the 26- or 30-byte layout",
                });
            }
            DeviceStats::Packets {
                recv: read_u32(raw, 2),
                sent: read_u32(raw, 6),
                flood_sent: read_u32(raw, 10),
                direct_sent: read_u32(raw, 14),
                flood_recv: read_u32(raw, 18),
                direct_recv: read_u32(raw, 22),
                recv_errors: (raw.len() == 30).then(|| read_u32(raw, 26)),
            }
        }
        _ => {
            return Err(ParseError::Malformed {
                reason: "unknown statistics subtype",
            });
        }
    };
    Ok(Packet::DeviceStats(stats))
}

fn parse_auto_add_config(raw: &[u8]) -> Result<Packet, ParseError> {
    // Compatibility: meshcore/reader.py:539-549 accepts the pre-v1.14.0 response without the
    // trailing `max_hops` byte as well as the current response.
    if !matches!(raw.len(), 2 | 3) {
        require_len(raw, 2, PacketCode::AutoaddConfig)?;
        return Err(ParseError::Malformed {
            reason: "auto-add config must use the 2- or 3-byte layout",
        });
    }
    let max_hops = raw.get(2).copied();
    if max_hops.is_some_and(|value| value > 64) {
        return Err(ParseError::Malformed {
            reason: "auto-add max hops exceeds firmware bound",
        });
    }
    Ok(Packet::AutoAddConfig(AutoAddConfig {
        config: raw[1],
        max_hops,
    }))
}

fn parse_allowed_repeat_frequencies(raw: &[u8]) -> Result<Packet, ParseError> {
    if !(raw.len() - 1).is_multiple_of(8) {
        return Err(ParseError::Malformed {
            reason: "repeat-frequency payload contains a truncated range",
        });
    }
    let mut ranges = Vec::with_capacity((raw.len() - 1) / 8);
    for chunk in raw[1..].chunks_exact(8) {
        let lower_khz = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        let upper_khz = u32::from_le_bytes([chunk[4], chunk[5], chunk[6], chunk[7]]);
        if lower_khz == 0 || upper_khz == 0 || lower_khz > upper_khz {
            return Err(ParseError::Malformed {
                reason: "repeat-frequency range is invalid",
            });
        }
        ranges.push(FrequencyRange {
            lower_khz,
            upper_khz,
        });
    }
    Ok(Packet::AllowedRepeatFrequencies(ranges))
}

fn parse_default_flood_scope(raw: &[u8]) -> Result<Packet, ParseError> {
    if raw.len() == 1 {
        return Ok(Packet::DefaultFloodScope(DefaultFloodScope::Unconfigured));
    }
    require_exact_len(raw, 48, PacketCode::DefaultFloodScope)?;
    let Some(name_end) = raw[1..32].iter().position(|byte| *byte == 0) else {
        return Err(ParseError::Malformed {
            reason: "default flood scope name is not NUL terminated",
        });
    };
    if name_end == 0 {
        return Err(ParseError::Malformed {
            reason: "configured default flood scope has an empty name",
        });
    }
    let name = std::str::from_utf8(&raw[1..=name_end])
        .map_err(|_| ParseError::InvalidUtf8Payload {
            context: "DefaultFloodScope.name",
        })?
        .to_owned();
    let mut key = [0_u8; 16];
    key.copy_from_slice(&raw[32..48]);
    Ok(Packet::DefaultFloodScope(DefaultFloodScope::Configured(
        Scope::new(Some(name), key),
    )))
}

fn read_u32(raw: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        raw[offset],
        raw[offset + 1],
        raw[offset + 2],
        raw[offset + 3],
    ])
}

fn read_u16(raw: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([raw[offset], raw[offset + 1]])
}

fn read_i16(raw: &[u8], offset: usize) -> i16 {
    i16::from_le_bytes([raw[offset], raw[offset + 1]])
}

fn parse_channel_info(raw: &[u8]) -> Result<Packet, ParseError> {
    require_len(raw, 50, PacketCode::ChannelInfo)?;
    let idx = raw[1];
    let name = decode_padded_string(&raw[2..34], "ChannelInfo.name")?;
    let mut secret = [0u8; 16];
    secret.copy_from_slice(&raw[34..50]);
    Ok(Packet::ChannelInfo(ChannelInfo::with_secret(
        idx,
        name,
        Some(secret),
    )))
}

fn parse_ack(raw: &[u8]) -> Result<Packet, ParseError> {
    require_len(raw, 5, PacketCode::Ack)?;
    if (6..9).contains(&raw.len()) {
        return Err(ParseError::Malformed {
            reason: "ACK packet contains a truncated trip-time field",
        });
    }
    let mut code = [0u8; 4];
    code.copy_from_slice(&raw[1..5]);
    let trip_time_ms = if raw.len() >= 9 {
        Some(u32::from_le_bytes([raw[5], raw[6], raw[7], raw[8]]))
    } else {
        None
    };
    Ok(Packet::Ack(Ack { code, trip_time_ms }))
}

fn decode_padded_string(raw: &[u8], context: &'static str) -> Result<String, ParseError> {
    let end = raw
        .iter()
        .position(|value| value == &0)
        .unwrap_or(raw.len());
    String::from_utf8(raw[..end].to_vec()).map_err(|_| ParseError::InvalidUtf8Payload { context })
}

fn decode_text(raw: &[u8], context: &'static str) -> Result<String, ParseError> {
    String::from_utf8(raw.to_vec()).map_err(|_| ParseError::InvalidUtf8Payload { context })
}

fn parse_scaled_i32(raw: [u8; 4]) -> f64 {
    f64::from(i32::from_le_bytes(raw)) / 1_000_000.0
}

/// Parsing command/packet-level conversion problem.
#[derive(Clone, Debug)]
pub struct CommandError {
    /// Unsupported command code value.
    pub code: u8,
}

impl std::fmt::Display for CommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "protocol command code {:02x} is not supported",
            self.code
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(raw: &[u8]) -> Packet {
        match Packet::parse(raw) {
            Ok(packet) => packet,
            Err(error) => panic!("packet should parse: {error}"),
        }
    }

    fn built(result: Result<Command, CoreError>) -> Command {
        match result {
            Ok(command) => command,
            Err(error) => panic!("command should build: {error}"),
        }
    }

    fn contact_packet() -> Vec<u8> {
        let mut raw = vec![0_u8; 148];
        raw[0] = PacketCode::Contact.to_u8();
        raw[1..33].fill(0x11);
        raw[33] = 1;
        raw[34] = 0x05;
        raw[35] = (1 << 6) | 2;
        raw[36..40].copy_from_slice(&[0xaa, 0xbb, 0xcc, 0xdd]);
        raw[40..100].fill(0xee);
        raw[100..105].copy_from_slice(b"Alice");
        raw[132..136].copy_from_slice(&123_u32.to_le_bytes());
        raw[136..140].copy_from_slice(&(-34_900_000_i32).to_le_bytes());
        raw[140..144].copy_from_slice(&138_600_000_i32.to_le_bytes());
        raw[144..148].copy_from_slice(&456_u32.to_le_bytes());
        raw
    }

    fn self_info_packet() -> Vec<u8> {
        let mut raw = vec![0_u8; 62];
        raw[0] = PacketCode::SelfInfo.to_u8();
        raw[1] = 2;
        raw[2] = 17;
        raw[3] = 22;
        raw[4..36].fill(0x22);
        raw[36..40].copy_from_slice(&(-34_900_000_i32).to_le_bytes());
        raw[40..44].copy_from_slice(&138_600_000_i32.to_le_bytes());
        raw[44] = 3;
        raw[45] = 1;
        raw[46] = 0b10_01_11;
        raw[47] = 1;
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

    #[test]
    fn app_start_matches_current_companion_wire_bytes() {
        assert_eq!(
            Command::app_start().encode(),
            b"\x01\x03      mccli".to_vec()
        );
    }

    #[test]
    fn device_command_builders_match_upstream_wire_bytes() {
        assert_eq!(Command::get_time().encode(), vec![0x05]);
        assert_eq!(
            Command::set_time(0x1234_5678).encode(),
            vec![0x06, 0x78, 0x56, 0x34, 0x12]
        );
        assert_eq!(Command::get_battery().encode(), vec![0x14]);
        assert_eq!(Command::send_self_advert(false).encode(), vec![0x07]);
        assert_eq!(Command::send_self_advert(true).encode(), vec![0x07, 0x01]);
        assert_eq!(
            built(Command::set_advert_name("nøde")).encode(),
            b"\x08n\xc3\xb8de"
        );

        let mut coordinates = vec![0x0e];
        coordinates.extend_from_slice(&(-34_900_000_i32).to_le_bytes());
        coordinates.extend_from_slice(&138_600_000_i32.to_le_bytes());
        coordinates.extend_from_slice(&0_i32.to_le_bytes());
        assert_eq!(
            built(Command::set_coordinates(-34.9, 138.6)).encode(),
            coordinates
        );

        assert_eq!(built(Command::set_tx_power(-9)).encode(), vec![0x0c, 0xf7]);
        let params = RadioParams {
            frequency_khz: 915_000,
            bandwidth_hz: 62_500,
            spreading_factor: 7,
            coding_rate: 5,
            repeat: Some(true),
        };
        let mut radio = vec![0x0b];
        radio.extend_from_slice(&915_000_u32.to_le_bytes());
        radio.extend_from_slice(&62_500_u32.to_le_bytes());
        radio.extend_from_slice(&[7, 5, 1]);
        assert_eq!(built(Command::set_radio_params(&params)).encode(), radio);

        let tuning = TuningParams {
            rx_delay: 1_250,
            airtime_factor: 3_000,
        };
        let mut set_tuning = vec![0x15];
        set_tuning.extend_from_slice(&1_250_u32.to_le_bytes());
        set_tuning.extend_from_slice(&3_000_u32.to_le_bytes());
        set_tuning.extend_from_slice(&[0, 0]);
        assert_eq!(Command::set_tuning(tuning).encode(), set_tuning);
        assert_eq!(Command::get_tuning().encode(), vec![0x2b]);
        assert_eq!(
            built(Command::set_path_hash_mode(2)).encode(),
            vec![0x3d, 0, 2]
        );
    }

    #[test]
    fn contact_and_configuration_builders_match_upstream_wire_bytes() {
        let key = [0x42_u8; 32];
        let mut keyed = vec![0x0d];
        keyed.extend_from_slice(&key);
        assert_eq!(built(Command::reset_path(&key)).encode(), keyed);

        for (command, code) in [
            (built(Command::share_contact(&key)), 0x10),
            (built(Command::export_contact(Some(&key))), 0x11),
            (built(Command::get_contact(&key)), 0x1e),
        ] {
            let mut expected = vec![code];
            expected.extend_from_slice(&key);
            assert_eq!(command.encode(), expected);
        }
        assert_eq!(built(Command::export_contact(None)).encode(), vec![0x11]);

        let contact = Contact {
            public_key: PublicKey::try_from_bytes(&key).expect("test key should be valid"),
            contact_type: ContactType::Repeater,
            flags: 1,
            route: ContactRoute::Path {
                hash_mode: 1,
                hop_count: 2,
            },
            out_path: Path::try_from_bytes(&[0x10, 0x20, 0x30, 0x40])
                .expect("test path should be valid"),
            adv_name: "relay".to_owned(),
            last_advert: 0x1234_5678,
            adv_lat: -34.9,
            adv_lon: 138.6,
            lastmod: 0xffff_ffff,
        };
        let mut update = vec![0x09];
        update.extend_from_slice(&key);
        update.extend_from_slice(&[1, 1, 0x42]);
        update.extend_from_slice(&[0x10, 0x20, 0x30, 0x40]);
        update.resize(1 + 32 + 1 + 1 + 1 + 64, 0);
        update.extend_from_slice(b"relay");
        update.resize(1 + 32 + 1 + 1 + 1 + 64 + 32, 0);
        update.extend_from_slice(&0x1234_5678_u32.to_le_bytes());
        update.extend_from_slice(&(-34_900_000_i32).to_le_bytes());
        update.extend_from_slice(&138_600_000_i32.to_le_bytes());
        assert_eq!(built(Command::update_contact(&contact)).encode(), update);
        assert_eq!(update.len(), 144);

        let mut advert_path = vec![0x2a, 0];
        advert_path.extend_from_slice(&key);
        assert_eq!(built(Command::get_advert_path(&key)).encode(), advert_path);

        let card = vec![0x51; 98];
        let mut import = vec![0x12];
        import.extend_from_slice(&card);
        assert_eq!(built(Command::import_contact(&card)).encode(), import);

        assert_eq!(
            built(Command::set_auto_add_config(AutoAddConfig {
                config: u8::MAX,
                max_hops: Some(64),
            }))
            .encode(),
            vec![0x3a, u8::MAX, 64]
        );
        assert_eq!(Command::get_auto_add_config().encode(), vec![0x3b]);
        assert_eq!(Command::get_custom_vars().encode(), vec![0x28]);
        assert_eq!(
            built(Command::set_custom_var("gps", "1")).encode(),
            b"\x29gps:1"
        );
        assert_eq!(Command::get_stats(StatsType::Core).encode(), vec![0x38, 0]);
        assert_eq!(Command::get_stats(StatsType::Radio).encode(), vec![0x38, 1]);
        assert_eq!(
            Command::get_stats(StatsType::Packets).encode(),
            vec![0x38, 2]
        );
        assert_eq!(
            Command::get_allowed_repeat_frequencies().encode(),
            vec![0x3c]
        );

        assert_eq!(
            Command::set_flood_scope(&FloodScope::Default).encode(),
            vec![0x36, 0]
        );
        assert_eq!(
            Command::set_flood_scope(&FloodScope::Unscoped).encode(),
            vec![0x36, 1]
        );
        let scope_key = [0xa5_u8; 16];
        let mut scoped = vec![0x36, 0];
        scoped.extend_from_slice(&scope_key);
        assert_eq!(
            Command::set_flood_scope(&FloodScope::Key(scope_key)).encode(),
            scoped
        );

        let mut default_scope = vec![0x3f];
        default_scope.extend_from_slice(b"local");
        default_scope.resize(1 + 31, 0);
        default_scope.extend_from_slice(&scope_key);
        assert_eq!(
            built(Command::set_default_flood_scope("local", scope_key)).encode(),
            default_scope
        );
        assert_eq!(Command::clear_default_flood_scope().encode(), vec![0x3f]);
        assert_eq!(Command::get_default_flood_scope().encode(), vec![0x40]);
    }

    #[test]
    fn channel_remote_and_privileged_builders_match_upstream_wire_bytes() {
        let key = [0x42_u8; 32];
        let mut remove = vec![0x0f];
        remove.extend_from_slice(&key);
        assert_eq!(built(Command::remove_contact(&key)).encode(), remove);

        assert_eq!(Command::get_channel(3).encode(), vec![0x1f, 3]);
        let mut channel = vec![0_u8; 50];
        channel[0] = 0x20;
        channel[1] = 3;
        channel[2..7].copy_from_slice(b"alpha");
        channel[34..].fill(0xa5);
        assert_eq!(
            built(Command::set_channel(3, "alpha", [0xa5; 16])).encode(),
            channel
        );
        assert_eq!(Command::clear_channel(3).encode(), {
            let mut clear = vec![0_u8; 50];
            clear[0] = 0x20;
            clear[1] = 3;
            clear
        });

        let mut login = vec![0x1a];
        login.extend_from_slice(&key);
        login.extend_from_slice(b"correct horse");
        assert_eq!(
            built(Command::send_login(&key, "correct horse")).encode(),
            login
        );
        for (command, code) in [
            (built(Command::send_status_request(&key)), 0x1b),
            (built(Command::has_connection(&key)), 0x1c),
            (built(Command::logout(&key)), 0x1d),
        ] {
            let mut expected = vec![code];
            expected.extend_from_slice(&key);
            assert_eq!(command.encode(), expected);
        }
        assert_eq!(Command::get_self_telemetry().encode(), vec![0x27, 0, 0, 0]);

        let mut binary = vec![0x32];
        binary.extend_from_slice(&key);
        binary.extend_from_slice(&[7, 0xde, 0xad]);
        assert_eq!(
            built(Command::send_binary_request(&key, 7, &[0xde, 0xad])).encode(),
            binary
        );
        let reply_path = Path::try_from_bytes(&[0xaa, 0xbb]).expect("test path should be valid");
        let mut anonymous = vec![0x39];
        anonymous.extend_from_slice(&key);
        anonymous.extend_from_slice(&[3, 2, 0xaa, 0xbb]);
        assert_eq!(
            built(Command::send_anonymous_request(
                &key,
                3,
                ContactRoute::Path {
                    hash_mode: 0,
                    hop_count: 2,
                },
                &reply_path,
            ))
            .encode(),
            anonymous
        );
        let mut discovery = vec![0x34, 0];
        discovery.extend_from_slice(&key);
        assert_eq!(built(Command::discover_path(&key)).encode(), discovery);

        assert_eq!(Command::sign_start().encode(), vec![0x21]);
        assert_eq!(built(Command::sign_data(b"chunk")).encode(), b"\x22chunk");
        assert_eq!(Command::sign_finish().encode(), vec![0x23]);
        assert_eq!(Command::export_private_key().encode(), vec![0x17]);
        let private_key = match PrivateKeyMaterial::try_from_bytes(&[0x53; 64]) {
            Ok(key) => key,
            Err(error) => panic!("private key should build: {error}"),
        };
        let mut import_key = vec![0x18];
        import_key.extend_from_slice(&[0x53; 64]);
        assert_eq!(
            Command::import_private_key(&private_key).encode(),
            import_key
        );
        assert_eq!(built(Command::set_device_pin(123_456)).encode(), {
            let mut pin = vec![0x25];
            pin.extend_from_slice(&123_456_u32.to_le_bytes());
            pin
        });
        assert_eq!(Command::reboot().encode(), b"\x13reboot");
        assert_eq!(Command::factory_reset().encode(), b"\x33reset");
    }

    #[test]
    fn node_discovery_builder_matches_exact_wire_bytes() {
        assert_eq!(
            built(Command::send_node_discovery(0x16, true, 0x1234_5678, None)).encode(),
            vec![0x37, 0x81, 0x16, 0x78, 0x56, 0x34, 0x12]
        );
        assert_eq!(
            built(Command::send_node_discovery(
                0xff,
                false,
                0x0102_0304,
                Some(0xa1b2_c3d4),
            ))
            .encode(),
            vec![
                0x37, 0x80, 0xff, 0x04, 0x03, 0x02, 0x01, 0xd4, 0xc3, 0xb2, 0xa1,
            ]
        );
    }

    #[test]
    fn checked_builders_reject_unsafe_or_unrepresentable_values() {
        assert!(Command::set_advert_name("éééééééééééééééé").is_err());
        assert!(Command::set_advert_name("bad\0name").is_err());
        assert!(Command::set_coordinates(f64::NAN, 0.0).is_err());
        assert!(Command::set_coordinates(90.000_001, 0.0).is_err());
        assert!(Command::set_coordinates(0.0, -180.000_001).is_err());
        assert!(Command::set_tx_power(-10).is_err());

        let invalid_radio = RadioParams {
            frequency_khz: 149_999,
            bandwidth_hz: 62_500,
            spreading_factor: 7,
            coding_rate: 5,
            repeat: None,
        };
        assert!(Command::set_radio_params(&invalid_radio).is_err());
        assert!(Command::reset_path(&[0; 31]).is_err());
        assert!(Command::share_contact(&[0; 33]).is_err());
        assert!(Command::get_contact(&[]).is_err());
        assert!(Command::get_advert_path(&[0; 6]).is_err());
        assert!(Command::import_contact(&[0; 97]).is_err());
        assert!(Command::import_contact(&[0; MAX_INNER_PAYLOAD]).is_err());
        assert_eq!(
            built(Command::import_contact(&[0; MAX_INNER_PAYLOAD - 1]))
                .encode()
                .len(),
            MAX_INNER_PAYLOAD
        );
        assert!(Command::set_custom_var("bad:key", "value").is_err());
        assert!(Command::set_custom_var("key", "bad,value").is_err());
        assert!(Command::set_custom_var("", "value").is_err());
        assert!(Command::set_custom_var("key", &"x".repeat(MAX_INNER_PAYLOAD)).is_err());
        assert!(Command::set_path_hash_mode(3).is_err());
        assert!(Command::set_path_hash_mode(u8::MAX).is_err());
        assert!(
            Command::set_auto_add_config(AutoAddConfig {
                config: u8::MAX,
                max_hops: Some(65),
            })
            .is_err()
        );
        assert!(Command::set_default_flood_scope(&"x".repeat(31), [0; 16]).is_err());
        assert!(Command::set_default_flood_scope("", [0; 16]).is_err());
        assert!(Command::set_channel(0, "", [0; 16]).is_err());
        assert!(Command::set_channel(0, "bad\0name", [0; 16]).is_err());
        assert!(Command::remove_contact(&[0; 31]).is_err());
        assert!(Command::send_login(&[0; 31], "password").is_err());
        assert!(Command::send_login(&[0; 32], "bad\0password").is_err());
        assert!(Command::send_binary_request(&[0; 31], 0, &[]).is_err());
        assert!(Command::send_binary_request(&[0; 32], 0, &[0; MAX_INNER_PAYLOAD]).is_err());
        let empty_path = Path::try_from_bytes(&[]).expect("empty path should be valid");
        assert!(
            Command::send_anonymous_request(&[0; 32], 1, ContactRoute::Flood, &empty_path,)
                .is_err()
        );
        assert!(
            Command::send_anonymous_request(
                &[0; 32],
                0,
                ContactRoute::Path {
                    hash_mode: 0,
                    hop_count: 0,
                },
                &empty_path,
            )
            .is_err()
        );
        assert!(Command::discover_path(&[0; 31]).is_err());
        assert!(Command::send_node_discovery(0xff, true, 0, None).is_err());
        assert!(Command::sign_data(&[]).is_err());
        assert!(Command::sign_data(&[0; MAX_INNER_PAYLOAD]).is_err());
        assert!(Command::set_device_pin(1_000_000).is_err());
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn debug_output_redacts_command_scope_path_and_raw_payloads() {
        let secret_text = "TOP_SECRET_SENTINEL";
        let command = built(Command::set_custom_var("token", secret_text));
        let command_debug = format!("{command:?}");
        assert!(command_debug.contains("SetCustomVar"));
        assert!(command_debug.contains("payload_len"));
        assert!(!command_debug.contains(secret_text));

        let scope = Scope::new(Some("PRIVATE_SCOPE_SENTINEL".to_owned()), [0xab; 16]);
        let scope_debug = format!("{scope:?}");
        assert!(!scope_debug.contains("PRIVATE_SCOPE_SENTINEL"));
        assert!(!scope_debug.contains("ababab"));
        assert!(scope_debug.contains("<redacted>"));

        let variables = CustomVariables {
            raw: b"password:RAW_SENTINEL".to_vec(),
            entries: vec![CustomVariable {
                key: "password".to_owned(),
                value: "RAW_SENTINEL".to_owned(),
            }],
        };
        let variables_debug = format!("{variables:?}");
        assert!(!variables_debug.contains("RAW_SENTINEL"));
        assert!(!format!("{:?}", variables.entries[0]).contains("RAW_SENTINEL"));

        let path = AdvertPath {
            received_at: 7,
            route: ContactRoute::Path {
                hash_mode: 1,
                hop_count: 2,
            },
            path: match Path::try_from_bytes(&[0xde, 0xad, 0xbe, 0xef]) {
                Ok(path) => path,
                Err(error) => panic!("path should build: {error}"),
            },
        };
        assert!(!format!("{path:?}").contains("deadbeef"));

        let contact_uri = ContactUri {
            uri: "meshcore://IDENTITY_SENTINEL".to_owned(),
            card: b"CARD_SENTINEL".to_vec(),
        };
        let uri_debug = format!("{contact_uri:?}");
        assert!(!uri_debug.contains("IDENTITY_SENTINEL"));
        assert!(!uri_debug.contains("CARD_SENTINEL"));

        let public_key = match PublicKey::try_from_bytes(&[0xaa; 32]) {
            Ok(key) => key,
            Err(error) => panic!("public key should build: {error}"),
        };
        assert!(!format!("{public_key:?}").contains("aaaaaaaa"));
        let direct_path = match Path::try_from_bytes(&[0xca, 0xfe, 0xba, 0xbe]) {
            Ok(path) => path,
            Err(error) => panic!("path should build: {error}"),
        };
        assert!(!format!("{direct_path:?}").contains("cafebabe"));

        let message = Message {
            observation_id: None,
            source: MessageSource::Direct {
                pubkey_prefix: "CONTACT_GRAPH_SENTINEL".to_owned(),
            },
            route: MessageRoute::Direct,
            txt_type: 0,
            sender_timestamp: 1,
            signature: Some(*b"SIGN"),
            text: "MESSAGE_CONTENT_SENTINEL".to_owned(),
            snr: Some(1.0),
            status: MessageStatus::Failed("DEVICE_REASON_SENTINEL".to_owned()),
        };
        for debug in [
            format!("{message:?}"),
            format!("{:?}", Event::Message(message.clone())),
            format!("{:?}", Packet::ContactMsg(message)),
        ] {
            assert!(!debug.contains("CONTACT_GRAPH_SENTINEL"));
            assert!(!debug.contains("MESSAGE_CONTENT_SENTINEL"));
            assert!(!debug.contains("DEVICE_REASON_SENTINEL"));
            assert!(!debug.contains("SIGN"));
        }

        let device_info = DeviceInfo {
            protocol_version: 3,
            max_contacts: Some(64),
            max_channels: Some(8),
            ble_pin: Some(987_654),
            firmware_build: Some("build".to_owned()),
            model: Some("model".to_owned()),
            firmware_version: Some("version".to_owned()),
            repeat_enabled: None,
            path_hash_mode: None,
        };
        assert!(!format!("{device_info:?}").contains("987654"));
        let device_json = match serde_json::to_string(&device_info) {
            Ok(json) => json,
            Err(error) => panic!("device info should serialize: {error}"),
        };
        assert!(!device_json.contains("ble_pin"));
        assert!(!device_json.contains("987654"));

        let scope_json = match serde_json::to_string(&scope) {
            Ok(json) => json,
            Err(error) => panic!("scope should serialize: {error}"),
        };
        assert!(!scope_json.contains("\"key\""));
        assert!(!scope_json.contains("171"));

        let login = built(Command::send_login(&[0x42; 32], "LOGIN_PASSWORD_SENTINEL"));
        let login_debug = format!("{login:?}");
        assert!(!login_debug.contains("LOGIN_PASSWORD_SENTINEL"));
        assert!(!login_debug.contains("424242"));
    }

    #[test]
    fn private_key_packets_are_strict_zeroizing_and_never_events_or_serialized() {
        let mut raw = vec![PacketCode::PrivateKey.to_u8()];
        raw.extend_from_slice(&[0x53; 64]);
        let packet = parsed(&raw);
        match &packet {
            Packet::PrivateKey(key) => assert_eq!(key.expose_secret(), &[0x53; 64]),
            other => panic!("unexpected packet: {other:?}"),
        }
        let debug = format!("{packet:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("535353"));
        assert!(serde_json::to_string(&packet).is_err());
        assert!(packet.into_event().is_none());

        assert!(Packet::parse(&raw[..64]).is_err());
        raw.push(0x53);
        assert!(Packet::parse(&raw).is_err());
    }

    #[test]
    fn parses_remote_session_packets_with_strict_extensions_and_redaction() {
        let prefix = [1, 2, 3, 4, 5, 6];
        let mut success = vec![PacketCode::LoginSuccess.to_u8(), 3];
        success.extend_from_slice(&prefix);
        success.extend_from_slice(&42_u32.to_le_bytes());
        success.extend_from_slice(&[5, 7]);
        match parsed(&success) {
            Packet::LoginSuccess(session) => {
                assert_eq!(session.permissions, 3);
                assert_eq!(session.pubkey_prefix, prefix);
                assert_eq!(session.server_timestamp, Some(42));
                assert_eq!(session.acl_permissions, Some(5));
                assert_eq!(session.firmware_version_level, Some(7));
                let debug = format!("{session:?}");
                assert!(!debug.contains("[1, 2, 3, 4, 5, 6]"));
                assert!(debug.contains("<redacted>"));
            }
            other => panic!("unexpected packet: {other:?}"),
        }
        for accepted_len in [8, 12, 13, 14] {
            assert!(Packet::parse(&success[..accepted_len]).is_ok());
        }
        for rejected_len in [9, 10, 11] {
            assert!(Packet::parse(&success[..rejected_len]).is_err());
        }
        let mut too_long = success;
        too_long.push(0);
        assert!(Packet::parse(&too_long).is_err());

        let mut failed = vec![PacketCode::LoginFailed.to_u8(), 0];
        failed.extend_from_slice(&prefix);
        assert!(matches!(
            parsed(&failed),
            Packet::LoginFailed { pubkey_prefix } if pubkey_prefix == prefix
        ));
        failed[1] = 1;
        assert!(Packet::parse(&failed).is_err());
        assert!(Packet::parse(&failed[..7]).is_err());
    }

    #[test]
    fn parses_status_telemetry_binary_and_path_responses_strictly() {
        let prefix = [1, 2, 3, 4, 5, 6];
        let mut status = vec![PacketCode::StatusResponse.to_u8(), 0];
        status.extend_from_slice(&prefix);
        status.extend_from_slice(&4_200_u16.to_le_bytes());
        status.extend_from_slice(&3_u16.to_le_bytes());
        status.extend_from_slice(&(-115_i16).to_le_bytes());
        status.extend_from_slice(&(-70_i16).to_le_bytes());
        for value in 1_u32..=8 {
            status.extend_from_slice(&value.to_le_bytes());
        }
        status.extend_from_slice(&9_u16.to_le_bytes());
        status.extend_from_slice(&6_i16.to_le_bytes());
        status.extend_from_slice(&10_u16.to_le_bytes());
        status.extend_from_slice(&11_u16.to_le_bytes());
        status.extend_from_slice(&12_u32.to_le_bytes());
        status.extend_from_slice(&13_u32.to_le_bytes());
        assert_eq!(status.len(), 64);
        match parsed(&status) {
            Packet::RemoteStatus(parsed_status) => {
                assert_eq!(parsed_status.pubkey_prefix, prefix);
                assert_eq!(parsed_status.battery_mv, 4_200);
                assert_eq!(parsed_status.tx_queue_length, 3);
                assert_eq!(parsed_status.noise_floor, -115);
                assert_eq!(parsed_status.last_rssi, -70);
                assert_eq!(parsed_status.uptime_seconds, 4);
                assert!((parsed_status.last_snr - 1.5).abs() < f32::EPSILON);
                assert_eq!(parsed_status.rx_airtime_seconds, 12);
                assert_eq!(parsed_status.receive_errors, Some(13));
                assert!(!format!("{parsed_status:?}").contains("[1, 2, 3, 4, 5, 6]"));
            }
            other => panic!("unexpected packet: {other:?}"),
        }
        assert!(Packet::parse(&status[..59]).is_err());
        assert!(Packet::parse(&status[..60]).is_ok());
        let mut status_bad_reserved = status;
        status_bad_reserved[1] = 1;
        assert!(Packet::parse(&status_bad_reserved).is_err());

        let mut telemetry = vec![PacketCode::TelemetryResponse.to_u8(), 0];
        telemetry.extend_from_slice(&prefix);
        telemetry.extend_from_slice(&[0xaa, 0xbb]);
        assert!(matches!(
            parsed(&telemetry),
            Packet::TelemetryResponse(TelemetryResponse { payload, .. }) if payload == [0xaa, 0xbb]
        ));
        telemetry[1] = 1;
        assert!(Packet::parse(&telemetry).is_err());

        let mut binary = vec![PacketCode::BinaryResponse.to_u8(), 0];
        binary.extend_from_slice(&[9, 8, 7, 6]);
        binary.extend_from_slice(&[0xde, 0xad]);
        assert!(matches!(
            parsed(&binary),
            Packet::BinaryResponse(BinaryResponse { tag, payload })
                if tag == [9, 8, 7, 6] && payload == [0xde, 0xad]
        ));
        assert!(Packet::parse(&binary[..5]).is_err());

        let mut path = vec![PacketCode::PathDiscoveryResponse.to_u8(), 0];
        path.extend_from_slice(&prefix);
        path.push((1 << 6) | 2);
        path.extend_from_slice(&[0xaa, 0xbb, 0xcc, 0xdd]);
        path.push(u8::MAX);
        match parsed(&path) {
            Packet::PathDiscovery(discovery) => {
                assert_eq!(discovery.pubkey_prefix, prefix);
                assert_eq!(
                    discovery.outbound_path.as_bytes(),
                    &[0xaa, 0xbb, 0xcc, 0xdd]
                );
                assert_eq!(discovery.inbound_route, ContactRoute::Flood);
                assert!(discovery.inbound_path.as_bytes().is_empty());
            }
            other => panic!("unexpected packet: {other:?}"),
        }
        path.push(0);
        assert!(Packet::parse(&path).is_err());
    }

    #[test]
    fn control_data_and_node_discovery_responses_are_strict_and_redacted() {
        let tag = 0x1234_5678_u32;
        let mut raw = vec![
            PacketCode::ControlData.to_u8(),
            (-8_i8).to_le_bytes()[0],
            (-91_i8).to_le_bytes()[0],
            0,
            0x92,
            (-12_i8).to_le_bytes()[0],
        ];
        raw.extend_from_slice(&tag.to_le_bytes());
        raw.extend_from_slice(&[0x42; 8]);

        for len in 0..5 {
            assert!(Packet::parse(&raw[..len]).is_err());
        }

        let packet = parsed(&raw);
        assert_eq!(packet.code(), PacketCode::ControlData);
        let data = match &packet {
            Packet::ControlData(data) => data,
            other => panic!("unexpected packet: {other:?}"),
        };
        assert_eq!(data.snr_qdb, -8);
        assert_eq!(data.rssi, -91);
        assert_eq!(data.path_len, 0);
        let response = match data.node_discovery_response() {
            Ok(Some(response)) => response,
            Ok(None) => panic!("node-discovery response was not recognized"),
            Err(error) => panic!("node-discovery response should parse: {error}"),
        };
        assert_eq!(response.node_type, 2);
        assert_eq!(response.inbound_snr_qdb, -12);
        assert_eq!(response.tag, tag);
        assert_eq!(response.public_key, [0x42; 8]);
        let packet_debug = format!("{packet:?}");
        let response_debug = format!("{response:?}");
        assert!(!packet_debug.contains("424242"));
        assert!(!response_debug.contains("424242"));
        assert!(response_debug.contains("public_key_len"));
        assert!(matches!(packet.into_event(), Some(Event::ControlData(_))));

        let unrelated = ControlData {
            snr_qdb: 0,
            rssi: 0,
            path_len: 0,
            payload: vec![0x80],
        };
        assert!(matches!(unrelated.node_discovery_response(), Ok(None)));

        for key_len in [0, 1, 7, 9, 31, 33] {
            let mut payload = vec![0x91, 0];
            payload.extend_from_slice(&tag.to_le_bytes());
            payload.resize(6 + key_len, 0x55);
            let malformed = ControlData {
                snr_qdb: 0,
                rssi: 0,
                path_len: 0,
                payload,
            };
            assert!(
                malformed.node_discovery_response().is_err(),
                "accepted public-key width {key_len}"
            );
        }

        for key_len in [8, 32] {
            let mut payload = vec![0x94, 4];
            payload.extend_from_slice(&tag.to_le_bytes());
            payload.resize(6 + key_len, 0x66);
            let valid = ControlData {
                snr_qdb: 0,
                rssi: 0,
                path_len: 0,
                payload,
            };
            assert!(matches!(
                valid.node_discovery_response(),
                Ok(Some(response)) if response.public_key.len() == key_len
            ));
        }

        let zero_tag = ControlData {
            snr_qdb: 0,
            rssi: 0,
            path_len: 0,
            payload: vec![0x90, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8],
        };
        assert!(zero_tag.node_discovery_response().is_err());
    }

    #[test]
    fn signing_and_disabled_packets_enforce_exact_lengths() {
        let mut start = vec![PacketCode::SignStart.to_u8(), 0];
        start.extend_from_slice(&512_u32.to_le_bytes());
        assert!(matches!(
            parsed(&start),
            Packet::SignStart {
                max_data_bytes: 512
            }
        ));
        start[1] = 1;
        assert!(Packet::parse(&start).is_err());

        let mut signature = vec![PacketCode::Signature.to_u8()];
        signature.extend_from_slice(&[0x5a; 64]);
        match parsed(&signature) {
            Packet::Signature(signature) => assert_eq!(signature.as_bytes(), &[0x5a; 64]),
            other => panic!("unexpected packet: {other:?}"),
        }
        assert!(Packet::parse(&signature[..64]).is_err());
        signature.push(0);
        assert!(Packet::parse(&signature).is_err());
        assert!(Packet::parse(&[PacketCode::Disabled.to_u8()]).is_ok());
        assert!(Packet::parse(&[PacketCode::Disabled.to_u8(), 0]).is_err());

        let invalid_signature_json = format!("[{}]", vec!["1"; 63].join(","));
        assert!(serde_json::from_str::<Signature>(&invalid_signature_json).is_err());
    }

    #[test]
    fn contact_uses_fixed_path_field_before_name() {
        match parsed(&contact_packet()) {
            Packet::Contact(contact) => {
                assert_eq!(contact.adv_name, "Alice");
                assert_eq!(contact.out_path.as_bytes(), &[0xaa, 0xbb, 0xcc, 0xdd]);
                assert_eq!(
                    contact.route,
                    ContactRoute::Path {
                        hash_mode: 1,
                        hop_count: 2
                    }
                );
                assert_eq!(contact.lastmod, 456);
            }
            other => panic!("unexpected packet: {other:?}"),
        }
    }

    #[test]
    fn parses_self_and_device_info_as_distinct_layouts() {
        match parsed(&self_info_packet()) {
            Packet::SelfInfo(info) => {
                assert_eq!(info.name, "node");
                assert!((info.radio_frequency_mhz - 915.0).abs() < f64::EPSILON);
                assert!((info.radio_bandwidth_khz - 62.5).abs() < f64::EPSILON);
                assert!(info.manual_add_contacts);
            }
            other => panic!("unexpected packet: {other:?}"),
        }

        match parsed(&device_info_packet()) {
            Packet::DeviceInfo(info) => {
                assert_eq!(info.protocol_version, 10);
                assert_eq!(info.max_contacts, Some(100));
                assert_eq!(info.firmware_version.as_deref(), Some("1.16.0"));
                assert_eq!(info.repeat_enabled, Some(true));
                assert_eq!(info.path_hash_mode, Some(2));
            }
            other => panic!("unexpected packet: {other:?}"),
        }
    }

    #[test]
    fn v3_messages_skip_reserved_bytes() {
        let mut direct = vec![0_u8; 18];
        direct[0] = PacketCode::ContactMsgRecvV3.to_u8();
        direct[1] = (-4_i8).to_le_bytes()[0];
        direct[2..4].copy_from_slice(&[0xaa, 0xbb]);
        direct[4..10].copy_from_slice(&[1, 2, 3, 4, 5, 6]);
        direct[10] = u8::MAX;
        direct[11] = 0;
        direct[12..16].copy_from_slice(&42_u32.to_le_bytes());
        direct[16..].copy_from_slice(b"hi");
        match parsed(&direct) {
            Packet::ContactMsg(message) => {
                assert_eq!(message.text, "hi");
                assert_eq!(message.snr, Some(-1.0));
                assert_eq!(message.route, MessageRoute::Direct);
            }
            other => panic!("unexpected packet: {other:?}"),
        }

        let mut channel = vec![0_u8; 13];
        channel[0] = PacketCode::ChannelMsgRecvV3.to_u8();
        channel[1] = 8;
        channel[2..4].copy_from_slice(&[0xcc, 0xdd]);
        channel[4] = 3;
        channel[5] = (2 << 6) | 1;
        channel[6] = 0;
        channel[7..11].copy_from_slice(&77_u32.to_le_bytes());
        channel[11..].copy_from_slice(b"ok");
        match parsed(&channel) {
            Packet::ChannelMsg(message) => {
                assert_eq!(message.text, "ok");
                assert_eq!(message.snr, Some(2.0));
                assert_eq!(
                    message.route,
                    MessageRoute::Path {
                        hash_mode: 2,
                        hop_count: 1
                    }
                );
            }
            other => panic!("unexpected packet: {other:?}"),
        }
    }

    #[test]
    fn channel_secret_is_redacted_and_short_packet_is_rejected() {
        assert!(Packet::parse(&[PacketCode::ChannelInfo.to_u8(); 19]).is_err());

        let mut raw = vec![0_u8; 50];
        raw[0] = PacketCode::ChannelInfo.to_u8();
        raw[1] = 2;
        raw[2..7].copy_from_slice(b"alpha");
        raw[34..50].fill(7);
        match parsed(&raw) {
            Packet::ChannelInfo(info) => {
                assert_eq!(info.name, "alpha");
                assert_eq!(info.secret(), Some(&[7_u8; 16]));
                assert!(format!("{info:?}").contains("<redacted>"));
                let json = match serde_json::to_string(&info) {
                    Ok(json) => json,
                    Err(error) => panic!("channel info should serialize: {error}"),
                };
                assert!(!json.contains("070707"));
                assert!(!json.contains("secret\":"));
            }
            other => panic!("unexpected packet: {other:?}"),
        }
    }

    #[test]
    fn truncated_storage_packet_is_not_silently_accepted() {
        assert!(matches!(
            Packet::parse(&[PacketCode::Battery.to_u8(), 1, 0, 2]),
            Err(ParseError::InvalidPacketLength { .. })
        ));
        assert!(matches!(
            Packet::parse(&[PacketCode::Battery.to_u8(); 12]),
            Err(ParseError::Malformed { .. })
        ));
    }

    #[test]
    fn parses_contact_uri_tuning_and_strict_custom_variables() {
        match parsed(&[PacketCode::ContactUri.to_u8(), 0xaa, 0xbb]) {
            Packet::ContactUri(uri) => {
                assert_eq!(uri.uri, "meshcore://aabb");
                assert_eq!(uri.card, vec![0xaa, 0xbb]);
            }
            other => panic!("unexpected packet: {other:?}"),
        }
        assert!(Packet::parse(&[PacketCode::ContactUri.to_u8()]).is_err());

        let mut tuning = vec![PacketCode::TuningParams.to_u8()];
        tuning.extend_from_slice(&1_250_u32.to_le_bytes());
        tuning.extend_from_slice(&3_000_u32.to_le_bytes());
        assert_eq!(parsed(&tuning).code(), PacketCode::TuningParams);
        match parsed(&tuning) {
            Packet::TuningParams(params) => {
                assert_eq!(params.rx_delay, 1_250);
                assert_eq!(params.airtime_factor, 3_000);
            }
            other => panic!("unexpected packet: {other:?}"),
        }
        let mut tuning_with_tail = tuning;
        tuning_with_tail.push(0);
        assert!(Packet::parse(&tuning_with_tail).is_err());

        let mut custom = vec![PacketCode::CustomVars.to_u8()];
        custom.extend_from_slice(b"gps:1,interval:60");
        match parsed(&custom) {
            Packet::CustomVariables(vars) => {
                assert_eq!(vars.raw, b"gps:1,interval:60");
                assert_eq!(vars.entries.len(), 2);
                assert_eq!(vars.entries[0].key, "gps");
                assert_eq!(vars.entries[1].value, "60");
            }
            other => panic!("unexpected packet: {other:?}"),
        }
        assert!(matches!(
            parsed(&[PacketCode::CustomVars.to_u8()]),
            Packet::CustomVariables(CustomVariables { entries, .. }) if entries.is_empty()
        ));
        for malformed in [
            b"missing".as_slice(),
            b"key:".as_slice(),
            b":value".as_slice(),
            b"key:one:two".as_slice(),
            b"key:one,key:two".as_slice(),
        ] {
            let mut raw = vec![PacketCode::CustomVars.to_u8()];
            raw.extend_from_slice(malformed);
            assert!(Packet::parse(&raw).is_err(), "accepted {malformed:?}");
        }
        assert!(Packet::parse(&[PacketCode::CustomVars.to_u8(), 0xff]).is_err());
        assert!(Packet::parse(&[PacketCode::CustomVars.to_u8(); 142]).is_err());
    }

    #[test]
    fn parses_advert_paths_and_rejects_descriptor_length_mismatches() {
        let mut raw = vec![PacketCode::AdvertPath.to_u8()];
        raw.extend_from_slice(&123_u32.to_le_bytes());
        raw.push((1 << 6) | 2);
        raw.extend_from_slice(&[0xaa, 0xbb, 0xcc, 0xdd]);
        match parsed(&raw) {
            Packet::AdvertPath(path) => {
                assert_eq!(path.received_at, 123);
                assert_eq!(path.path.as_bytes(), &[0xaa, 0xbb, 0xcc, 0xdd]);
                assert_eq!(
                    path.route,
                    ContactRoute::Path {
                        hash_mode: 1,
                        hop_count: 2,
                    }
                );
            }
            other => panic!("unexpected packet: {other:?}"),
        }

        assert!(Packet::parse(&raw[..raw.len() - 1]).is_err());
        let mut extra = raw;
        extra.push(0);
        assert!(Packet::parse(&extra).is_err());
        let mut flood = vec![PacketCode::AdvertPath.to_u8()];
        flood.extend_from_slice(&7_u32.to_le_bytes());
        flood.push(u8::MAX);
        assert!(matches!(
            parsed(&flood),
            Packet::AdvertPath(AdvertPath {
                route: ContactRoute::Flood,
                ..
            })
        ));
        flood.push(1);
        assert!(Packet::parse(&flood).is_err());
    }

    #[test]
    fn parses_all_stats_layouts_and_only_known_subtypes() {
        let mut core = vec![PacketCode::Stats.to_u8(), 0];
        core.extend_from_slice(&4_200_u16.to_le_bytes());
        core.extend_from_slice(&99_u32.to_le_bytes());
        core.extend_from_slice(&3_u16.to_le_bytes());
        core.push(2);
        assert!(matches!(
            parsed(&core),
            Packet::DeviceStats(DeviceStats::Core {
                battery_mv: 4_200,
                uptime_seconds: 99,
                errors: 3,
                queue_length: 2,
            })
        ));

        let mut radio = vec![PacketCode::Stats.to_u8(), 1];
        radio.extend_from_slice(&(-115_i16).to_le_bytes());
        radio.push((-70_i8).to_le_bytes()[0]);
        radio.push(6);
        radio.extend_from_slice(&10_u32.to_le_bytes());
        radio.extend_from_slice(&20_u32.to_le_bytes());
        match parsed(&radio) {
            Packet::DeviceStats(DeviceStats::Radio {
                noise_floor,
                last_rssi,
                last_snr,
                tx_airtime_seconds,
                rx_airtime_seconds,
            }) => {
                assert_eq!(noise_floor, -115);
                assert_eq!(last_rssi, -70);
                assert!((last_snr - 1.5).abs() < f32::EPSILON);
                assert_eq!(tx_airtime_seconds, 10);
                assert_eq!(rx_airtime_seconds, 20);
            }
            other => panic!("unexpected packet: {other:?}"),
        }

        let mut packets = vec![PacketCode::Stats.to_u8(), 2];
        for value in 1_u32..=6 {
            packets.extend_from_slice(&value.to_le_bytes());
        }
        assert!(matches!(
            parsed(&packets),
            Packet::DeviceStats(DeviceStats::Packets {
                recv: 1,
                sent: 2,
                flood_sent: 3,
                direct_sent: 4,
                flood_recv: 5,
                direct_recv: 6,
                recv_errors: None,
            })
        ));
        packets.extend_from_slice(&7_u32.to_le_bytes());
        assert!(matches!(
            parsed(&packets),
            Packet::DeviceStats(DeviceStats::Packets {
                recv_errors: Some(7),
                ..
            })
        ));

        assert!(Packet::parse(&[PacketCode::Stats.to_u8(), 3]).is_err());
        packets.push(0);
        assert!(Packet::parse(&packets).is_err());
    }

    #[test]
    fn parses_auto_add_repeat_ranges_and_default_scope_strictly() {
        assert!(matches!(
            parsed(&[PacketCode::AutoaddConfig.to_u8(), u8::MAX]),
            Packet::AutoAddConfig(AutoAddConfig {
                config: u8::MAX,
                max_hops: None,
            })
        ));
        assert!(matches!(
            parsed(&[PacketCode::AutoaddConfig.to_u8(), 7, 64]),
            Packet::AutoAddConfig(AutoAddConfig {
                config: 7,
                max_hops: Some(64),
            })
        ));
        assert!(Packet::parse(&[PacketCode::AutoaddConfig.to_u8(), 7, 65]).is_err());
        assert!(Packet::parse(&[PacketCode::AutoaddConfig.to_u8(), 7, 1, 2]).is_err());

        let mut ranges = vec![PacketCode::AllowedRepeatFreq.to_u8()];
        ranges.extend_from_slice(&902_000_u32.to_le_bytes());
        ranges.extend_from_slice(&928_000_u32.to_le_bytes());
        match parsed(&ranges) {
            Packet::AllowedRepeatFrequencies(ranges) => {
                assert_eq!(ranges.len(), 1);
                assert_eq!(ranges[0].lower_khz, 902_000);
                assert_eq!(ranges[0].upper_khz, 928_000);
            }
            other => panic!("unexpected packet: {other:?}"),
        }
        ranges.pop();
        assert!(Packet::parse(&ranges).is_err());
        let mut invalid_range = vec![PacketCode::AllowedRepeatFreq.to_u8()];
        invalid_range.extend_from_slice(&2_u32.to_le_bytes());
        invalid_range.extend_from_slice(&1_u32.to_le_bytes());
        assert!(Packet::parse(&invalid_range).is_err());

        assert!(matches!(
            parsed(&[PacketCode::DefaultFloodScope.to_u8()]),
            Packet::DefaultFloodScope(DefaultFloodScope::Unconfigured)
        ));
        let mut scope = vec![0_u8; 48];
        scope[0] = PacketCode::DefaultFloodScope.to_u8();
        scope[1..6].copy_from_slice(b"local");
        scope[32..].fill(0xa5);
        match parsed(&scope) {
            Packet::DefaultFloodScope(DefaultFloodScope::Configured(scope)) => {
                assert_eq!(scope.name(), Some("local"));
                assert_eq!(scope.key, [0xa5; 16]);
            }
            other => panic!("unexpected packet: {other:?}"),
        }
        scope[1..32].fill(b'x');
        assert!(Packet::parse(&scope).is_err());
        scope.push(0);
        assert!(Packet::parse(&scope).is_err());
    }

    #[test]
    fn ok_value_and_ack_trip_time_are_never_partially_accepted() {
        assert!(matches!(
            parsed(&[PacketCode::Ok.to_u8()]),
            Packet::Ok(None)
        ));
        assert!(matches!(
            parsed(&[PacketCode::Ok.to_u8(), 1, 2, 3, 4]),
            Packet::Ok(Some(value)) if value == u32::from_le_bytes([1, 2, 3, 4])
        ));
        for length in [2, 3, 4, 6] {
            let raw = vec![PacketCode::Ok.to_u8(); length];
            assert!(matches!(
                Packet::parse(&raw),
                Err(ParseError::Malformed { .. })
            ));
        }

        let mut legacy_ack = vec![PacketCode::Ack.to_u8()];
        legacy_ack.extend_from_slice(&[1, 2, 3, 4]);
        assert!(matches!(
            parsed(&legacy_ack),
            Packet::Ack(Ack {
                trip_time_ms: None,
                ..
            })
        ));
        for length in 6..9 {
            let raw = vec![PacketCode::Ack.to_u8(); length];
            assert!(matches!(
                Packet::parse(&raw),
                Err(ParseError::Malformed { .. })
            ));
        }

        assert!(Packet::parse(&[PacketCode::CurrentTime.to_u8(), 1, 2, 3, 4, 5]).is_err());
    }

    #[test]
    fn current_firmware_frame_limit_rejects_byte_177() {
        let raw = vec![0xff; MAX_INNER_PAYLOAD + 1];
        assert!(matches!(
            Packet::parse(&raw),
            Err(ParseError::OversizedPacketPayload {
                actual,
                maximum: MAX_INNER_PAYLOAD,
            }) if actual == MAX_INNER_PAYLOAD + 1
        ));
    }

    #[test]
    fn parsing_known_packets_never_panics_on_truncation() {
        for sample in [contact_packet(), self_info_packet(), device_info_packet()] {
            for len in 0..sample.len() {
                let result = std::panic::catch_unwind(|| Packet::parse(&sample[..len]));
                assert!(result.is_ok(), "parser panicked at prefix length {len}");
            }
        }
    }
}

use meshquill_core::domain::{
    AdvertPath, AutoAddConfig, CommandTracking, ContactUri, CustomVariable, CustomVariables,
    DefaultFloodScope, DeviceStats, FrequencyRange, MessageSource, TuningParams,
};
use meshquill_core::{
    Ack, Contact, ContactRoute, ContactType, DeviceInfo, Event, Message, MessageRoute,
    MessageStatus, SelfInfo, TelemetryResponse, TransportKind,
};
use meshquill_transport::{DiscoveredDevice, TransportTarget};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

/// An immutable contact returned by the companion address book.
#[pyclass(
    name = "Contact",
    frozen,
    get_all,
    skip_from_py_object,
    module = "meshcore_sdk._native"
)]
#[derive(Clone)]
pub(crate) struct PyContact {
    public_key: String,
    contact_type: String,
    flags: u8,
    route: String,
    route_hash_mode: Option<u8>,
    route_hop_count: Option<u8>,
    path: String,
    name: String,
    last_advert: u32,
    latitude: f64,
    longitude: f64,
    last_modified: u32,
}

#[pymethods]
impl PyContact {
    fn __repr__(&self) -> String {
        format!(
            "Contact(name={:?}, public_key={:?}, contact_type={:?})",
            self.name, self.public_key, self.contact_type
        )
    }
}

impl From<Contact> for PyContact {
    fn from(contact: Contact) -> Self {
        let (route, route_hash_mode, route_hop_count) = contact_route(contact.route);
        Self {
            public_key: contact.public_key.to_hex(),
            contact_type: contact_type(contact.contact_type),
            flags: contact.flags,
            route,
            route_hash_mode,
            route_hop_count,
            path: contact.out_path.to_hex(),
            name: contact.adv_name,
            last_advert: contact.last_advert,
            latitude: contact.adv_lat,
            longitude: contact.adv_lon,
            last_modified: contact.lastmod,
        }
    }
}

fn contact_type(value: ContactType) -> String {
    match value {
        ContactType::Chat => "chat".to_owned(),
        ContactType::Repeater => "repeater".to_owned(),
        ContactType::Room => "room".to_owned(),
        ContactType::Sensor => "sensor".to_owned(),
        ContactType::Unknown(raw) => format!("unknown:{raw}"),
    }
}

fn contact_route(value: ContactRoute) -> (String, Option<u8>, Option<u8>) {
    match value {
        ContactRoute::Flood => ("flood".to_owned(), None, None),
        ContactRoute::Path {
            hash_mode,
            hop_count,
        } => ("path".to_owned(), Some(hash_mode), Some(hop_count)),
    }
}

/// An immutable inbound direct or channel message.
#[pyclass(
    name = "Message",
    frozen,
    skip_from_py_object,
    module = "meshcore_sdk._native"
)]
#[derive(Clone)]
pub(crate) struct PyMessage {
    #[pyo3(get)]
    source: String,
    sender: String,
    #[pyo3(get)]
    source_key_prefix: Option<String>,
    #[pyo3(get)]
    channel: Option<u8>,
    #[pyo3(get)]
    route: String,
    #[pyo3(get)]
    route_hash_mode: Option<u8>,
    #[pyo3(get)]
    route_hop_count: Option<u8>,
    #[pyo3(get)]
    text_type: u8,
    #[pyo3(get)]
    sender_timestamp: u32,
    #[pyo3(get)]
    text: String,
    #[pyo3(get)]
    snr: Option<f32>,
    #[pyo3(get)]
    status: String,
    #[pyo3(get)]
    failure_reason: Option<String>,
    #[pyo3(get)]
    suggested_timeout_ms: Option<u32>,
    signature: Option<[u8; 4]>,
}

#[pymethods]
impl PyMessage {
    /// Stable source label used by the public quickstart.
    ///
    /// Direct messages return their hexadecimal public-key prefix. Channel messages return
    /// `"channel:<index>"`; this is a channel label, not a resolved human identity.
    #[getter]
    fn sender(&self) -> &str {
        &self.sender
    }

    /// Return the optional four-byte message signature.
    #[getter]
    fn signature<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyBytes>> {
        self.signature
            .as_ref()
            .map(|signature| PyBytes::new(py, signature))
    }

    fn __repr__(&self) -> String {
        format!(
            "Message(source={:?}, text_type={}, text_len={}, status={:?})",
            self.source,
            self.text_type,
            self.text.len(),
            self.status
        )
    }
}

impl From<Message> for PyMessage {
    fn from(message: Message) -> Self {
        let (source, sender, source_key_prefix, channel) = message_source(message.source);
        let (route, route_hash_mode, route_hop_count) = match message.route {
            MessageRoute::Direct => ("direct".to_owned(), None, None),
            MessageRoute::Path {
                hash_mode,
                hop_count,
            } => ("path".to_owned(), Some(hash_mode), Some(hop_count)),
        };
        let (status, failure_reason, suggested_timeout_ms) = match message.status {
            MessageStatus::Received => ("received".to_owned(), None, None),
            MessageStatus::Queued => ("queued".to_owned(), None, None),
            MessageStatus::Sent {
                suggested_timeout_ms,
            } => ("sent".to_owned(), None, suggested_timeout_ms),
            MessageStatus::Acked => ("acked".to_owned(), None, None),
            MessageStatus::Failed(reason) => ("failed".to_owned(), Some(reason), None),
        };
        Self {
            source,
            sender,
            source_key_prefix,
            channel,
            route,
            route_hash_mode,
            route_hop_count,
            text_type: message.txt_type,
            sender_timestamp: message.sender_timestamp,
            text: message.text,
            snr: message.snr,
            status,
            failure_reason,
            suggested_timeout_ms,
            signature: message.signature,
        }
    }
}

fn message_source(source: MessageSource) -> (String, String, Option<String>, Option<u8>) {
    match source {
        MessageSource::Direct { pubkey_prefix } => (
            "direct".to_owned(),
            pubkey_prefix.clone(),
            Some(pubkey_prefix),
            None,
        ),
        MessageSource::Channel { channel_idx } => (
            "channel".to_owned(),
            format!("channel:{channel_idx}"),
            None,
            Some(channel_idx),
        ),
    }
}

/// An immutable firmware acknowledgement.
#[pyclass(
    name = "Ack",
    frozen,
    skip_from_py_object,
    module = "meshcore_sdk._native"
)]
#[derive(Clone)]
pub(crate) struct PyAck {
    #[pyo3(get)]
    code_hex: String,
    #[pyo3(get)]
    trip_time_ms: Option<u32>,
    code: [u8; 4],
}

#[pymethods]
impl PyAck {
    /// Return the four-byte acknowledgement code.
    #[getter]
    fn code<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.code)
    }

    fn __repr__(&self) -> String {
        format!(
            "Ack(code_hex={:?}, trip_time_ms={:?})",
            self.code_hex, self.trip_time_ms
        )
    }
}

impl From<Ack> for PyAck {
    fn from(ack: Ack) -> Self {
        Self {
            code_hex: hex::encode(ack.code),
            trip_time_ms: ack.trip_time_ms,
            code: ack.code,
        }
    }
}

/// Tracking information returned after firmware accepts a direct send.
#[pyclass(
    name = "SendReceipt",
    frozen,
    skip_from_py_object,
    module = "meshcore_sdk._native"
)]
#[derive(Clone)]
pub(crate) struct PySendReceipt {
    #[pyo3(get)]
    code_hex: String,
    #[pyo3(get)]
    pub(crate) suggested_timeout_ms: u32,
    pub(crate) code: [u8; 4],
}

#[pymethods]
impl PySendReceipt {
    /// Return the expected four-byte acknowledgement code.
    #[getter]
    fn ack_code<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.code)
    }

    /// Return the firmware timeout hint in seconds.
    #[getter]
    fn suggested_timeout(&self) -> f64 {
        f64::from(self.suggested_timeout_ms) / 1_000.0
    }

    fn __repr__(&self) -> String {
        format!(
            "SendReceipt(code_hex={:?}, suggested_timeout_ms={})",
            self.code_hex, self.suggested_timeout_ms
        )
    }
}

impl From<CommandTracking> for PySendReceipt {
    fn from(tracking: CommandTracking) -> Self {
        Self {
            code_hex: hex::encode(tracking.ack_code),
            suggested_timeout_ms: tracking.timeout_ms,
            code: tracking.ack_code,
        }
    }
}

/// Immutable session identity returned by the `APP_START` handshake.
#[pyclass(
    name = "SelfInfo",
    frozen,
    get_all,
    skip_from_py_object,
    module = "meshcore_sdk._native"
)]
#[derive(Clone)]
pub(crate) struct PySelfInfo {
    advertising_type: u8,
    tx_power: u8,
    max_tx_power: u8,
    public_key: String,
    latitude: f64,
    longitude: f64,
    multi_acks: u8,
    advert_location_policy: u8,
    telemetry_mode_environment: u8,
    telemetry_mode_location: u8,
    telemetry_mode_base: u8,
    manual_add_contacts: bool,
    radio_frequency_mhz: f64,
    radio_bandwidth_khz: f64,
    radio_spreading_factor: u8,
    radio_coding_rate: u8,
    name: String,
}

#[pymethods]
impl PySelfInfo {
    fn __repr__(&self) -> String {
        format!(
            "SelfInfo(name={:?}, public_key={:?}, radio_frequency_mhz={})",
            self.name, self.public_key, self.radio_frequency_mhz
        )
    }
}

impl From<SelfInfo> for PySelfInfo {
    fn from(info: SelfInfo) -> Self {
        Self {
            advertising_type: info.advertising_type,
            tx_power: info.tx_power,
            max_tx_power: info.max_tx_power,
            public_key: info.public_key.to_hex(),
            latitude: info.adv_lat,
            longitude: info.adv_lon,
            multi_acks: info.multi_acks,
            advert_location_policy: info.advert_loc_policy,
            telemetry_mode_environment: info.telemetry_mode_env,
            telemetry_mode_location: info.telemetry_mode_loc,
            telemetry_mode_base: info.telemetry_mode_base,
            manual_add_contacts: info.manual_add_contacts,
            radio_frequency_mhz: info.radio_frequency_mhz,
            radio_bandwidth_khz: info.radio_bandwidth_khz,
            radio_spreading_factor: info.radio_spreading_factor,
            radio_coding_rate: info.radio_coding_rate,
            name: info.name,
        }
    }
}

/// Immutable firmware and companion metadata.
#[pyclass(
    name = "DeviceInfo",
    frozen,
    get_all,
    skip_from_py_object,
    module = "meshcore_sdk._native"
)]
#[derive(Clone)]
pub(crate) struct PyDeviceInfo {
    protocol_version: u8,
    max_contacts: Option<u16>,
    max_channels: Option<u8>,
    firmware_build: Option<String>,
    model: Option<String>,
    firmware_version: Option<String>,
    repeat_enabled: Option<bool>,
    path_hash_mode: Option<u8>,
}

#[pymethods]
impl PyDeviceInfo {
    fn __repr__(&self) -> String {
        format!(
            "DeviceInfo(protocol_version={}, model={:?}, firmware_version={:?})",
            self.protocol_version, self.model, self.firmware_version
        )
    }
}

impl From<DeviceInfo> for PyDeviceInfo {
    fn from(info: DeviceInfo) -> Self {
        Self {
            protocol_version: info.protocol_version,
            max_contacts: info.max_contacts,
            max_channels: info.max_channels,
            firmware_build: info.firmware_build,
            model: info.model,
            firmware_version: info.firmware_version,
            repeat_enabled: info.repeat_enabled,
            path_hash_mode: info.path_hash_mode,
        }
    }
}

/// Immutable contact URI response. Its representation does not print URI or card contents.
#[pyclass(
    name = "ContactUri",
    frozen,
    skip_from_py_object,
    module = "meshcore_sdk._native"
)]
#[derive(Clone)]
pub(crate) struct PyContactUri {
    #[pyo3(get)]
    uri: String,
    card: Vec<u8>,
}

#[pymethods]
impl PyContactUri {
    /// Return the optional companion contact-card bytes.
    #[getter]
    fn card<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.card)
    }

    fn __repr__(&self) -> String {
        format!("ContactUri(card_len={})", self.card.len())
    }
}

impl From<ContactUri> for PyContactUri {
    fn from(value: ContactUri) -> Self {
        Self {
            uri: value.uri,
            card: value.card,
        }
    }
}

/// Immutable firmware tuning parameters.
#[pyclass(
    name = "TuningParams",
    frozen,
    get_all,
    skip_from_py_object,
    module = "meshcore_sdk._native"
)]
#[derive(Clone)]
pub(crate) struct PyTuningParams {
    rx_delay: u32,
    airtime_factor: u32,
}

impl From<TuningParams> for PyTuningParams {
    fn from(value: TuningParams) -> Self {
        Self {
            rx_delay: value.rx_delay,
            airtime_factor: value.airtime_factor,
        }
    }
}

/// Immutable custom-variable key/value entry.
#[pyclass(
    name = "CustomVariable",
    frozen,
    get_all,
    skip_from_py_object,
    module = "meshcore_sdk._native"
)]
#[derive(Clone)]
pub(crate) struct PyCustomVariable {
    key: String,
    value: String,
}

impl From<CustomVariable> for PyCustomVariable {
    fn from(value: CustomVariable) -> Self {
        Self {
            key: value.key,
            value: value.value,
        }
    }
}

/// Immutable custom-variable payload. Its representation redacts values and raw bytes.
#[pyclass(
    name = "CustomVariables",
    frozen,
    skip_from_py_object,
    module = "meshcore_sdk._native"
)]
#[derive(Clone)]
pub(crate) struct PyCustomVariables {
    raw: Vec<u8>,
    entries: Vec<CustomVariable>,
}

#[pymethods]
impl PyCustomVariables {
    /// Return the raw companion payload.
    #[getter]
    fn raw<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.raw)
    }

    /// Return parsed key/value entries.
    #[getter]
    fn entries(&self, py: Python<'_>) -> PyResult<Vec<Py<PyCustomVariable>>> {
        self.entries
            .iter()
            .cloned()
            .map(PyCustomVariable::from)
            .map(|entry| Py::new(py, entry))
            .collect()
    }

    fn __repr__(&self) -> String {
        format!(
            "CustomVariables(raw_len={}, entry_count={})",
            self.raw.len(),
            self.entries.len()
        )
    }
}

impl From<CustomVariables> for PyCustomVariables {
    fn from(value: CustomVariables) -> Self {
        Self {
            raw: value.raw,
            entries: value.entries,
        }
    }
}

/// Immutable advertised route-path observation.
#[pyclass(
    name = "AdvertPath",
    frozen,
    get_all,
    skip_from_py_object,
    module = "meshcore_sdk._native"
)]
#[derive(Clone)]
pub(crate) struct PyAdvertPath {
    received_at: u32,
    route: String,
    route_hash_mode: Option<u8>,
    route_hop_count: Option<u8>,
    path: String,
}

impl From<AdvertPath> for PyAdvertPath {
    fn from(value: AdvertPath) -> Self {
        let (route, route_hash_mode, route_hop_count) = contact_route(value.route);
        Self {
            received_at: value.received_at,
            route,
            route_hash_mode,
            route_hop_count,
            path: value.path.to_hex(),
        }
    }
}

/// Immutable device statistics carried by an event.
#[pyclass(
    name = "DeviceStats",
    frozen,
    get_all,
    skip_from_py_object,
    module = "meshcore_sdk._native"
)]
#[derive(Clone)]
pub(crate) struct PyDeviceStats {
    kind: &'static str,
    battery_mv: Option<u16>,
    uptime_seconds: Option<u32>,
    errors: Option<u16>,
    queue_length: Option<u8>,
    noise_floor: Option<i16>,
    last_rssi: Option<i8>,
    last_snr: Option<f32>,
    tx_airtime_seconds: Option<u32>,
    rx_airtime_seconds: Option<u32>,
    received: Option<u32>,
    sent: Option<u32>,
    flood_received: Option<u32>,
    flood_sent: Option<u32>,
    direct_received: Option<u32>,
    direct_sent: Option<u32>,
    receive_errors: Option<u32>,
}

impl From<DeviceStats> for PyDeviceStats {
    fn from(value: DeviceStats) -> Self {
        let mut stats = Self {
            kind: "",
            battery_mv: None,
            uptime_seconds: None,
            errors: None,
            queue_length: None,
            noise_floor: None,
            last_rssi: None,
            last_snr: None,
            tx_airtime_seconds: None,
            rx_airtime_seconds: None,
            received: None,
            sent: None,
            flood_received: None,
            flood_sent: None,
            direct_received: None,
            direct_sent: None,
            receive_errors: None,
        };
        match value {
            DeviceStats::Core {
                battery_mv,
                uptime_seconds,
                errors,
                queue_length,
            } => {
                stats.kind = "core";
                stats.battery_mv = Some(battery_mv);
                stats.uptime_seconds = Some(uptime_seconds);
                stats.errors = Some(errors);
                stats.queue_length = Some(queue_length);
            }
            DeviceStats::Radio {
                noise_floor,
                last_rssi,
                last_snr,
                tx_airtime_seconds,
                rx_airtime_seconds,
            } => {
                stats.kind = "radio";
                stats.noise_floor = Some(noise_floor);
                stats.last_rssi = Some(last_rssi);
                stats.last_snr = Some(last_snr);
                stats.tx_airtime_seconds = Some(tx_airtime_seconds);
                stats.rx_airtime_seconds = Some(rx_airtime_seconds);
            }
            DeviceStats::Packets {
                recv,
                sent,
                flood_recv,
                flood_sent,
                direct_recv,
                direct_sent,
                recv_errors,
            } => {
                stats.kind = "packets";
                stats.received = Some(recv);
                stats.sent = Some(sent);
                stats.flood_received = Some(flood_recv);
                stats.flood_sent = Some(flood_sent);
                stats.direct_received = Some(direct_recv);
                stats.direct_sent = Some(direct_sent);
                stats.receive_errors = recv_errors;
            }
        }
        stats
    }
}

/// Immutable raw telemetry returned by a local sensor request.
#[pyclass(
    name = "TelemetryResponse",
    frozen,
    skip_from_py_object,
    module = "meshcore_sdk._native"
)]
#[derive(Clone)]
pub(crate) struct PyTelemetryResponse {
    #[pyo3(get)]
    source_key_prefix: String,
    payload: Vec<u8>,
}

#[pymethods]
impl PyTelemetryResponse {
    /// Return the bounded Cayenne-LPP-compatible payload bytes.
    #[getter]
    fn payload<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.payload)
    }

    fn __repr__(&self) -> String {
        format!("TelemetryResponse(payload_len={})", self.payload.len())
    }
}

impl From<TelemetryResponse> for PyTelemetryResponse {
    fn from(value: TelemetryResponse) -> Self {
        Self {
            source_key_prefix: hex::encode(value.pubkey_prefix),
            payload: value.payload,
        }
    }
}

/// Immutable auto-add contact configuration.
#[pyclass(
    name = "AutoAddConfig",
    frozen,
    get_all,
    skip_from_py_object,
    module = "meshcore_sdk._native"
)]
#[derive(Clone)]
pub(crate) struct PyAutoAddConfig {
    config: u8,
    max_hops: Option<u8>,
}

impl From<AutoAddConfig> for PyAutoAddConfig {
    fn from(value: AutoAddConfig) -> Self {
        Self {
            config: value.config,
            max_hops: value.max_hops,
        }
    }
}

/// Immutable allowed repeat-frequency range.
#[pyclass(
    name = "FrequencyRange",
    frozen,
    get_all,
    skip_from_py_object,
    module = "meshcore_sdk._native"
)]
#[derive(Clone)]
pub(crate) struct PyFrequencyRange {
    lower_khz: u32,
    upper_khz: u32,
}

impl From<FrequencyRange> for PyFrequencyRange {
    fn from(value: FrequencyRange) -> Self {
        Self {
            lower_khz: value.lower_khz,
            upper_khz: value.upper_khz,
        }
    }
}

/// Immutable default flood-scope state with all scope key material omitted.
#[pyclass(
    name = "DefaultFloodScope",
    frozen,
    get_all,
    skip_from_py_object,
    module = "meshcore_sdk._native"
)]
#[derive(Clone)]
pub(crate) struct PyDefaultFloodScope {
    state: String,
    name: Option<String>,
}

impl From<DefaultFloodScope> for PyDefaultFloodScope {
    fn from(value: DefaultFloodScope) -> Self {
        match value {
            DefaultFloodScope::Unconfigured => Self {
                state: "unconfigured".to_owned(),
                name: None,
            },
            DefaultFloodScope::Configured(scope) => Self {
                state: "configured".to_owned(),
                name: scope.name().map(str::to_owned),
            },
        }
    }
}

/// One immutable event from a client's bounded Rust broadcast stream.
#[pyclass(
    name = "Event",
    frozen,
    skip_from_py_object,
    module = "meshcore_sdk._native"
)]
#[derive(Clone)]
pub(crate) struct PyEvent {
    pub(crate) event: Event,
}

#[pymethods]
impl PyEvent {
    /// Stable snake-case event discriminator.
    #[getter]
    fn kind(&self) -> &'static str {
        event_kind(&self.event)
    }

    /// Message payload for a `message` event.
    #[getter]
    fn message(&self, py: Python<'_>) -> PyResult<Option<Py<PyMessage>>> {
        match &self.event {
            Event::Message(message) => Py::new(py, PyMessage::from(message.clone())).map(Some),
            _ => Ok(None),
        }
    }

    /// ACK payload for an `ack` event.
    #[getter]
    fn ack(&self, py: Python<'_>) -> PyResult<Option<Py<PyAck>>> {
        match &self.event {
            Event::Ack(ack) => Py::new(py, PyAck::from(ack.clone())).map(Some),
            _ => Ok(None),
        }
    }

    /// Session identity for a `self_info` event.
    #[getter]
    fn self_info(&self, py: Python<'_>) -> PyResult<Option<Py<PySelfInfo>>> {
        match &self.event {
            Event::SelfInfo(info) => Py::new(py, PySelfInfo::from(info.clone())).map(Some),
            _ => Ok(None),
        }
    }

    /// Device metadata for a `device_info` event.
    #[getter]
    fn device_info(&self, py: Python<'_>) -> PyResult<Option<Py<PyDeviceInfo>>> {
        match &self.event {
            Event::DeviceInfo(info) => Py::new(py, PyDeviceInfo::from(info.clone())).map(Some),
            _ => Ok(None),
        }
    }

    /// Contacts for a `contacts` event.
    #[getter]
    fn contacts(&self, py: Python<'_>) -> PyResult<Option<Vec<Py<PyContact>>>> {
        match &self.event {
            Event::Contacts { contacts, .. } => contacts
                .iter()
                .cloned()
                .map(PyContact::from)
                .map(|contact| Py::new(py, contact))
                .collect::<PyResult<Vec<_>>>()
                .map(Some),
            _ => Ok(None),
        }
    }

    /// Last-modified marker for a `contacts` event.
    #[getter]
    fn last_modified(&self) -> Option<u32> {
        match &self.event {
            Event::Contacts { lastmod, .. } => Some(*lastmod),
            _ => None,
        }
    }

    /// Channel index for a `channel_info` event.
    #[getter]
    fn channel(&self) -> Option<u8> {
        match &self.event {
            Event::ChannelInfo { idx, .. } => Some(*idx),
            _ => None,
        }
    }

    /// Channel name for a `channel_info` event.
    #[getter]
    fn channel_name(&self) -> Option<String> {
        match &self.event {
            Event::ChannelInfo { name, .. } => Some(name.clone()),
            _ => None,
        }
    }

    /// Redacted channel secret hash for a `channel_info` event.
    #[getter]
    fn channel_secret_hash(&self) -> Option<u8> {
        match &self.event {
            Event::ChannelInfo { secret_hash, .. } => *secret_hash,
            _ => None,
        }
    }

    /// Destination type for a `message_sent` event.
    #[getter]
    fn destination_type(&self) -> Option<u8> {
        match &self.event {
            Event::MessageSent {
                destination_type, ..
            } => Some(*destination_type),
            _ => None,
        }
    }

    /// ACK code for a `message_sent` event.
    #[getter]
    fn ack_code<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyBytes>> {
        match &self.event {
            Event::MessageSent { ack_code, .. } => Some(PyBytes::new(py, ack_code)),
            _ => None,
        }
    }

    /// Firmware timeout hint for a `message_sent` event.
    #[getter]
    fn suggested_timeout_ms(&self) -> Option<u32> {
        match &self.event {
            Event::MessageSent {
                suggested_timeout_ms,
                ..
            } => Some(*suggested_timeout_ms),
            _ => None,
        }
    }

    /// Firmware timestamp for a `current_time` event.
    #[getter]
    fn timestamp(&self) -> Option<u32> {
        match &self.event {
            Event::CurrentTime(timestamp) => Some(*timestamp),
            _ => None,
        }
    }

    /// Battery level for a `battery` event.
    #[getter]
    fn battery_level(&self) -> Option<u16> {
        match &self.event {
            Event::Battery { level, .. } => Some(*level),
            _ => None,
        }
    }

    /// Used storage for a `battery` event.
    #[getter]
    fn storage_used_kb(&self) -> Option<u32> {
        match &self.event {
            Event::Battery { used_kb, .. } => *used_kb,
            _ => None,
        }
    }

    /// Total storage for a `battery` event.
    #[getter]
    fn storage_total_kb(&self) -> Option<u32> {
        match &self.event {
            Event::Battery { total_kb, .. } => *total_kb,
            _ => None,
        }
    }

    /// Safe firmware error text for a `protocol_error` event.
    #[getter]
    fn error(&self) -> Option<String> {
        match &self.event {
            Event::ProtocolError(error) => Some(error.clone()),
            _ => None,
        }
    }

    /// Packet code for an `unknown_packet` event.
    #[getter]
    fn unknown_code(&self) -> Option<u8> {
        match &self.event {
            Event::UnknownPacket { code, .. } => Some(*code),
            _ => None,
        }
    }

    /// Raw payload for an `unknown_packet` event.
    #[getter]
    fn unknown_payload<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyBytes>> {
        match &self.event {
            Event::UnknownPacket { payload, .. } => Some(PyBytes::new(py, payload)),
            _ => None,
        }
    }

    /// Signed quarter-decibel SNR for a `control_data` event.
    #[getter]
    fn control_snr_qdb(&self) -> Option<i8> {
        match &self.event {
            Event::ControlData(data) => Some(data.snr_qdb),
            _ => None,
        }
    }

    /// RSSI in dBm for a `control_data` event.
    #[getter]
    fn control_rssi_dbm(&self) -> Option<i8> {
        match &self.event {
            Event::ControlData(data) => Some(data.rssi),
            _ => None,
        }
    }

    /// Reported route length for a `control_data` event.
    #[getter]
    fn control_path_len(&self) -> Option<u8> {
        match &self.event {
            Event::ControlData(data) => Some(data.path_len),
            _ => None,
        }
    }

    /// Bounded raw control payload for a `control_data` event.
    #[getter]
    fn control_payload<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyBytes>> {
        match &self.event {
            Event::ControlData(data) => Some(PyBytes::new(py, &data.payload)),
            _ => None,
        }
    }

    /// Contact URI payload for a `contact_uri` event.
    #[getter]
    fn contact_uri(&self, py: Python<'_>) -> PyResult<Option<Py<PyContactUri>>> {
        match &self.event {
            Event::ContactUri(value) => Py::new(py, PyContactUri::from(value.clone())).map(Some),
            _ => Ok(None),
        }
    }

    /// Tuning parameters for a `tuning_params` event.
    #[getter]
    fn tuning_params(&self, py: Python<'_>) -> PyResult<Option<Py<PyTuningParams>>> {
        match &self.event {
            Event::TuningParams(value) => Py::new(py, PyTuningParams::from(*value)).map(Some),
            _ => Ok(None),
        }
    }

    /// Custom variables for a `custom_variables` event.
    #[getter]
    fn custom_variables(&self, py: Python<'_>) -> PyResult<Option<Py<PyCustomVariables>>> {
        match &self.event {
            Event::CustomVariables(value) => {
                Py::new(py, PyCustomVariables::from(value.clone())).map(Some)
            }
            _ => Ok(None),
        }
    }

    /// Advertised path for an `advert_path` event.
    #[getter]
    fn advert_path(&self, py: Python<'_>) -> PyResult<Option<Py<PyAdvertPath>>> {
        match &self.event {
            Event::AdvertPath(value) => Py::new(py, PyAdvertPath::from(value.clone())).map(Some),
            _ => Ok(None),
        }
    }

    /// Device statistics for a `device_stats` event.
    #[getter]
    fn device_stats(&self, py: Python<'_>) -> PyResult<Option<Py<PyDeviceStats>>> {
        match &self.event {
            Event::DeviceStats(value) => Py::new(py, PyDeviceStats::from(value.clone())).map(Some),
            _ => Ok(None),
        }
    }

    /// Raw sensor payload for a `telemetry` event.
    #[getter]
    fn telemetry(&self, py: Python<'_>) -> PyResult<Option<Py<PyTelemetryResponse>>> {
        match &self.event {
            Event::Telemetry(value) => {
                Py::new(py, PyTelemetryResponse::from(value.clone())).map(Some)
            }
            _ => Ok(None),
        }
    }

    /// Auto-add configuration for an `auto_add_config` event.
    #[getter]
    fn auto_add_config(&self, py: Python<'_>) -> PyResult<Option<Py<PyAutoAddConfig>>> {
        match &self.event {
            Event::AutoAddConfig(value) => Py::new(py, PyAutoAddConfig::from(*value)).map(Some),
            _ => Ok(None),
        }
    }

    /// Frequency ranges for an `allowed_repeat_frequencies` event.
    #[getter]
    fn allowed_repeat_frequencies(
        &self,
        py: Python<'_>,
    ) -> PyResult<Option<Vec<Py<PyFrequencyRange>>>> {
        match &self.event {
            Event::AllowedRepeatFrequencies(values) => values
                .iter()
                .cloned()
                .map(PyFrequencyRange::from)
                .map(|value| Py::new(py, value))
                .collect::<PyResult<Vec<_>>>()
                .map(Some),
            _ => Ok(None),
        }
    }

    /// Redacted scope state for a `default_flood_scope` event.
    #[getter]
    fn default_flood_scope(&self, py: Python<'_>) -> PyResult<Option<Py<PyDefaultFloodScope>>> {
        match &self.event {
            Event::DefaultFloodScope(value) => {
                Py::new(py, PyDefaultFloodScope::from(value.clone())).map(Some)
            }
            _ => Ok(None),
        }
    }

    fn __repr__(&self) -> String {
        format!("Event(kind={:?})", event_kind(&self.event))
    }
}

fn event_kind(event: &Event) -> &'static str {
    match event {
        Event::Connected => "connected",
        Event::Disconnected => "disconnected",
        Event::Contacts { .. } => "contacts",
        Event::SelfInfo(_) => "self_info",
        Event::DeviceInfo(_) => "device_info",
        Event::Message(_) => "message",
        Event::ChannelInfo { .. } => "channel_info",
        Event::Ack(_) => "ack",
        Event::MessageSent { .. } => "message_sent",
        Event::CurrentTime(_) => "current_time",
        Event::Battery { .. } => "battery",
        Event::ContactUri(_) => "contact_uri",
        Event::TuningParams(_) => "tuning_params",
        Event::CustomVariables(_) => "custom_variables",
        Event::AdvertPath(_) => "advert_path",
        Event::DeviceStats(_) => "device_stats",
        Event::Telemetry(_) => "telemetry",
        Event::AutoAddConfig(_) => "auto_add_config",
        Event::AllowedRepeatFrequencies(_) => "allowed_repeat_frequencies",
        Event::DefaultFloodScope(_) => "default_flood_scope",
        Event::LoginSucceeded(_)
        | Event::LoginFailed { .. }
        | Event::RemoteStatus(_)
        | Event::BinaryResponse(_)
        | Event::PathDiscovery(_)
        | Event::Signature(_) => "unsupported",
        Event::ControlData(_) => "control_data",
        Event::InboxEmpty => "inbox_empty",
        Event::MessagesWaiting => "messages_waiting",
        Event::ProtocolError(_) => "protocol_error",
        Event::UnknownPacket { .. } => "unknown_packet",
    }
}

/// One immutable BLE or serial discovery result.
#[pyclass(
    name = "DiscoveredDevice",
    frozen,
    get_all,
    skip_from_py_object,
    module = "meshcore_sdk._native"
)]
#[derive(Clone)]
pub(crate) struct PyDiscoveredDevice {
    id: String,
    name: String,
    transport: String,
    address: Option<String>,
    port: Option<u16>,
    baud: Option<u32>,
    selector: Option<String>,
    rssi: Option<i16>,
    notes: Vec<String>,
}

#[pymethods]
impl PyDiscoveredDevice {
    fn __repr__(&self) -> String {
        format!(
            "DiscoveredDevice(id={:?}, name={:?}, transport={:?})",
            self.id, self.name, self.transport
        )
    }
}

impl From<DiscoveredDevice> for PyDiscoveredDevice {
    fn from(device: DiscoveredDevice) -> Self {
        let transport = match device.transport {
            TransportKind::Ble => "ble",
            TransportKind::Serial => "serial",
            TransportKind::Tcp => "tcp",
            TransportKind::Scripted => "scripted",
            TransportKind::Unknown => "unknown",
        }
        .to_owned();
        let (baud, selector) = match &device.target {
            TransportTarget::Ble { selector } => (None, Some(selector.clone())),
            TransportTarget::Serial { baud, .. } => (Some(*baud), None),
            TransportTarget::Tcp { .. } => (None, None),
        };
        Self {
            id: device.id,
            name: device.display_name,
            transport,
            address: device.address,
            port: device.port,
            baud,
            selector,
            rssi: device.rssi,
            notes: device.notes,
        }
    }
}

pub(crate) fn add_classes(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyContact>()?;
    module.add_class::<PyMessage>()?;
    module.add_class::<PyAck>()?;
    module.add_class::<PySendReceipt>()?;
    module.add_class::<PySelfInfo>()?;
    module.add_class::<PyDeviceInfo>()?;
    module.add_class::<PyContactUri>()?;
    module.add_class::<PyTuningParams>()?;
    module.add_class::<PyCustomVariable>()?;
    module.add_class::<PyCustomVariables>()?;
    module.add_class::<PyAdvertPath>()?;
    module.add_class::<PyDeviceStats>()?;
    module.add_class::<PyTelemetryResponse>()?;
    module.add_class::<PyAutoAddConfig>()?;
    module.add_class::<PyFrequencyRange>()?;
    module.add_class::<PyDefaultFloodScope>()?;
    module.add_class::<PyEvent>()?;
    module.add_class::<PyDiscoveredDevice>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_sender_projects_direct_prefix_and_channel_index() {
        let (source, sender, prefix, channel) = message_source(MessageSource::Direct {
            pubkey_prefix: "aabbccddeeff".to_owned(),
        });
        assert_eq!(source, "direct");
        assert_eq!(sender, "aabbccddeeff");
        assert_eq!(prefix.as_deref(), Some("aabbccddeeff"));
        assert_eq!(channel, None);

        let (source, sender, prefix, channel) =
            message_source(MessageSource::Channel { channel_idx: 7 });
        assert_eq!(source, "channel");
        assert_eq!(sender, "channel:7");
        assert_eq!(prefix, None);
        assert_eq!(channel, Some(7));
    }

    #[test]
    fn telemetry_response_projects_lowercase_prefix_and_redacts_repr() {
        let response = PyTelemetryResponse::from(TelemetryResponse {
            pubkey_prefix: [0xab, 0xcd, 0xef, 0x01, 0x23, 0x45],
            payload: vec![0xaa, 0xbb, 0xcc],
        });

        assert_eq!(response.source_key_prefix, "abcdef012345");
        assert_eq!(response.payload, [0xaa, 0xbb, 0xcc]);
        assert_eq!(response.__repr__(), "TelemetryResponse(payload_len=3)");
        assert!(!response.__repr__().contains("abcdef012345"));
        assert!(!response.__repr__().contains("aa"));
    }

    #[test]
    fn telemetry_event_has_typed_kind() {
        let event = Event::Telemetry(TelemetryResponse {
            pubkey_prefix: [1, 2, 3, 4, 5, 6],
            payload: vec![7, 8],
        });

        assert_eq!(event_kind(&event), "telemetry");
    }
}

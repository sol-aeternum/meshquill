#![warn(missing_docs, unreachable_pub)]

//! Typed request and response helpers for MeshCore companion remote operations.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::protocol::MAX_INNER_PAYLOAD;

/// Maximum payload size accepted by remote payload parsers.
pub const MAX_REMOTE_PAYLOAD_LEN: usize = MAX_INNER_PAYLOAD;

/// Minimum supported size for a polyline record payload.
const LPP_MIN_POLYLINE_SIZE: usize = 8;

/// Remote request kind sent after the `BINARY_REQ` request header.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum BinaryRequestKind {
    /// Remote status request.
    Status = 1,
    /// Keepalive request.
    KeepAlive = 2,
    /// Sensor telemetry request.
    Telemetry = 3,
    /// Summary (`MMA`) request.
    Summary = 4,
    /// ACL request.
    Acl = 5,
    /// Neighbour listing request.
    Neighbours = 6,
}

impl BinaryRequestKind {
    /// Returns the protocol wire value for this request kind.
    #[must_use]
    pub const fn code(self) -> u8 {
        self as u8
    }

    /// Returns whether this request has a required, fixed-size payload.
    #[must_use]
    pub const fn has_fixed_payload(self) -> bool {
        matches!(self, Self::Acl | Self::Summary)
    }
}

/// Remote request kind sent after the `SEND_ANON_REQ` request header.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum AnonymousRequestKind {
    /// Region listing request.
    Regions = 1,
    /// Owner metadata request.
    Owner = 2,
    /// Basic capability response request.
    Basic = 3,
}

impl AnonymousRequestKind {
    /// Returns the protocol wire value for this request kind.
    #[must_use]
    pub const fn code(self) -> u8 {
        self as u8
    }
}

/// Neighbour sort hint.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum NeighbourOrder {
    /// Newest-first order.
    Newest,
    /// Oldest-first order.
    Oldest,
    /// Strongest-first order.
    Strongest,
    /// Weakest-first order.
    Weakest,
    /// Unknown firmware value.
    Unknown(u8),
}

impl NeighbourOrder {
    /// Returns the wire representation for the order value.
    #[must_use]
    pub const fn code(self) -> u8 {
        match self {
            Self::Newest => 0,
            Self::Oldest => 1,
            Self::Strongest => 2,
            Self::Weakest => 3,
            Self::Unknown(value) => value,
        }
    }

    /// Decodes a wire-order byte.
    #[must_use]
    pub const fn from_code(code: u8) -> Self {
        match code {
            0 => Self::Newest,
            1 => Self::Oldest,
            2 => Self::Strongest,
            3 => Self::Weakest,
            other => Self::Unknown(other),
        }
    }
}

/// Neighbour page request descriptor.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NeighbourQuery {
    /// Request format version.
    pub version: u8,
    /// Maximum number of neighbours to request.
    pub count: u8,
    /// Byte offset into matched neighbour list.
    pub offset: u16,
    /// Requested ordering mode.
    pub order: NeighbourOrder,
    /// Prefix length to match (1..=32 bytes).
    pub prefix_length: u8,
    /// Randomized request nonce.
    pub nonce: u32,
}

impl NeighbourQuery {
    /// Builds a new query descriptor.
    #[must_use]
    pub const fn new(
        count: u8,
        offset: u16,
        order: NeighbourOrder,
        prefix_length: u8,
        nonce: u32,
    ) -> Self {
        Self {
            version: 0,
            count,
            offset,
            order,
            prefix_length,
            nonce,
        }
    }

    /// Serializes the query descriptor.
    ///
    /// # Errors
    ///
    /// Returns [`RemotePayloadError::Malformed`] when the version, count,
    /// ordering, prefix length, or nonce is not valid for request version zero.
    pub fn encode(self) -> Result<Vec<u8>, RemotePayloadError> {
        validate_neighbour_query(self)?;

        let mut payload = Vec::with_capacity(10);
        payload.push(self.version);
        payload.push(self.count);
        payload.extend_from_slice(&self.offset.to_le_bytes());
        payload.push(self.order.code());
        payload.push(self.prefix_length);
        payload.extend_from_slice(&self.nonce.to_le_bytes());
        Ok(payload)
    }
}

/// Convenience payloads for known binary request kinds.
///
/// * `Acl` yields [`acl_request_payload`].
/// * `Summary` requires [`summary_request_payload`].
#[must_use]
pub fn binary_request_payload(kind: BinaryRequestKind) -> Option<Vec<u8>> {
    match kind {
        BinaryRequestKind::Acl => Some(acl_request_payload()),
        BinaryRequestKind::Summary => None,
        _ => Some(Vec::new()),
    }
}

/// Builds the fixed ACL request payload.
#[must_use]
pub fn acl_request_payload() -> Vec<u8> {
    vec![0, 0]
}

/// Builds the summary request payload.
///
/// Format: `start_secs_ago` (u32, LE), `end_secs_ago` (u32, LE), and two padding bytes.
#[must_use]
pub fn summary_request_payload(start_secs_ago: u32, end_secs_ago: u32) -> Vec<u8> {
    let mut payload = Vec::with_capacity(10);
    payload.extend_from_slice(&start_secs_ago.to_le_bytes());
    payload.extend_from_slice(&end_secs_ago.to_le_bytes());
    payload.extend_from_slice(&[0u8, 0]);
    payload
}

/// Feature flags included in basic metadata responses.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemoteFeature {
    /// Feature kind field.
    pub kind: u8,
    /// Whether feature is disabled.
    pub disabled: bool,
}

impl RemoteFeature {
    /// Decodes one feature byte.
    #[must_use]
    pub const fn from_feature_byte(raw: u8) -> Self {
        Self {
            kind: raw & 0x7f,
            disabled: (raw & 0x80) != 0,
        }
    }

    /// Encodes one feature byte.
    #[must_use]
    pub const fn to_feature_byte(self) -> u8 {
        self.kind | if self.disabled { 0x80 } else { 0 }
    }
}

/// Parsed `basic` anonymous response.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BasicResponse {
    /// Remote time in seconds.
    pub clock: u32,
    /// Feature summary.
    pub feature: RemoteFeature,
}

/// Prefix entry inside ACL responses.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AclEntry {
    /// Six-byte public-key prefix.
    pub pubkey_prefix: [u8; 6],
    /// Permission mask.
    pub permissions: u8,
}

/// Parsed ACL response.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AclResponse {
    /// ACL entries.
    pub entries: Vec<AclEntry>,
}

/// Parsed neighbour entry.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NeighbourEntry {
    /// Peer key prefix, using the length requested by the caller.
    pub pubkey_prefix: Vec<u8>,
    /// Seconds since last seen.
    pub secs_ago: u32,
    /// Link quality in dB.
    pub snr_db: f64,
}

/// Parsed neighbour page.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NeighbourPage {
    /// Total matches reported by firmware.
    pub total_count: u16,
    /// Number of records carried in this payload.
    pub result_count: u16,
    /// Entries returned in this response.
    pub entries: Vec<NeighbourEntry>,
}

/// Parsed anonymous regions response.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RegionsResponse {
    /// Remote clock in seconds.
    pub clock: u32,
    /// Region labels.
    pub names: Vec<String>,
}

/// Parsed anonymous owner response.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OwnerResponse {
    /// Remote clock in seconds.
    pub clock: u32,
    /// Name field.
    pub name: String,
    /// Owner field.
    pub owner: String,
}

/// Cayenne-LPP-like telemetry kind used by remote payloads.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum LppTelemetryKind {
    /// Digital input.
    DigitalInput = 0,
    /// Digital output.
    DigitalOutput = 1,
    /// Analog input.
    AnalogInput = 2,
    /// Analog output.
    AnalogOutput = 3,
    /// Generic sensor.
    GenericSensor = 100,
    /// Luminosity.
    Luminosity = 101,
    /// Presence.
    Presence = 102,
    /// Temperature.
    Temperature = 103,
    /// Relative humidity.
    RelativeHumidity = 104,
    /// Accelerometer.
    Accelerometer = 113,
    /// Barometric pressure.
    BarometricPressure = 115,
    /// Voltage.
    Voltage = 116,
    /// Current.
    Current = 117,
    /// Frequency.
    Frequency = 118,
    /// Percentage.
    Percentage = 120,
    /// Altitude.
    Altitude = 121,
    /// Concentration.
    Concentration = 125,
    /// Power.
    Power = 128,
    /// Distance.
    Distance = 130,
    /// Energy.
    Energy = 131,
    /// Direction.
    Direction = 132,
    /// Unix time.
    UnixTime = 133,
    /// Gyrometer.
    Gyrometer = 134,
    /// Colour.
    Colour = 135,
    /// GPS.
    Gps = 136,
    /// Switch.
    Switch = 142,
    /// Polyline.
    Polyline = 240,
}

impl LppTelemetryKind {
    const fn from_code(code: u8) -> Option<Self> {
        Some(match code {
            0 => Self::DigitalInput,
            1 => Self::DigitalOutput,
            2 => Self::AnalogInput,
            3 => Self::AnalogOutput,
            100 => Self::GenericSensor,
            101 => Self::Luminosity,
            102 => Self::Presence,
            103 => Self::Temperature,
            104 => Self::RelativeHumidity,
            113 => Self::Accelerometer,
            115 => Self::BarometricPressure,
            116 => Self::Voltage,
            117 => Self::Current,
            118 => Self::Frequency,
            120 => Self::Percentage,
            121 => Self::Altitude,
            125 => Self::Concentration,
            128 => Self::Power,
            130 => Self::Distance,
            131 => Self::Energy,
            132 => Self::Direction,
            133 => Self::UnixTime,
            134 => Self::Gyrometer,
            135 => Self::Colour,
            136 => Self::Gps,
            142 => Self::Switch,
            240 => Self::Polyline,
            _ => return None,
        })
    }

    const fn data_size_and_count(self) -> (usize, usize) {
        match self {
            Self::DigitalInput
            | Self::DigitalOutput
            | Self::Presence
            | Self::RelativeHumidity
            | Self::Percentage
            | Self::Switch => (1, 1),
            Self::AnalogInput
            | Self::AnalogOutput
            | Self::Luminosity
            | Self::Temperature
            | Self::BarometricPressure
            | Self::Voltage
            | Self::Current
            | Self::Altitude
            | Self::Concentration
            | Self::Power
            | Self::Direction => (2, 1),
            Self::GenericSensor
            | Self::Frequency
            | Self::Distance
            | Self::Energy
            | Self::UnixTime => (4, 1),
            Self::Accelerometer | Self::Gyrometer => (2, 3),
            Self::Gps => (3, 3),
            Self::Colour => (1, 3),
            Self::Polyline => (0, 0),
        }
    }

    const fn is_signed(self) -> bool {
        matches!(
            self,
            Self::AnalogInput
                | Self::AnalogOutput
                | Self::Temperature
                | Self::Current
                | Self::Altitude
                | Self::Accelerometer
                | Self::Gyrometer
                | Self::Gps
        )
    }

    const fn multiplier(self, component: usize) -> f64 {
        match self {
            Self::Gps if component < 2 => 10_000.0,
            Self::AnalogInput
            | Self::AnalogOutput
            | Self::Voltage
            | Self::Gyrometer
            | Self::Gps => 100.0,
            Self::Temperature | Self::BarometricPressure => 10.0,
            Self::RelativeHumidity => 2.0,
            Self::Accelerometer | Self::Current | Self::Distance | Self::Energy => 1000.0,
            Self::DigitalInput
            | Self::DigitalOutput
            | Self::GenericSensor
            | Self::Luminosity
            | Self::Presence
            | Self::Frequency
            | Self::Percentage
            | Self::Altitude
            | Self::Concentration
            | Self::Power
            | Self::Direction
            | Self::UnixTime
            | Self::Colour
            | Self::Switch
            | Self::Polyline => 1.0,
        }
    }

    const fn is_vector(self) -> bool {
        matches!(
            self,
            Self::Accelerometer | Self::Gyrometer | Self::Gps | Self::Colour | Self::Polyline
        )
    }

    const fn summary_width(self) -> Option<usize> {
        match self {
            Self::DigitalInput
            | Self::DigitalOutput
            | Self::Presence
            | Self::Percentage
            | Self::Switch => Some(1),
            Self::AnalogInput
            | Self::AnalogOutput
            | Self::Luminosity
            | Self::Temperature
            | Self::RelativeHumidity
            | Self::BarometricPressure
            | Self::Voltage
            | Self::Current
            | Self::Altitude
            | Self::Concentration
            | Self::Power
            | Self::Direction => Some(2),
            Self::GenericSensor
            | Self::Frequency
            | Self::Distance
            | Self::Energy
            | Self::UnixTime => Some(4),
            Self::Accelerometer | Self::Gyrometer | Self::Colour | Self::Gps | Self::Polyline => {
                None
            }
        }
    }

    const fn summary_is_signed(self) -> bool {
        matches!(
            self,
            Self::AnalogInput | Self::AnalogOutput | Self::Temperature | Self::Altitude
        )
    }

    const fn summary_multiplier(self) -> f64 {
        match self {
            Self::Current | Self::Distance | Self::Energy => 1000.0,
            Self::Voltage | Self::AnalogInput | Self::AnalogOutput => 100.0,
            Self::Temperature | Self::BarometricPressure | Self::RelativeHumidity => 10.0,
            _ => 1.0,
        }
    }

    /// Returns the wire value.
    #[must_use]
    pub const fn code(self) -> u8 {
        self as u8
    }
}

/// Single telemetry sample inside a telemetry response.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TelemetrySample {
    /// Channel index.
    pub channel: u8,
    /// Reading type.
    pub kind: LppTelemetryKind,
    /// Sample values.
    pub values: Vec<f64>,
    /// Lossless dynamic record bytes for types such as polyline.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw: Option<Vec<u8>>,
}

/// Single summary record inside an `MMA` payload.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SummaryRecord {
    /// Channel index.
    pub channel: u8,
    /// Reading type.
    pub kind: LppTelemetryKind,
    /// Minimum value.
    pub minimum: f64,
    /// Maximum value.
    pub maximum: f64,
    /// Average value.
    pub average: f64,
}

/// Parsed summary response payload.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SummaryResponse {
    /// Remote clock in seconds.
    pub clock: u32,
    /// Per-channel summary entries.
    pub entries: Vec<SummaryRecord>,
}

/// Errors raised while parsing or encoding remote payloads.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RemotePayloadError {
    /// Payload exceeded companion maximum length.
    #[error("payload exceeds maximum size: {actual} > {maximum}")]
    PayloadTooLarge {
        /// Actual payload size in bytes.
        actual: usize,
        /// Maximum accepted payload size in bytes.
        maximum: usize,
    },
    /// Input was shorter than required by a field.
    #[error("payload truncated while parsing {context}")]
    Truncated {
        /// Field or record being decoded.
        context: &'static str,
    },
    /// A non-zero-specified telemetry type appeared.
    #[error("unknown telemetry type: {0}")]
    UnknownType(u8),
    /// UTF-8 decoding failed.
    #[error("payload contains invalid UTF-8 in {context}")]
    InvalidUtf8 {
        /// Text field being decoded.
        context: &'static str,
    },
    /// Field-level parse error.
    #[error("malformed payload: {reason}")]
    Malformed {
        /// Stable explanation of the rejected wire value.
        reason: &'static str,
    },
    /// Numeric value converted to a non-finite value.
    #[error("non-finite numeric value in {context}")]
    NonFinite {
        /// Numeric field being decoded.
        context: &'static str,
    },
}

fn check_max_len(payload: &[u8]) -> Result<(), RemotePayloadError> {
    if payload.len() > MAX_REMOTE_PAYLOAD_LEN {
        return Err(RemotePayloadError::PayloadTooLarge {
            actual: payload.len(),
            maximum: MAX_REMOTE_PAYLOAD_LEN,
        });
    }
    Ok(())
}

fn validate_neighbour_query(query: NeighbourQuery) -> Result<(), RemotePayloadError> {
    if query.version != 0 {
        return Err(RemotePayloadError::Malformed {
            reason: "unsupported neighbour request version",
        });
    }

    if query.count == 0 {
        return Err(RemotePayloadError::Malformed {
            reason: "count must be greater than zero",
        });
    }

    if !(1..=32).contains(&query.prefix_length) {
        return Err(RemotePayloadError::Malformed {
            reason: "prefix length must be between 1 and 32",
        });
    }

    if matches!(query.order, NeighbourOrder::Unknown(_)) {
        return Err(RemotePayloadError::Malformed {
            reason: "neighbour order must be between 0 and 3",
        });
    }

    if query.nonce == 0 {
        return Err(RemotePayloadError::Malformed {
            reason: "neighbour nonce must be non-zero",
        });
    }

    Ok(())
}

fn trim_trailing_nul<'a>(
    raw: &'a [u8],
    context: &'static str,
) -> Result<&'a [u8], RemotePayloadError> {
    if let Some((first_zero, _)) = raw.iter().enumerate().find(|&(_, byte)| *byte == 0) {
        if raw[first_zero + 1..].iter().any(|byte| *byte != 0) {
            return Err(RemotePayloadError::Malformed { reason: context });
        }
        Ok(&raw[..first_zero])
    } else {
        Ok(raw)
    }
}

fn decode_string(raw: &[u8], context: &'static str) -> Result<String, RemotePayloadError> {
    std::str::from_utf8(raw)
        .map(str::to_owned)
        .map_err(|_| RemotePayloadError::InvalidUtf8 { context })
}

fn read_i16_le(raw: &[u8], idx: usize) -> Result<i16, RemotePayloadError> {
    raw.get(idx..idx + 2)
        .map(|slice| i16::from_le_bytes([slice[0], slice[1]]))
        .ok_or(RemotePayloadError::Truncated {
            context: "fixed field",
        })
}

fn read_u32_le(raw: &[u8], idx: usize) -> Result<u32, RemotePayloadError> {
    raw.get(idx..idx + 4)
        .map(|slice| u32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]]))
        .ok_or(RemotePayloadError::Truncated {
            context: "fixed field",
        })
}

fn read_u16_be(raw: &[u8], idx: usize) -> Result<u16, RemotePayloadError> {
    raw.get(idx..idx + 2)
        .map(|slice| u16::from_be_bytes([slice[0], slice[1]]))
        .ok_or(RemotePayloadError::Truncated {
            context: "value field",
        })
}

fn read_i16_be(raw: &[u8], idx: usize) -> Result<i16, RemotePayloadError> {
    raw.get(idx..idx + 2)
        .map(|slice| i16::from_be_bytes([slice[0], slice[1]]))
        .ok_or(RemotePayloadError::Truncated {
            context: "value field",
        })
}

fn read_u24_be(raw: &[u8], idx: usize) -> Result<u32, RemotePayloadError> {
    raw.get(idx..idx + 3)
        .map(|slice| (u32::from(slice[0]) << 16) | (u32::from(slice[1]) << 8) | u32::from(slice[2]))
        .ok_or(RemotePayloadError::Truncated {
            context: "value field",
        })
}

fn read_i24_be(raw: &[u8], idx: usize) -> Result<i32, RemotePayloadError> {
    let value = read_u24_be(raw, idx)?.cast_signed();
    if (value & 0x80_0000) != 0 {
        Ok(value | !0xFF_FFFF)
    } else {
        Ok(value)
    }
}

fn read_u32_be(raw: &[u8], idx: usize) -> Result<u32, RemotePayloadError> {
    raw.get(idx..idx + 4)
        .map(|slice| u32::from_be_bytes([slice[0], slice[1], slice[2], slice[3]]))
        .ok_or(RemotePayloadError::Truncated {
            context: "value field",
        })
}

fn read_i32_be(raw: &[u8], idx: usize) -> Result<i32, RemotePayloadError> {
    raw.get(idx..idx + 4)
        .map(|slice| i32::from_be_bytes([slice[0], slice[1], slice[2], slice[3]]))
        .ok_or(RemotePayloadError::Truncated {
            context: "value field",
        })
}

fn decode_value(
    raw: &[u8],
    kind: LppTelemetryKind,
    cursor: &mut usize,
    width: usize,
    component: usize,
) -> Result<f64, RemotePayloadError> {
    let raw_value = match width {
        1 => {
            let byte = raw
                .get(*cursor)
                .copied()
                .ok_or(RemotePayloadError::Truncated {
                    context: "telemetry value",
                })?;
            *cursor += 1;
            if kind.is_signed() {
                f64::from(i8::from_ne_bytes([byte]))
            } else {
                f64::from(byte)
            }
        }
        2 => {
            let value = if kind.is_signed() {
                f64::from(read_i16_be(raw, *cursor)?)
            } else {
                f64::from(read_u16_be(raw, *cursor)?)
            };
            *cursor = cursor.saturating_add(2);
            value
        }
        3 => {
            let value = if kind.is_signed() {
                f64::from(read_i24_be(raw, *cursor)?)
            } else {
                f64::from(read_u24_be(raw, *cursor)?)
            };
            *cursor = cursor.saturating_add(3);
            value
        }
        4 => {
            let value = if kind.is_signed() {
                f64::from(read_i32_be(raw, *cursor)?)
            } else {
                f64::from(read_u32_be(raw, *cursor)?)
            };
            *cursor = cursor.saturating_add(4);
            value
        }
        _ => {
            return Err(RemotePayloadError::Malformed {
                reason: "unsupported telemetry width",
            });
        }
    };

    let scaled = raw_value / kind.multiplier(component);
    if !scaled.is_finite() {
        return Err(RemotePayloadError::NonFinite {
            context: if kind.is_vector() {
                "vector component"
            } else {
                "scalar value"
            },
        });
    }

    Ok(scaled)
}

fn decode_values(
    payload: &[u8],
    cursor: &mut usize,
    kind: LppTelemetryKind,
    count: usize,
    context: &'static str,
) -> Result<Vec<f64>, RemotePayloadError> {
    let (width, _) = kind.data_size_and_count();
    let max_bytes = width
        .checked_mul(count)
        .ok_or(RemotePayloadError::Malformed {
            reason: "telemetry count overflow",
        })?;
    let end = cursor
        .checked_add(max_bytes)
        .ok_or(RemotePayloadError::Malformed {
            reason: "telemetry cursor overflow",
        })?;
    if end > payload.len() {
        return Err(RemotePayloadError::Truncated {
            context: "telemetry values",
        });
    }

    let mut values = Vec::with_capacity(count);
    let mut component = 0usize;
    while *cursor < end {
        values.push(decode_value(payload, kind, cursor, width, component)?);
        component = component.saturating_add(1);
    }

    if values.len() != count {
        return Err(RemotePayloadError::Truncated { context });
    }

    Ok(values)
}

fn decode_summary_value(
    payload: &[u8],
    cursor: &mut usize,
    kind: LppTelemetryKind,
) -> Result<f64, RemotePayloadError> {
    let width = kind.summary_width().ok_or(RemotePayloadError::Malformed {
        reason: "vector and dynamic telemetry types are not valid MMA records",
    })?;
    let end = cursor
        .checked_add(width)
        .ok_or(RemotePayloadError::Malformed {
            reason: "summary cursor overflow",
        })?;
    if end > payload.len() {
        return Err(RemotePayloadError::Truncated {
            context: "summary value",
        });
    }

    let raw = match (width, kind.summary_is_signed()) {
        (1, true) => f64::from(i8::from_ne_bytes([payload[*cursor]])),
        (1, false) => f64::from(payload[*cursor]),
        (2, true) => f64::from(read_i16_be(payload, *cursor)?),
        (2, false) => f64::from(read_u16_be(payload, *cursor)?),
        (4, true) => f64::from(read_i32_be(payload, *cursor)?),
        (4, false) => f64::from(read_u32_be(payload, *cursor)?),
        _ => {
            return Err(RemotePayloadError::Malformed {
                reason: "unsupported MMA value width",
            });
        }
    };
    *cursor = end;
    Ok(raw / kind.summary_multiplier())
}

/// Parses neighbour page payloads from binary responses.
///
/// The response does not repeat the requested key-prefix length, so callers must
/// supply the same value used in [`NeighbourQuery`].
///
/// # Errors
///
/// Returns an error for invalid prefix lengths, truncated input, negative or
/// inconsistent counts, trailing bytes, or payloads over the protocol limit.
pub fn parse_neighbour_page(
    payload: &[u8],
    prefix_length: u8,
) -> Result<NeighbourPage, RemotePayloadError> {
    check_max_len(payload)?;

    if !(1..=32).contains(&prefix_length) {
        return Err(RemotePayloadError::Malformed {
            reason: "prefix length must be between 1 and 32",
        });
    }

    if payload.len() < 4 {
        return Err(RemotePayloadError::Truncated {
            context: "neighbour header",
        });
    }

    let total_count = read_i16_le(payload, 0)?;
    let result_count = read_i16_le(payload, 2)?;
    if total_count < 0 || result_count < 0 {
        return Err(RemotePayloadError::Malformed {
            reason: "neighbour counts must not be negative",
        });
    }
    let total_count = u16::try_from(total_count).map_err(|_| RemotePayloadError::Malformed {
        reason: "neighbour total count is invalid",
    })?;
    let result_count = u16::try_from(result_count).map_err(|_| RemotePayloadError::Malformed {
        reason: "neighbour result count is invalid",
    })?;
    if result_count > total_count {
        return Err(RemotePayloadError::Malformed {
            reason: "neighbour result count exceeds total count",
        });
    }
    let entry_count = usize::from(result_count);
    let entry_size = usize::from(prefix_length).saturating_add(5);
    let expected = 4usize.saturating_add(entry_count.saturating_mul(entry_size));
    if expected != payload.len() {
        return Err(RemotePayloadError::Malformed {
            reason: "neighbour page length mismatch",
        });
    }

    let mut entries = Vec::with_capacity(entry_count);
    let mut cursor = 4usize;
    for _ in 0..entry_count {
        let prefix_end = cursor.saturating_add(usize::from(prefix_length));
        let pubkey_prefix = payload[cursor..prefix_end].to_vec();
        let secs_ago = read_u32_le(payload, prefix_end)?;
        let snr_raw = payload[prefix_end + 4];
        let snr_db = f64::from(i8::from_ne_bytes([snr_raw])) / 4.0;
        cursor = cursor.saturating_add(entry_size);

        entries.push(NeighbourEntry {
            pubkey_prefix,
            secs_ago,
            snr_db,
        });
    }

    Ok(NeighbourPage {
        total_count,
        result_count,
        entries,
    })
}

/// Parses region metadata responses.
///
/// # Errors
///
/// Returns an error for oversized, truncated, non-UTF-8, empty, or malformed
/// comma-separated payloads.
pub fn parse_regions_response(payload: &[u8]) -> Result<RegionsResponse, RemotePayloadError> {
    check_max_len(payload)?;
    if payload.len() < 4 {
        return Err(RemotePayloadError::Truncated {
            context: "regions header",
        });
    }

    let clock = read_u32_le(payload, 0)?;
    let text = trim_trailing_nul(&payload[4..], "regions")?;
    if text.is_empty() {
        return Err(RemotePayloadError::Malformed {
            reason: "at least one region entry is required",
        });
    }

    let value = decode_string(text, "regions")?;
    let names: Vec<String> = value.split(',').map(str::to_owned).collect();
    if names.is_empty() || names.iter().any(String::is_empty) {
        return Err(RemotePayloadError::Malformed {
            reason: "at least one region entry is required",
        });
    }

    Ok(RegionsResponse { clock, names })
}

/// Parses owner metadata responses.
///
/// # Errors
///
/// Returns an error for oversized, truncated, non-UTF-8, or malformed
/// name-and-owner payloads.
pub fn parse_owner_response(payload: &[u8]) -> Result<OwnerResponse, RemotePayloadError> {
    check_max_len(payload)?;
    if payload.len() < 4 {
        return Err(RemotePayloadError::Truncated {
            context: "owner header",
        });
    }

    let clock = read_u32_le(payload, 0)?;
    let body = trim_trailing_nul(&payload[4..], "owner")?;
    let text = decode_string(body, "owner")?;
    let mut parts = text.splitn(3, '\n');
    let name =
        parts
            .next()
            .filter(|name| !name.is_empty())
            .ok_or(RemotePayloadError::Malformed {
                reason: "owner name",
            })?;
    let owner =
        parts
            .next()
            .filter(|owner| !owner.is_empty())
            .ok_or(RemotePayloadError::Malformed {
                reason: "owner identity",
            })?;

    if parts.next().is_some() {
        return Err(RemotePayloadError::Malformed {
            reason: "owner contains too many fields",
        });
    }

    Ok(OwnerResponse {
        clock,
        name: name.to_owned(),
        owner: owner.to_owned(),
    })
}

/// Parses basic metadata responses.
///
/// # Errors
///
/// Returns an error unless the payload is exactly a four-byte clock followed by
/// one feature byte and is within the protocol limit.
pub fn parse_basic_response(payload: &[u8]) -> Result<BasicResponse, RemotePayloadError> {
    check_max_len(payload)?;
    if payload.len() != 5 {
        return Err(RemotePayloadError::Malformed {
            reason: "basic payload must be exactly five bytes",
        });
    }

    let clock = read_u32_le(payload, 0)?;
    Ok(BasicResponse {
        clock,
        feature: RemoteFeature::from_feature_byte(payload[4]),
    })
}

/// Parses ACL payloads.
///
/// # Errors
///
/// Returns an error when the payload is oversized or is not a complete sequence
/// of six-byte prefixes and one-byte permission masks.
pub fn parse_acl_payload(payload: &[u8]) -> Result<AclResponse, RemotePayloadError> {
    check_max_len(payload)?;

    if !payload.len().is_multiple_of(7) {
        return Err(RemotePayloadError::Malformed {
            reason: "acl payload length must be a multiple of 7",
        });
    }

    let mut entries = Vec::with_capacity(payload.len() / 7);
    for chunk in payload.chunks_exact(7) {
        let mut pubkey_prefix = [0_u8; 6];
        pubkey_prefix.copy_from_slice(&chunk[..6]);
        if pubkey_prefix == [0_u8; 6] {
            // Older firmware can leave tombstone slots in fixed ACL buffers.
            continue;
        }
        entries.push(AclEntry {
            pubkey_prefix,
            permissions: chunk[6],
        });
    }

    Ok(AclResponse { entries })
}

/// Parses Cayenne-LPP telemetry payloads.
///
/// # Errors
///
/// Returns an error for oversized, truncated, unknown, or internally
/// inconsistent telemetry records.
pub fn parse_telemetry_payload(payload: &[u8]) -> Result<Vec<TelemetrySample>, RemotePayloadError> {
    check_max_len(payload)?;

    if payload.is_empty() {
        return Ok(Vec::new());
    }

    let mut cursor = 0usize;
    let mut readings = Vec::new();

    while cursor < payload.len() {
        let channel = *payload.get(cursor).ok_or(RemotePayloadError::Truncated {
            context: "telemetry channel",
        })?;
        cursor = cursor.saturating_add(1);

        if channel == 0 {
            if !payload[cursor..].iter().all(|byte| *byte == 0) {
                return Err(RemotePayloadError::Malformed {
                    reason: "channel zero must be followed only by zero padding",
                });
            }
            break;
        }

        let kind_code = *payload.get(cursor).ok_or(RemotePayloadError::Truncated {
            context: "telemetry type",
        })?;
        cursor = cursor.saturating_add(1);

        let kind = LppTelemetryKind::from_code(kind_code)
            .ok_or(RemotePayloadError::UnknownType(kind_code))?;

        if kind == LppTelemetryKind::Polyline {
            let record_start = cursor;
            let record_size =
                usize::from(*payload.get(cursor).ok_or(RemotePayloadError::Truncated {
                    context: "polyline size",
                })?);

            if record_size < LPP_MIN_POLYLINE_SIZE {
                return Err(RemotePayloadError::Malformed {
                    reason: "polyline record size below minimum",
                });
            }
            let end =
                record_start
                    .checked_add(record_size)
                    .ok_or(RemotePayloadError::Malformed {
                        reason: "polyline length overflow",
                    })?;
            if end > payload.len() {
                return Err(RemotePayloadError::Truncated {
                    context: "polyline payload",
                });
            }

            let raw = payload[record_start..end].to_vec();
            cursor = end;

            readings.push(TelemetrySample {
                channel,
                kind,
                values: Vec::new(),
                raw: Some(raw),
            });
            continue;
        }

        let (_size, count) = kind.data_size_and_count();
        let values = decode_values(payload, &mut cursor, kind, count, "telemetry")?;
        readings.push(TelemetrySample {
            channel,
            kind,
            values,
            raw: None,
        });
    }

    Ok(readings)
}

/// Parses MMA summary payloads.
///
/// # Errors
///
/// Returns an error for oversized, truncated, unknown, dynamic, vector, or
/// internally inconsistent summary records.
pub fn parse_summary_payload(payload: &[u8]) -> Result<SummaryResponse, RemotePayloadError> {
    check_max_len(payload)?;
    if payload.len() < 4 {
        return Err(RemotePayloadError::Truncated {
            context: "summary clock",
        });
    }

    let clock = read_u32_le(payload, 0)?;
    let mut cursor = 4usize;
    let mut entries = Vec::new();

    while cursor < payload.len() {
        let channel = *payload.get(cursor).ok_or(RemotePayloadError::Truncated {
            context: "summary channel",
        })?;
        cursor = cursor.saturating_add(1);

        if channel == 0 {
            if !payload[cursor..].iter().all(|byte| *byte == 0) {
                return Err(RemotePayloadError::Malformed {
                    reason: "summary terminator must be followed only by zero padding",
                });
            }
            break;
        }

        let kind_code = *payload.get(cursor).ok_or(RemotePayloadError::Truncated {
            context: "summary kind",
        })?;
        cursor = cursor.saturating_add(1);
        let kind = LppTelemetryKind::from_code(kind_code)
            .ok_or(RemotePayloadError::UnknownType(kind_code))?;

        let minimum = decode_summary_value(payload, &mut cursor, kind)?;
        let maximum = decode_summary_value(payload, &mut cursor, kind)?;
        let average = decode_summary_value(payload, &mut cursor, kind)?;

        entries.push(SummaryRecord {
            channel,
            kind,
            minimum,
            maximum,
            average,
        });
    }

    Ok(SummaryResponse { clock, entries })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_kind_codes_match_protocol() {
        assert_eq!(BinaryRequestKind::Status.code(), 1);
        assert_eq!(BinaryRequestKind::KeepAlive.code(), 2);
        assert_eq!(BinaryRequestKind::Telemetry.code(), 3);
        assert_eq!(BinaryRequestKind::Summary.code(), 4);
        assert_eq!(BinaryRequestKind::Acl.code(), 5);
        assert_eq!(BinaryRequestKind::Neighbours.code(), 6);
        assert_eq!(AnonymousRequestKind::Regions.code(), 1);
        assert_eq!(AnonymousRequestKind::Owner.code(), 2);
        assert_eq!(AnonymousRequestKind::Basic.code(), 3);
    }

    #[test]
    fn summary_payload_is_encoded_with_sentinel_bytes() {
        assert_eq!(
            summary_request_payload(1, 9),
            vec![1, 0, 0, 0, 9, 0, 0, 0, 0, 0]
        );
        assert_eq!(acl_request_payload(), vec![0, 0]);
    }

    #[test]
    fn neighbour_query_validation_and_encoding() {
        let query = NeighbourQuery::new(3, 2, NeighbourOrder::Newest, 8, 0x1234_5678);
        let bytes = query.encode().expect("query should encode");
        assert_eq!(bytes, vec![0, 3, 2, 0, 0, 8, 0x78, 0x56, 0x34, 0x12]);

        assert!(
            NeighbourQuery::new(0, 0, NeighbourOrder::Newest, 1, 1)
                .encode()
                .is_err()
        );
        assert!(
            NeighbourQuery::new(1, 0, NeighbourOrder::Newest, 0, 1)
                .encode()
                .is_err()
        );
        assert!(
            NeighbourQuery::new(1, 0, NeighbourOrder::Newest, 33, 1)
                .encode()
                .is_err()
        );
        assert!(
            NeighbourQuery::new(1, 0, NeighbourOrder::Unknown(4), 6, 1)
                .encode()
                .is_err()
        );
        assert!(
            NeighbourQuery::new(1, 0, NeighbourOrder::Newest, 6, 0)
                .encode()
                .is_err()
        );
    }

    #[test]
    fn parse_regions_response_is_strict() {
        let mut raw = Vec::new();
        raw.extend_from_slice(&42_u32.to_le_bytes());
        raw.extend_from_slice(b"eu,apac\0\0");
        let parsed = parse_regions_response(&raw).expect("valid regions");
        assert_eq!(parsed.clock, 42);
        assert_eq!(parsed.names, vec!["eu", "apac"]);

        assert!(parse_regions_response(&[0, 1, 2, 3]).is_err());
        assert!(parse_regions_response(&[0; 5]).is_err());
        let invalid_utf8 = {
            let mut raw = 42_u32.to_le_bytes().to_vec();
            raw.push(0xff);
            raw
        };
        assert!(parse_regions_response(&invalid_utf8).is_err());

        let interior_zero = {
            let mut raw = 42_u32.to_le_bytes().to_vec();
            raw.extend_from_slice(b"a\0,b");
            raw
        };
        assert!(parse_regions_response(&interior_zero).is_err());
    }

    #[test]
    fn parse_owner_response_is_strict() {
        let mut valid = Vec::new();
        valid.extend_from_slice(&99_u32.to_le_bytes());
        valid.extend_from_slice(b"node\nAlice\0");
        let parsed = parse_owner_response(&valid).expect("owner should parse");
        assert_eq!(parsed.clock, 99);
        assert_eq!(parsed.name, "node");
        assert_eq!(parsed.owner, "Alice");

        let missing_newline = {
            let mut raw = Vec::new();
            raw.extend_from_slice(&1_u32.to_le_bytes());
            raw.extend_from_slice(b"noda owner\0");
            raw
        };
        assert!(parse_owner_response(&missing_newline).is_err());

        let extra_newline = {
            let mut raw = Vec::new();
            raw.extend_from_slice(&1_u32.to_le_bytes());
            raw.extend_from_slice(b"name\nowner\nextra");
            raw
        };
        assert!(parse_owner_response(&extra_newline).is_err());
    }

    #[test]
    fn parse_basic_response_requires_exact_length() {
        let valid = {
            let mut raw = Vec::new();
            raw.extend_from_slice(&7_u32.to_le_bytes());
            raw.push(0x82);
            raw
        };
        let parsed = parse_basic_response(&valid).expect("basic should parse");
        assert_eq!(parsed.clock, 7);
        assert_eq!(parsed.feature.kind, 0x02);
        assert!(parsed.feature.disabled);

        let mut short = valid.clone();
        short.pop();
        assert!(parse_basic_response(&short).is_err());

        let mut long = valid.clone();
        long.push(0);
        assert!(parse_basic_response(&long).is_err());
    }

    #[test]
    fn parse_acl_payload_skips_tombstones_and_rejects_partial_entries() {
        let raw = vec![0, 0, 0, 0, 0, 0, 0x80, 1, 2, 3, 4, 5, 6, 0x80];
        let parsed = parse_acl_payload(&raw).expect("ACL should parse");
        assert_eq!(parsed.entries.len(), 1);
        assert_eq!(parsed.entries[0].pubkey_prefix, [1, 2, 3, 4, 5, 6]);

        let raw = vec![1, 2, 3, 4, 5, 6, 0x80, 1, 2, 3];
        assert!(parse_acl_payload(&raw).is_err());
    }

    #[test]
    fn parse_neighbour_page_is_strict() {
        let mut raw = Vec::new();
        raw.extend_from_slice(&2_u16.to_le_bytes());
        raw.extend_from_slice(&2_u16.to_le_bytes());
        raw.extend_from_slice(&[1, 2, 3, 4, 5, 6, 10, 0, 0, 0, 4]);
        raw.extend_from_slice(&[7, 8, 9, 10, 11, 12, 5, 0, 0, 0, 248]);
        let parsed = parse_neighbour_page(&raw, 6).expect("neighbour page");
        assert_eq!(parsed.total_count, 2);
        assert_eq!(parsed.result_count, 2);
        assert_eq!(parsed.entries[1].secs_ago, 5);
        assert!((parsed.entries[1].snr_db - -2.0).abs() < f64::EPSILON);

        let mut short = raw.clone();
        short.pop();
        assert!(parse_neighbour_page(&short, 6).is_err());

        let mut bad_counts = raw.clone();
        bad_counts[2] = 255;
        bad_counts[3] = 255;
        assert!(parse_neighbour_page(&bad_counts, 6).is_err());

        let mut extra = raw.clone();
        extra.extend_from_slice(&[1, 2, 3]);
        assert!(parse_neighbour_page(&extra, 6).is_err());
        assert!(parse_neighbour_page(&raw, 0).is_err());
    }

    #[test]
    fn parse_telemetry_payload_parses_known_types_and_terminates() {
        let mut raw = vec![
            1,
            LppTelemetryKind::DigitalInput.code(),
            0x80,
            2,
            LppTelemetryKind::RelativeHumidity.code(),
            255,
            3,
            LppTelemetryKind::Polyline.code(),
            8,
            227,
        ];
        raw.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        raw.push(0);

        let readings = parse_telemetry_payload(&raw).expect("telemetry");
        assert_eq!(readings.len(), 3);
        assert_eq!(readings[0].kind.code(), 0);
        assert!((readings[1].values[0] - 127.5).abs() < f64::EPSILON);
        assert_eq!(readings[2].raw.as_deref(), Some(&raw[8..16]));

        let truncated = &raw[..raw.len() - 2];
        assert!(parse_telemetry_payload(truncated).is_err());

        let mut bad = raw.clone();
        bad[1] = 200;
        assert!(parse_telemetry_payload(&bad).is_err());
    }

    #[test]
    fn parse_telemetry_payload_uses_meshcore_widths_and_scales() {
        let raw = [
            1,
            LppTelemetryKind::Voltage.code(),
            0x01,
            0xF4,
            2,
            LppTelemetryKind::Frequency.code(),
            0,
            0,
            0x01,
            0xF4,
            3,
            LppTelemetryKind::Gps.code(),
            0x05,
            0xB8,
            0xD8,
            0xFE,
            0x4E,
            0x68,
            0,
            0x30,
            0x39,
        ];
        let parsed = parse_telemetry_payload(&raw).expect("telemetry should parse");
        assert_eq!(parsed[0].values, vec![5.0]);
        assert_eq!(parsed[1].values, vec![500.0]);
        assert_eq!(parsed[2].values, vec![37.5, -11.1, 123.45]);
    }

    #[test]
    fn parse_summary_payload_rejects_unknown_or_truncated() {
        let mut raw = Vec::new();
        raw.extend_from_slice(&1_u32.to_le_bytes());
        raw.push(3);
        raw.push(LppTelemetryKind::Temperature.code());
        raw.extend_from_slice(&1000_i16.to_be_bytes());
        raw.extend_from_slice(&2000_i16.to_be_bytes());
        raw.extend_from_slice(&1500_i16.to_be_bytes());

        let parsed = parse_summary_payload(&raw).expect("summary");
        assert_eq!(parsed.entries.len(), 1);
        assert_eq!(parsed.entries[0].channel, 3);
        assert!((parsed.entries[0].minimum - 100.0).abs() < f64::EPSILON);
        assert!((parsed.entries[0].maximum - 200.0).abs() < f64::EPSILON);
        assert!((parsed.entries[0].average - 150.0).abs() < f64::EPSILON);

        let mut truncated = raw.clone();
        truncated.pop();
        assert!(parse_summary_payload(&truncated).is_err());

        let mut unknown = raw.clone();
        unknown[5] = 199;
        assert!(parse_summary_payload(&unknown).is_err());
    }

    #[test]
    fn parse_functions_reject_payloads_beyond_limit() {
        let oversized = vec![0_u8; MAX_REMOTE_PAYLOAD_LEN + 1];
        assert_eq!(
            parse_telemetry_payload(&oversized),
            Err(RemotePayloadError::PayloadTooLarge {
                actual: MAX_REMOTE_PAYLOAD_LEN + 1,
                maximum: MAX_REMOTE_PAYLOAD_LEN,
            })
        );
    }

    #[test]
    fn feature_flag_helpers_round_trip() {
        let encoded = RemoteFeature::from_feature_byte(0x81);
        assert_eq!(encoded.kind, 1);
        assert!(encoded.disabled);
        assert_eq!(encoded.to_feature_byte(), 0x81);
    }

    #[test]
    fn binary_request_payload_builder() {
        assert_eq!(
            binary_request_payload(BinaryRequestKind::Acl),
            Some(vec![0, 0])
        );
        assert_eq!(binary_request_payload(BinaryRequestKind::Summary), None);
        assert!(binary_request_payload(BinaryRequestKind::Status).is_some());
    }
}

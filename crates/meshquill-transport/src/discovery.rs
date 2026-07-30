//! Backend-neutral discovery data.

use std::fmt;

use meshquill_core::transport::TransportKind;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// A concrete target that can be persisted and used to create a transport.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
pub enum TransportTarget {
    /// A BLE peripheral selected by the stable identifier returned by discovery.
    Ble {
        /// Platform peripheral identifier.
        selector: String,
    },
    /// A serial device path and line speed.
    Serial {
        /// Device path or Windows COM name.
        port: String,
        /// Baud rate in symbols per second.
        baud: u32,
    },
    /// A manually supplied TCP endpoint.
    Tcp {
        /// DNS hostname or IP address.
        host: String,
        /// TCP port.
        port: u16,
    },
}

impl TransportTarget {
    /// Returns the core diagnostic transport kind.
    #[must_use]
    pub const fn kind(&self) -> TransportKind {
        match self {
            Self::Ble { .. } => TransportKind::Ble,
            Self::Serial { .. } => TransportKind::Serial,
            Self::Tcp { .. } => TransportKind::Tcp,
        }
    }

    /// Validates this target without opening it.
    ///
    /// # Errors
    /// Returns [`TargetError`] when a selector, path, host, baud rate, or port is invalid.
    pub fn validate(&self) -> Result<(), TargetError> {
        match self {
            Self::Ble { selector } => validate_nonempty("selector", selector),
            Self::Serial { port, baud } => {
                validate_nonempty("port", port)?;
                validate_nonzero("baud", *baud)
            }
            Self::Tcp { host, port } => {
                validate_nonempty("host", host)?;
                validate_nonzero("port", *port)
            }
        }
    }
}

/// One device or endpoint reported by transport discovery.
///
/// `id` is stable within the operating-system backend and is prefixed by the transport name so it
/// can safely be used as a UI or cache key. `target` contains the exact reusable connection data.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DiscoveredDevice {
    /// Stable, transport-qualified identifier.
    pub id: String,
    /// Human-readable device or endpoint name.
    pub display_name: String,
    /// Core transport kind, serialized as a lowercase string.
    #[serde(with = "transport_kind_serde")]
    pub transport: TransportKind,
    /// Reusable connection target.
    pub target: TransportTarget,
    /// BLE address, serial path, or TCP hostname when available.
    pub address: Option<String>,
    /// TCP port when applicable.
    pub port: Option<u16>,
    /// BLE received signal strength in dBm when advertised.
    pub rssi: Option<i16>,
    /// Human-readable capability and recovery notes.
    pub notes: Vec<String>,
}

impl DiscoveredDevice {
    /// Validates that the record's kind matches its reusable target.
    ///
    /// # Errors
    /// Returns [`TargetError`] for invalid target data or mismatched record metadata.
    pub fn validate(&self) -> Result<(), TargetError> {
        self.target.validate()?;
        if self.transport != self.target.kind() {
            return Err(TargetError::Invalid {
                field: "transport",
                message: "record kind does not match its target".to_string(),
            });
        }
        Ok(())
    }
}

/// Compatibility name for a unified discovery result.
pub type DiscoveryRecord = DiscoveredDevice;

/// Builds a discovery-style record for a manually supplied TCP endpoint.
///
/// TCP has no network scan in this crate: callers explicitly supply a host and port. This helper
/// puts that endpoint into the same shape as BLE and serial discovery results.
///
/// # Errors
/// Returns [`TargetError`] when `host` is blank or `port` is zero.
pub fn manual_tcp_device(
    host: impl Into<String>,
    port: u16,
) -> Result<DiscoveredDevice, TargetError> {
    let host = host.into();
    let target = TransportTarget::Tcp {
        host: host.clone(),
        port,
    };
    target.validate()?;

    let endpoint = format_tcp_endpoint(&host, port);
    Ok(DiscoveredDevice {
        id: format!("tcp:{endpoint}"),
        display_name: endpoint,
        transport: TransportKind::Tcp,
        target,
        address: Some(host),
        port: Some(port),
        rssi: None,
        notes: vec![
            "Manually configured TCP endpoint; reachability is checked on connect.".to_string(),
        ],
    })
}

/// Configuration error for a reusable transport target.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum TargetError {
    /// A field does not satisfy the target's invariants.
    #[error("invalid transport target field '{field}': {message}")]
    Invalid {
        /// Name of the invalid field.
        field: &'static str,
        /// Actionable validation detail.
        message: String,
    },
}

/// Discovery failure with a recovery-oriented diagnostic.
#[derive(Debug, Error)]
pub enum DiscoveryError {
    /// The caller explicitly cancelled BLE discovery.
    #[error("BLE discovery was cancelled; retry the scan when ready")]
    Cancelled,
    /// No host Bluetooth adapter is available.
    #[error(
        "no Bluetooth Low Energy adapter is available; enable Bluetooth and check that the OS exposes an adapter"
    )]
    NoBleAdapter,
    /// Every detected Bluetooth adapter is powered off.
    #[error("all Bluetooth adapters are powered off; enable Bluetooth and retry discovery")]
    BlePoweredOff,
    /// A BLE provider operation failed.
    #[error(
        "BLE {operation} failed: {source}; check Bluetooth permissions, adapter state, and whether another application owns the device"
    )]
    Ble {
        /// Operation that failed.
        operation: &'static str,
        /// Provider failure.
        #[source]
        source: btleplug::Error,
    },
    /// A BLE provider operation exceeded the configured discovery phase timeout.
    #[error(
        "BLE {operation} timed out after {timeout:?}; check adapter responsiveness and retry discovery"
    )]
    BleTimeout {
        /// Operation that exceeded its deadline.
        operation: &'static str,
        /// Configured timeout for the discovery phase.
        timeout: std::time::Duration,
    },
    /// Serial enumeration failed.
    #[error(
        "serial port enumeration failed: {source}; check device presence and OS device permissions"
    )]
    SerialEnumeration {
        /// Provider failure.
        #[source]
        source: tokio_serial::Error,
    },
    /// A manually supplied or discovered target is invalid.
    #[error(transparent)]
    InvalidTarget(#[from] TargetError),
    /// A background enumeration worker could not be joined.
    #[error("serial enumeration worker failed: {message}; retry the operation")]
    Worker {
        /// Join failure detail.
        message: String,
    },
}

pub(crate) fn validate_nonempty(field: &'static str, value: &str) -> Result<(), TargetError> {
    if value.trim().is_empty() {
        return Err(TargetError::Invalid {
            field,
            message: "must not be blank".to_string(),
        });
    }
    Ok(())
}

pub(crate) fn validate_nonzero<T>(field: &'static str, value: T) -> Result<(), TargetError>
where
    T: Copy + Default + PartialEq + fmt::Display,
{
    if value == T::default() {
        return Err(TargetError::Invalid {
            field,
            message: format!("must be non-zero (got {value})"),
        });
    }
    Ok(())
}

pub(crate) fn format_tcp_endpoint(host: &str, port: u16) -> String {
    if host.contains(':') && !(host.starts_with('[') && host.ends_with(']')) {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

mod transport_kind_serde {
    use meshquill_core::transport::TransportKind;
    use serde::{Deserialize, Deserializer, Serializer, de};

    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub(super) fn serialize<S>(kind: &TransportKind, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(match kind {
            TransportKind::Ble => "ble",
            TransportKind::Serial => "serial",
            TransportKind::Tcp => "tcp",
            TransportKind::Scripted => "scripted",
            TransportKind::Unknown => "unknown",
        })
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<TransportKind, D::Error>
    where
        D: Deserializer<'de>,
    {
        match String::deserialize(deserializer)?.as_str() {
            "ble" => Ok(TransportKind::Ble),
            "serial" => Ok(TransportKind::Serial),
            "tcp" => Ok(TransportKind::Tcp),
            "scripted" => Ok(TransportKind::Scripted),
            "unknown" => Ok(TransportKind::Unknown),
            value => Err(de::Error::unknown_variant(
                value,
                &["ble", "serial", "tcp", "scripted", "unknown"],
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_tcp_record_is_actionable_and_serde_stable() {
        let record = manual_tcp_device("2001:db8::5", 5_000).expect("valid endpoint");
        assert_eq!(record.id, "tcp:[2001:db8::5]:5000");
        assert_eq!(record.display_name, "[2001:db8::5]:5000");
        assert_eq!(record.transport, TransportKind::Tcp);
        assert_eq!(record.address.as_deref(), Some("2001:db8::5"));
        assert_eq!(record.port, Some(5_000));
        assert!(record.validate().is_ok());

        let json = serde_json::to_string(&record).expect("record should serialize");
        assert!(json.contains("\"transport\":\"tcp\""));
        let decoded: DiscoveredDevice =
            serde_json::from_str(&json).expect("record should deserialize");
        assert_eq!(decoded, record);
    }

    #[test]
    fn targets_reject_blank_or_zero_fields() {
        assert!(manual_tcp_device(" ", 5_000).is_err());
        assert!(manual_tcp_device("host", 0).is_err());
        assert!(
            TransportTarget::Serial {
                port: String::new(),
                baud: 115_200,
            }
            .validate()
            .is_err()
        );
    }
}

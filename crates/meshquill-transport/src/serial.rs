//! Serial discovery and framed logical-packet transport.

use std::{fmt, io, time::Duration};

use async_trait::async_trait;
use meshquill_core::{
    TransportError,
    transport::{ReconnectableTransport, Transport, TransportKind},
};
use tokio::{io::AsyncWriteExt, task, time};
use tokio_serial::{
    SerialPortBuilderExt, SerialPortInfo, SerialPortType, SerialStream, UsbPortInfo,
};

use crate::{
    discovery::{DiscoveredDevice, DiscoveryError, TargetError, TransportTarget, validate_timeout},
    framed::{
        FramedReadState, invalidate_on_terminal_read_error, validate_payload, write_framed_bounded,
    },
};

/// Default baud rate used for discovered `MeshCore` serial devices.
pub const DEFAULT_SERIAL_BAUD: u32 = 115_200;

/// Enumerates serial ports exposed by the operating system.
///
/// Enumeration only reports candidates. A listed device can still be unavailable, disconnected,
/// or held exclusively by another process when it is opened.
///
/// # Errors
/// Returns [`DiscoveryError::SerialEnumeration`] when the operating system cannot enumerate ports,
/// or [`DiscoveryError::InvalidTarget`] if a provider emits an unusable record.
pub fn discover_serial() -> Result<Vec<DiscoveredDevice>, DiscoveryError> {
    let ports = tokio_serial::available_ports()
        .map_err(|source| DiscoveryError::SerialEnumeration { source })?;
    serial_records(ports)
}

/// Enumerates serial ports without blocking an async runtime worker.
///
/// # Errors
/// Returns the same errors as [`discover_serial`], plus [`DiscoveryError::Worker`] if the blocking
/// enumeration worker terminates unexpectedly.
pub async fn discover_serial_async() -> Result<Vec<DiscoveredDevice>, DiscoveryError> {
    task::spawn_blocking(discover_serial)
        .await
        .map_err(|error| DiscoveryError::Worker {
            message: error.to_string(),
        })?
}

/// A logical-packet transport over an asynchronous serial device.
///
/// App-to-device writes use `0x3c` outer frames and device-to-app reads decode and resynchronize
/// `0x3e` frames through `meshquill-core`'s bounded decoder. The configured timeout bounds
/// provider open, framed-write, and close operations.
pub struct SerialTransport {
    port: String,
    baud: u32,
    connect_timeout: Duration,
    stream: Option<SerialStream>,
    read_state: FramedReadState,
}

impl SerialTransport {
    /// Creates a disconnected serial transport. `connect_timeout` is retained for API
    /// compatibility and bounds each provider open, framed-write, and close operation.
    ///
    /// # Errors
    /// Returns [`TargetError`] when the port is blank, the baud rate is zero, or the timeout is
    /// zero or greater than 24 hours.
    pub fn new(
        port: impl Into<String>,
        baud: u32,
        connect_timeout: Duration,
    ) -> Result<Self, TargetError> {
        let port = port.into();
        let target = TransportTarget::Serial {
            port: port.clone(),
            baud,
        };
        target.validate()?;
        validate_timeout("connect_timeout", connect_timeout)?;

        Ok(Self {
            port,
            baud,
            connect_timeout,
            stream: None,
            read_state: FramedReadState::new(),
        })
    }

    /// Returns the configured port path or name.
    #[must_use]
    pub fn port(&self) -> &str {
        &self.port
    }

    /// Returns the configured baud rate.
    #[must_use]
    pub const fn baud(&self) -> u32 {
        self.baud
    }

    /// Returns the configured provider-operation timeout for open, write, and close.
    #[must_use]
    pub const fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    /// Returns a persistable copy of the connection target.
    #[must_use]
    pub fn target(&self) -> TransportTarget {
        TransportTarget::Serial {
            port: self.port.clone(),
            baud: self.baud,
        }
    }

    /// Reports whether this value currently owns an open serial handle.
    #[must_use]
    pub const fn is_connected(&self) -> bool {
        self.stream.is_some()
    }
}

impl fmt::Debug for SerialTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SerialTransport")
            .field("port", &self.port)
            .field("baud", &self.baud)
            .field("connect_timeout", &self.connect_timeout)
            .field("connected", &self.stream.is_some())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl Transport for SerialTransport {
    fn kind(&self) -> TransportKind {
        TransportKind::Serial
    }

    async fn connect(&mut self) -> Result<(), TransportError> {
        if self.stream.is_some() {
            return Err(TransportError::Io(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "serial port '{}' is already connected; disconnect before connecting again",
                    self.port
                ),
            )));
        }

        self.read_state.reset();
        let port = self.port.clone();
        let baud = self.baud;
        let mut open =
            task::spawn_blocking(move || tokio_serial::new(port, baud).open_native_async());

        let stream = match time::timeout(self.connect_timeout, &mut open).await {
            Ok(Ok(Ok(stream))) => stream,
            Ok(Ok(Err(error))) => {
                return Err(TransportError::Io(serial_provider_error(
                    &error, "open", &self.port,
                )));
            }
            Ok(Err(error)) => {
                return Err(TransportError::Io(io::Error::other(format!(
                    "serial open worker for '{}' failed: {error}",
                    self.port
                ))));
            }
            Err(_elapsed) => {
                open.abort();
                return Err(serial_timeout("open", &self.port, self.connect_timeout));
            }
        };

        self.stream = Some(stream);
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<(), TransportError> {
        self.read_state.reset();
        let Some(mut stream) = self.stream.take() else {
            return Ok(());
        };

        time::timeout(self.connect_timeout, stream.shutdown())
            .await
            .map_err(|_elapsed| serial_timeout("close", &self.port, self.connect_timeout))?
            .map_err(|error| contextual_io(&error, "close", &self.port))
            .map_err(TransportError::Io)
    }

    async fn write(&mut self, payload: &[u8]) -> Result<(), TransportError> {
        validate_payload(payload)?;
        let port = self.port.clone();
        write_framed_bounded(
            &mut self.stream,
            &mut self.read_state,
            payload,
            self.connect_timeout,
        )
        .await
        .map_err(|error| contextual_transport(error, "write to", &port))
    }

    async fn read(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
        let port = self.port.clone();
        let result = match self.stream.as_mut() {
            Some(stream) => self.read_state.read_from(stream).await,
            None => Err(TransportError::NotConnected),
        };

        match result {
            Ok(None) => {
                self.stream = None;
                self.read_state.reset();
                Ok(None)
            }
            Ok(packet) => Ok(packet),
            Err(error) => {
                invalidate_on_terminal_read_error(&mut self.stream, &mut self.read_state, &error);
                Err(contextual_transport(error, "read from", &port))
            }
        }
    }
}

impl ReconnectableTransport for SerialTransport {}

fn serial_records(
    ports: impl IntoIterator<Item = SerialPortInfo>,
) -> Result<Vec<DiscoveredDevice>, DiscoveryError> {
    let mut records = ports
        .into_iter()
        .map(serial_record)
        .collect::<Result<Vec<_>, _>>()?;
    records.sort_by(|left, right| left.id.cmp(&right.id));
    Ok(records)
}

fn serial_record(info: SerialPortInfo) -> Result<DiscoveredDevice, TargetError> {
    let SerialPortInfo {
        port_name,
        port_type,
    } = info;
    let target = TransportTarget::Serial {
        port: port_name.clone(),
        baud: DEFAULT_SERIAL_BAUD,
    };
    target.validate()?;

    let (id, display_name, notes) = match port_type {
        SerialPortType::UsbPort(usb) => usb_metadata(&port_name, &usb),
        SerialPortType::BluetoothPort => (
            format!("serial:{port_name}"),
            port_name.clone(),
            vec![
                "Bluetooth-backed serial port reported by the operating system.".to_string(),
                format!("Default baud: {DEFAULT_SERIAL_BAUD}."),
            ],
        ),
        SerialPortType::PciPort => (
            format!("serial:{port_name}"),
            port_name.clone(),
            vec![
                "Built-in PCI serial port reported by the operating system.".to_string(),
                format!("Default baud: {DEFAULT_SERIAL_BAUD}."),
            ],
        ),
        SerialPortType::Unknown => (
            format!("serial:{port_name}"),
            port_name.clone(),
            vec![
                "Serial port type was not identified; verify this is the MeshCore device before connecting."
                    .to_string(),
                format!("Default baud: {DEFAULT_SERIAL_BAUD}."),
            ],
        ),
    };

    Ok(DiscoveredDevice {
        id,
        display_name,
        transport: TransportKind::Serial,
        target,
        address: Some(port_name),
        port: None,
        rssi: None,
        notes,
    })
}

fn usb_metadata(port_name: &str, usb: &UsbPortInfo) -> (String, String, Vec<String>) {
    let mut notes = vec![
        format!("USB VID:PID {:04x}:{:04x}.", usb.vid, usb.pid),
        format!("Default baud: {DEFAULT_SERIAL_BAUD}."),
    ];
    if let Some(manufacturer) = &usb.manufacturer {
        notes.push(format!("Manufacturer: {manufacturer}."));
    }
    if let Some(serial) = &usb.serial_number {
        notes.push(format!("USB serial number: {serial}."));
    }

    let display_name = usb.product.as_deref().map_or_else(
        || port_name.to_string(),
        |product| format!("{product} ({port_name})"),
    );
    let id = usb.serial_number.as_deref().map_or_else(
        || format!("serial:{port_name}"),
        |serial| format!("serial:usb:{:04x}:{:04x}:{serial}", usb.vid, usb.pid),
    );
    (id, display_name, notes)
}

fn serial_provider_error(error: &tokio_serial::Error, operation: &str, port: &str) -> io::Error {
    let kind = match error.kind() {
        tokio_serial::ErrorKind::NoDevice => io::ErrorKind::NotFound,
        tokio_serial::ErrorKind::InvalidInput => io::ErrorKind::InvalidInput,
        tokio_serial::ErrorKind::Io(kind) => kind,
        tokio_serial::ErrorKind::Unknown => io::ErrorKind::Other,
    };
    io::Error::new(
        kind,
        format!(
            "serial {operation} for '{port}' failed: {error}; check the device path, permissions, and whether another process holds the port"
        ),
    )
}

fn serial_timeout(operation: &str, port: &str, timeout: Duration) -> TransportError {
    TransportError::Io(io::Error::new(
        io::ErrorKind::TimedOut,
        format!("serial {operation} for '{port}' timed out after {timeout:?}"),
    ))
}

fn contextual_transport(error: TransportError, operation: &str, port: &str) -> TransportError {
    match error {
        TransportError::Io(error) => TransportError::Io(contextual_io(&error, operation, port)),
        other => other,
    }
}

fn contextual_io(error: &io::Error, operation: &str, port: &str) -> io::Error {
    io::Error::new(
        error.kind(),
        format!("serial {operation} '{port}' failed: {error}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usb_metadata_is_stable_and_actionable() {
        let records = serial_records([SerialPortInfo {
            port_name: "/dev/ttyACM0".to_string(),
            port_type: SerialPortType::UsbPort(UsbPortInfo {
                vid: 0x239a,
                pid: 0x8029,
                serial_number: Some("mesh-7".to_string()),
                manufacturer: Some("Mesh Vendor".to_string()),
                product: Some("MeshCore Companion".to_string()),
            }),
        }])
        .expect("valid provider metadata");

        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert_eq!(record.id, "serial:usb:239a:8029:mesh-7");
        assert_eq!(record.display_name, "MeshCore Companion (/dev/ttyACM0)");
        assert_eq!(record.address.as_deref(), Some("/dev/ttyACM0"));
        assert!(record.notes.iter().any(|note| note.contains("239a:8029")));
        assert!(record.notes.iter().any(|note| note.contains("Mesh Vendor")));
        assert!(record.validate().is_ok());
    }

    #[test]
    fn discovery_records_are_sorted_by_stable_id() {
        let records = serial_records([
            SerialPortInfo {
                port_name: "z-port".to_string(),
                port_type: SerialPortType::Unknown,
            },
            SerialPortInfo {
                port_name: "a-port".to_string(),
                port_type: SerialPortType::Unknown,
            },
        ])
        .expect("valid records");
        assert_eq!(records[0].id, "serial:a-port");
        assert_eq!(records[1].id, "serial:z-port");
    }

    #[test]
    fn constructor_validates_and_exposes_target() {
        assert!(SerialTransport::new("", 115_200, Duration::from_secs(1)).is_err());
        assert!(SerialTransport::new("COM1", 0, Duration::from_secs(1)).is_err());
        assert!(SerialTransport::new("COM1", 115_200, Duration::ZERO).is_err());
        assert!(
            SerialTransport::new(
                "COM1",
                115_200,
                meshquill_core::MAX_OPERATION_TIMEOUT.saturating_add(Duration::from_nanos(1)),
            )
            .is_err()
        );

        let transport = SerialTransport::new("COM1", 115_200, Duration::from_secs(2))
            .expect("valid serial target");
        assert_eq!(transport.port(), "COM1");
        assert_eq!(transport.baud(), 115_200);
        assert_eq!(
            transport.target(),
            TransportTarget::Serial {
                port: "COM1".to_string(),
                baud: 115_200,
            }
        );
    }
}

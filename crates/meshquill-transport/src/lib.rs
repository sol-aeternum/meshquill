#![warn(missing_docs, unreachable_pub)]

//! Operating-system transport implementations for Meshquill.
//!
//! The [`Transport`](meshquill_core::transport::Transport) boundary always carries one logical
//! companion packet. TCP and serial add/remove the `MeshCore` outer stream envelope internally,
//! while BLE sends and receives the logical packet directly over the Nordic UART service.

/// Bluetooth Low Energy discovery and Nordic UART transport.
pub mod ble;
/// Shared discovery records, targets, and errors.
pub mod discovery;
mod framed;
/// USB and platform serial discovery and transport.
pub mod serial;
/// Framed TCP transport.
pub mod tcp;

pub use ble::{
    BleTransport, NORDIC_UART_NOTIFY_CHARACTERISTIC, NORDIC_UART_SERVICE,
    NORDIC_UART_WRITE_CHARACTERISTIC, discover_ble, discover_ble_with_cancellation,
};
pub use discovery::{
    DiscoveredDevice, DiscoveryError, DiscoveryRecord, TargetError, TransportTarget,
    manual_tcp_device,
};
pub use serial::{SerialTransport, discover_serial, discover_serial_async};
pub use tcp::TcpTransport;

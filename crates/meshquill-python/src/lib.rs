#![warn(missing_docs, unreachable_pub)]

//! Async Python bindings for Meshquill's Rust managed client, store, and transports.

mod client;
mod demo;
mod discovery;
mod errors;
mod models;
mod streams;
mod util;

use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3::wrap_pyfunction;

const PUBLIC_EXPORTS: &[&str] = &[
    "Ack",
    "AdvertPath",
    "AmbiguousContactError",
    "AuthenticationError",
    "AutoAddConfig",
    "BackpressureError",
    "Client",
    "ClientClosedError",
    "ConfigurationError",
    "Contact",
    "ContactUri",
    "CustomVariable",
    "CustomVariables",
    "DefaultFloodScope",
    "DeviceInfo",
    "DeviceRejectedError",
    "DeviceStats",
    "DisconnectedError",
    "DiscoveredDevice",
    "DiscoveryError",
    "Event",
    "EventStream",
    "FrequencyRange",
    "InvalidArgumentError",
    "MeshcoreError",
    "Message",
    "MessageStream",
    "ProtocolError",
    "SelfInfo",
    "SendReceipt",
    "StreamLaggedError",
    "TelemetryResponse",
    "TimeoutError",
    "TransportError",
    "TuningParams",
    "UnsupportedFeatureError",
    "__version__",
    "discover_ble",
    "discover_serial",
];

/// Native implementation module for the `meshcore_sdk` mixed Python package.
#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    errors::add_exceptions(module)?;
    models::add_classes(module)?;
    streams::add_classes(module)?;
    client::add_class(module)?;
    module.add_function(wrap_pyfunction!(discovery::discover_ble_py, module)?)?;
    module.add_function(wrap_pyfunction!(discovery::discover_serial_py, module)?)?;
    module.add("__all__", PyList::new(module.py(), PUBLIC_EXPORTS)?)?;
    Ok(())
}

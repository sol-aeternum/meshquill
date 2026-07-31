use meshquill_core::{CoreError, TransportError as CoreTransportError};
use meshquill_store::StoreError;
use meshquill_transport::{DiscoveryError as CoreDiscoveryError, TargetError};
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

create_exception!(_native, MeshcoreError, PyException);
create_exception!(_native, ConfigurationError, MeshcoreError);
create_exception!(_native, DiscoveryError, MeshcoreError);
create_exception!(_native, TransportError, MeshcoreError);
create_exception!(_native, ProtocolError, MeshcoreError);
create_exception!(_native, DeviceRejectedError, ProtocolError);
create_exception!(_native, TimeoutError, MeshcoreError);
create_exception!(_native, DisconnectedError, TransportError);
create_exception!(_native, InvalidArgumentError, MeshcoreError);
create_exception!(_native, AmbiguousContactError, InvalidArgumentError);
create_exception!(_native, BackpressureError, TransportError);
create_exception!(_native, StreamLaggedError, MeshcoreError);
create_exception!(_native, UnsupportedFeatureError, MeshcoreError);
create_exception!(_native, AuthenticationError, MeshcoreError);
create_exception!(_native, ClientClosedError, MeshcoreError);

#[derive(Clone, Copy)]
pub(crate) enum Operation {
    Read,
    Send,
}

pub(crate) fn core_error(error: CoreError, operation: Operation) -> PyErr {
    let ambiguous = matches!(operation, Operation::Send)
        && matches!(
            &error,
            CoreError::Transport(_) | CoreError::Timeout | CoreError::Disconnected
        );
    let suffix = if ambiguous {
        " The device may have received the command; this SDK will not replay it automatically."
    } else {
        ""
    };

    match error {
        CoreError::Transport(error) => transport_error(error, suffix),
        CoreError::Parse(error) => ProtocolError::new_err(format!(
            "the companion returned malformed protocol data: {error}"
        )),
        CoreError::Timeout => TimeoutError::new_err(format!("operation timed out.{suffix}")),
        CoreError::ProtocolInvariant(message) => {
            ProtocolError::new_err(format!("companion protocol error: {message}"))
        }
        CoreError::DeviceRejected { operation, code } => {
            let code = code.map_or_else(String::new, |value| {
                format!(" (firmware code {value:#04x})")
            });
            DeviceRejectedError::new_err(format!("the device rejected {operation}{code}"))
        }
        CoreError::FeatureDisabled { feature } => UnsupportedFeatureError::new_err(format!(
            "the device firmware does not enable the {feature} feature"
        )),
        CoreError::AuthenticationFailed => {
            AuthenticationError::new_err("remote authentication failed")
        }
        CoreError::InvalidArgument { field, message } => {
            InvalidArgumentError::new_err(format!("invalid {field}: {message}"))
        }
        CoreError::InvalidUtf8 { field } => {
            ProtocolError::new_err(format!("the companion returned invalid UTF-8 in {field}"))
        }
        CoreError::Disconnected => DisconnectedError::new_err(format!(
            "the companion transport disconnected.{suffix} Call reconnect() before retrying."
        )),
        CoreError::ActorStopped => {
            ClientClosedError::new_err("the client is closed and cannot accept more operations")
        }
    }
}

fn transport_error(error: CoreTransportError, suffix: &str) -> PyErr {
    match error {
        CoreTransportError::NotConnected | CoreTransportError::Closed => {
            DisconnectedError::new_err(format!(
                "the companion transport is not connected.{suffix} Call reconnect() before retrying."
            ))
        }
        CoreTransportError::Timeout => {
            TimeoutError::new_err(format!("the companion transport timed out.{suffix}"))
        }
        CoreTransportError::Backpressure { queue, capacity } => BackpressureError::new_err(
            format!("the bounded transport queue '{queue}' is full (capacity {capacity})"),
        ),
        CoreTransportError::PayloadTooLarge { maximum, actual } => InvalidArgumentError::new_err(
            format!("payload is {actual} bytes; the transport maximum is {maximum} bytes"),
        ),
        CoreTransportError::ReconnectUnsupported => TransportError::new_err(
            "this transport does not support reconnect; construct a new client",
        ),
        CoreTransportError::ReconnectFailed { message } => {
            TransportError::new_err(format!("transport reconnect failed: {message}"))
        }
        CoreTransportError::Io(error) => TransportError::new_err(format!(
            "companion transport I/O failed: {error}.{suffix} Check the target, permissions, and device availability."
        )),
    }
}

pub(crate) fn target_error(error: &TargetError) -> PyErr {
    InvalidArgumentError::new_err(error.to_string())
}

pub(crate) fn discovery_error(error: &CoreDiscoveryError) -> PyErr {
    DiscoveryError::new_err(error.to_string())
}

pub(crate) fn store_error(error: StoreError) -> PyErr {
    match error {
        StoreError::Io(error) => {
            ConfigurationError::new_err(format!("could not read Meshquill configuration: {error}"))
        }
        StoreError::MissingRuntimePath { platform, context } => {
            ConfigurationError::new_err(format!(
                "cannot locate the Meshquill configuration directory on {platform:?}: {context}"
            ))
        }
        StoreError::LockTimeout { path } => ConfigurationError::new_err(format!(
            "another Meshquill process is still updating '{}'; wait for it to finish, then retry",
            path.display()
        )),
        StoreError::Serde { .. } => ConfigurationError::new_err(
            "could not decode Meshquill configuration; check the TOML structure",
        ),
        StoreError::Parse { path, .. } => ConfigurationError::new_err(format!(
            "malformed Meshquill configuration at '{}'; correct the TOML syntax",
            path.display()
        )),
        StoreError::UnsupportedVersion { version } => ConfigurationError::new_err(format!(
            "Meshquill configuration version {version} is not supported by this SDK"
        )),
        StoreError::Validation { field, message } => ConfigurationError::new_err(format!(
            "invalid Meshquill configuration field '{field}': {message}"
        )),
        StoreError::SecretUnavailable { backend, .. } => ConfigurationError::new_err(format!(
            "a configured secret could not be loaded from {backend}; check that secret backend"
        )),
        StoreError::PromptRequired => ConfigurationError::new_err(
            "the configured secret requires an interactive prompt, which the async SDK does not perform",
        ),
        StoreError::AtomicRename { path, .. } => ConfigurationError::new_err(format!(
            "could not atomically update Meshquill configuration at '{}'",
            path.display()
        )),
    }
}

pub(crate) fn add_exceptions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    // `create_exception!` accepts an identifier rather than a dotted module path. Canonicalize
    // every exported class to the extension's installed name so import-based pickle resolution
    // reaches the same object.
    macro_rules! add_exception {
        ($exception:ty, $name:literal) => {{
            let exception = module.py().get_type::<$exception>();
            exception.setattr("__module__", "meshcore_sdk._native")?;
            module.add($name, exception)?;
        }};
    }

    add_exception!(MeshcoreError, "MeshcoreError");
    add_exception!(ConfigurationError, "ConfigurationError");
    add_exception!(DiscoveryError, "DiscoveryError");
    add_exception!(TransportError, "TransportError");
    add_exception!(ProtocolError, "ProtocolError");
    add_exception!(DeviceRejectedError, "DeviceRejectedError");
    add_exception!(TimeoutError, "TimeoutError");
    add_exception!(DisconnectedError, "DisconnectedError");
    add_exception!(InvalidArgumentError, "InvalidArgumentError");
    add_exception!(AmbiguousContactError, "AmbiguousContactError");
    add_exception!(BackpressureError, "BackpressureError");
    add_exception!(StreamLaggedError, "StreamLaggedError");
    add_exception!(UnsupportedFeatureError, "UnsupportedFeatureError");
    add_exception!(AuthenticationError, "AuthenticationError");
    add_exception!(ClientClosedError, "ClientClosedError");
    Ok(())
}

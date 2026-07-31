use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use meshquill_core::domain::CommandTracking;
use meshquill_core::{
    Client as CoreClient, Contact, CoreError, MANAGED_CLIENT_COMMAND_CAPACITY, ManagedClient,
    ReconnectableTransport, SelfInfo, StatsType,
};
use meshquill_store::{Config, ConfigStore, LoadOutcome, Platform, TransportConfig};
use meshquill_transport::{BleTransport, SerialTransport, TcpTransport};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use tokio::sync::watch;

use crate::demo::DemoTransport;
use crate::errors::{
    AmbiguousContactError, ClientClosedError, ConfigurationError, InvalidArgumentError,
    MeshcoreError, Operation, core_error, store_error, target_error,
};
use crate::models::{
    PyAck, PyContact, PyDeviceInfo, PyDeviceStats, PyMessage, PySelfInfo, PySendReceipt,
    PyTelemetryResponse,
};
use crate::streams::{EventHub, PyEventStream, PyMessageStream};
use crate::util::duration_from_seconds;

const DEFAULT_CONNECT_TIMEOUT: f64 = 5.0;
const DEFAULT_REQUEST_TIMEOUT: f64 = 5.0;
const APP_NAME: &str = "meshquill";

struct ClientInner {
    managed: ManagedClient,
    event_hub: EventHub,
    self_info: RwLock<SelfInfo>,
    closed: watch::Sender<bool>,
    profile_name: Option<String>,
    transport: &'static str,
    request_timeout: Duration,
}

impl Drop for ClientInner {
    fn drop(&mut self) {
        let _ = self.closed.send_replace(true);
    }
}

/// Async `MeshCore` client backed by the Rust managed-client actor.
#[pyclass(
    name = "Client",
    frozen,
    skip_from_py_object,
    module = "meshcore_sdk._native"
)]
#[derive(Clone)]
pub(crate) struct PyClient {
    inner: Arc<ClientInner>,
}

#[pymethods]
impl PyClient {
    /// Load the default stored profile, connect its configured transport, and perform the handshake.
    #[classmethod]
    #[pyo3(signature = (*, profile=None, config_path=None))]
    fn auto<'py>(
        _class: &Bound<'py, pyo3::types::PyType>,
        py: Python<'py>,
        profile: Option<String>,
        config_path: Option<PathBuf>,
    ) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let selected = tokio::task::spawn_blocking(move || {
                load_auto_selection(profile.as_deref(), config_path)
            })
            .await
            .map_err(|error| {
                ConfigurationError::new_err(format!(
                    "configuration loading worker stopped unexpectedly: {error}"
                ))
            })??;
            let client = connect_selected(selected).await?;
            Ok(client)
        })
    }

    /// Connect an explicit TCP endpoint. TCP discovery is intentionally not performed.
    #[classmethod]
    #[pyo3(signature = (host, port=5000, *, connect_timeout=DEFAULT_CONNECT_TIMEOUT, request_timeout=DEFAULT_REQUEST_TIMEOUT))]
    fn tcp<'py>(
        _class: &Bound<'py, pyo3::types::PyType>,
        py: Python<'py>,
        host: String,
        port: u16,
        connect_timeout: f64,
        request_timeout: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let connect_timeout = duration_from_seconds(connect_timeout, "connect_timeout")?;
        let request_timeout = duration_from_seconds(request_timeout, "request_timeout")?;
        let transport =
            TcpTransport::new(host, port, connect_timeout).map_err(|error| target_error(&error))?;
        connected_awaitable(
            py,
            transport,
            request_timeout,
            MANAGED_CLIENT_COMMAND_CAPACITY,
            None,
            "tcp",
        )
    }

    /// Connect an explicit serial port using framed `MeshCore` companion packets.
    #[classmethod]
    #[pyo3(signature = (port, *, baud=115_200, connect_timeout=DEFAULT_CONNECT_TIMEOUT, request_timeout=DEFAULT_REQUEST_TIMEOUT))]
    fn serial<'py>(
        _class: &Bound<'py, pyo3::types::PyType>,
        py: Python<'py>,
        port: String,
        baud: u32,
        connect_timeout: f64,
        request_timeout: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let connect_timeout = duration_from_seconds(connect_timeout, "connect_timeout")?;
        let request_timeout = duration_from_seconds(request_timeout, "request_timeout")?;
        let transport = SerialTransport::new(port, baud, connect_timeout)
            .map_err(|error| target_error(&error))?;
        connected_awaitable(
            py,
            transport,
            request_timeout,
            MANAGED_CLIENT_COMMAND_CAPACITY,
            None,
            "serial",
        )
    }

    /// Connect an explicit BLE selector returned by `discover_ble`.
    #[classmethod]
    #[pyo3(signature = (selector, *, connect_timeout=DEFAULT_CONNECT_TIMEOUT, request_timeout=DEFAULT_REQUEST_TIMEOUT))]
    fn ble<'py>(
        _class: &Bound<'py, pyo3::types::PyType>,
        py: Python<'py>,
        selector: String,
        connect_timeout: f64,
        request_timeout: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let connect_timeout = duration_from_seconds(connect_timeout, "connect_timeout")?;
        let request_timeout = duration_from_seconds(request_timeout, "request_timeout")?;
        let transport =
            BleTransport::new(selector, connect_timeout).map_err(|error| target_error(&error))?;
        connected_awaitable(
            py,
            transport,
            request_timeout,
            MANAGED_CLIENT_COMMAND_CAPACITY,
            None,
            "ble",
        )
    }

    /// Connect a deterministic in-memory companion for examples and tests only.
    #[classmethod]
    #[pyo3(signature = (*, request_timeout=DEFAULT_REQUEST_TIMEOUT))]
    fn demo<'py>(
        _class: &Bound<'py, pyo3::types::PyType>,
        py: Python<'py>,
        request_timeout: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let request_timeout = duration_from_seconds(request_timeout, "request_timeout")?;
        let transport = DemoTransport::seeded().map_err(|_| {
            MeshcoreError::new_err("could not initialize the deterministic demo companion")
        })?;
        connected_awaitable(
            py,
            transport,
            request_timeout,
            MANAGED_CLIENT_COMMAND_CAPACITY,
            None,
            "demo",
        )
    }

    /// Transport kind (`ble`, `serial`, `tcp`, or `demo`).
    #[getter]
    fn transport(&self) -> &'static str {
        self.inner.transport
    }

    /// Stored profile name for `auto()`, or `None` for explicit constructors.
    #[getter]
    fn profile_name(&self) -> Option<String> {
        self.inner.profile_name.clone()
    }

    /// Effective companion request timeout in seconds.
    #[getter]
    fn request_timeout(&self) -> f64 {
        self.inner.request_timeout.as_secs_f64()
    }

    /// Whether graceful shutdown has completed.
    #[getter]
    fn is_closed(&self) -> bool {
        *self.inner.closed.borrow()
    }

    /// Session identity returned by the most recent connect or reconnect handshake.
    #[getter]
    fn self_info(&self, py: Python<'_>) -> PyResult<Py<PySelfInfo>> {
        let info = read_self_info(&self.inner.self_info);
        Py::new(py, PySelfInfo::from(info))
    }

    /// Create an independent bounded replay/live subscription to every client event.
    ///
    /// Replay starts at client construction and includes the initial handshake. If bounded
    /// retention or live delivery overflows, iteration raises `StreamLaggedError` and can resume.
    fn events(&self, py: Python<'_>) -> PyResult<Py<PyEventStream>> {
        self.ensure_open()?;
        Py::new(
            py,
            PyEventStream::new(
                self.inner.event_hub.subscribe(),
                self.inner.closed.subscribe(),
            ),
        )
    }

    /// Create an independent bounded replay/live subscription filtered to messages.
    ///
    /// Messages observed while the constructor handshake was in progress are retained. If
    /// bounded retention or live delivery overflows, iteration raises `StreamLaggedError`.
    fn messages(&self, py: Python<'_>) -> PyResult<Py<PyMessageStream>> {
        self.ensure_open()?;
        Py::new(
            py,
            PyMessageStream::new(
                self.inner.event_hub.subscribe(),
                self.inner.closed.subscribe(),
            ),
        )
    }

    /// List contacts, optionally starting at a firmware last-modified marker.
    #[pyo3(signature = (last_modified=None))]
    fn list_contacts<'py>(
        &self,
        py: Python<'py>,
        last_modified: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let managed = self.inner.managed.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let contacts = managed
                .list_contacts(last_modified)
                .await
                .map_err(|error| core_error(error, Operation::Read))?;
            Ok(contacts_into_py(contacts))
        })
    }

    /// Send text to a unique contact name or an unambiguous public-key hex prefix.
    ///
    /// Cancellation can race with the device write. A cancelled caller must treat the outcome as
    /// ambiguous; the Rust actor finishes the queued operation and never retries or replays it.
    #[pyo3(signature = (contact, text, *, attempt=0))]
    fn send<'py>(
        &self,
        py: Python<'py>,
        contact: String,
        text: String,
        attempt: u8,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let managed = self.inner.managed.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let contacts = managed
                .list_contacts(None)
                .await
                .map_err(|error| core_error(error, Operation::Read))?;
            let destination = resolve_contact(&contacts, &contact)?;
            let tracking = managed
                .send_direct_text(&destination, attempt, &text)
                .await
                .map_err(|error| core_error(error, Operation::Send))?;
            Ok(receipt_into_py(tracking))
        })
    }

    /// Send text directly to a six-byte prefix or full 32-byte public key.
    ///
    /// Cancellation and transport failures can leave an ambiguous device outcome. This method is
    /// never replayed automatically.
    #[pyo3(signature = (destination, text, *, attempt=0))]
    fn send_direct<'py>(
        &self,
        py: Python<'py>,
        destination: &Bound<'_, PyAny>,
        text: String,
        attempt: u8,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let destination = destination_prefix(destination)?;
        let managed = self.inner.managed.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let tracking = managed
                .send_direct_text(&destination, attempt, &text)
                .await
                .map_err(|error| core_error(error, Operation::Send))?;
            Ok(receipt_into_py(tracking))
        })
    }

    /// Send a direct command string to a six-byte prefix or full public key.
    #[pyo3(signature = (destination, command, *, attempt=0))]
    fn send_command<'py>(
        &self,
        py: Python<'py>,
        destination: &Bound<'_, PyAny>,
        command: String,
        attempt: u8,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let destination = destination_prefix(destination)?;
        let managed = self.inner.managed.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let tracking = managed
                .send_direct_command(&destination, attempt, &command)
                .await
                .map_err(|error| core_error(error, Operation::Send))?;
            Ok(receipt_into_py(tracking))
        })
    }

    /// Send text to a channel and wait for the firmware's immediate confirmation.
    #[pyo3(signature = (channel, text, *, text_type=0))]
    fn send_channel<'py>(
        &self,
        py: Python<'py>,
        channel: u8,
        text: String,
        text_type: u8,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let managed = self.inner.managed.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            managed
                .send_channel_message(channel, text_type, &text)
                .await
                .map_err(|error| core_error(error, Operation::Send))?;
            Ok(())
        })
    }

    /// Fetch one queued message, returning `None` when the firmware queue is empty.
    fn fetch_queued_message<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let managed = self.inner.managed.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let message = managed
                .sync_next_message()
                .await
                .map_err(|error| core_error(error, Operation::Read))?;
            Ok(message.map(PyMessage::from))
        })
    }

    /// Wait for an ACK by `SendReceipt`, four-byte code, or eight-digit hex code.
    #[pyo3(signature = (receipt_or_code, *, timeout=None))]
    fn wait_for_ack<'py>(
        &self,
        py: Python<'py>,
        receipt_or_code: &Bound<'_, PyAny>,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let (code, suggested_timeout) = ack_code(receipt_or_code)?;
        let timeout = timeout
            .map(|value| duration_from_seconds(value, "timeout"))
            .transpose()?
            .or(suggested_timeout);
        let managed = self.inner.managed.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let ack = managed
                .wait_for_ack(code, timeout)
                .await
                .map_err(|error| core_error(error, Operation::Read))?;
            Ok(PyAck::from(ack))
        })
    }

    /// Query firmware and device metadata.
    fn device_info<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let managed = self.inner.managed.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let info = managed
                .query_device_info()
                .await
                .map_err(|error| core_error(error, Operation::Read))?;
            Ok(PyDeviceInfo::from(info))
        })
    }

    /// Disconnect while retaining the target for a later explicit `reconnect()`.
    fn disconnect<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let managed = self.inner.managed.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            managed
                .disconnect()
                .await
                .map_err(|error| core_error(error, Operation::Read))?;
            Ok(())
        })
    }

    /// Reconnect the same target and perform a fresh handshake without replaying sends.
    fn reconnect<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let info = inner
                .managed
                .reconnect()
                .await
                .map_err(|error| core_error(error, Operation::Read))?;
            write_self_info(&inner.self_info, info.clone());
            Ok(PySelfInfo::from(info))
        })
    }

    /// Gracefully disconnect and stop the Rust actor. The operation is idempotent after success.
    fn shutdown<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.shutdown_awaitable(py)
    }

    /// Query one device-statistics family (`core`, `radio`, or `packets`).
    #[pyo3(signature = (kind = "core"))]
    fn telemetry<'py>(&self, py: Python<'py>, kind: &str) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let stats_type = match kind {
            "core" => StatsType::Core,
            "radio" => StatsType::Radio,
            "packets" => StatsType::Packets,
            _ => {
                return Err(InvalidArgumentError::new_err(
                    "telemetry kind must be 'core', 'radio', or 'packets'",
                ));
            }
        };
        let managed = self.inner.managed.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let stats = managed
                .get_stats(stats_type)
                .await
                .map_err(|error| core_error(error, Operation::Read))?;
            Ok(PyDeviceStats::from(stats))
        })
    }

    /// Query the local node's raw Cayenne-LPP-compatible telemetry payload.
    fn self_telemetry<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let managed = self.inner.managed.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = managed
                .get_self_telemetry()
                .await
                .map_err(|error| core_error(error, Operation::Read))?;
            Ok(PyTelemetryResponse::from(response))
        })
    }

    fn __aenter__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let client = self.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(client) })
    }

    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exception_type: &Bound<'_, PyAny>,
        _exception: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.shutdown_awaitable(py)
    }

    fn __repr__(&self) -> String {
        format!(
            "Client(transport={:?}, profile_name={:?}, is_closed={})",
            self.inner.transport,
            self.inner.profile_name,
            *self.inner.closed.borrow()
        )
    }
}

impl PyClient {
    fn ensure_open(&self) -> PyResult<()> {
        if *self.inner.closed.borrow() {
            return Err(ClientClosedError::new_err(
                "the client has been shut down; construct a new client",
            ));
        }
        Ok(())
    }

    fn shutdown_awaitable<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if *inner.closed.borrow() {
                return Ok(());
            }
            // Once this future has been polled, graceful shutdown must finish even if its Python
            // waiter is cancelled. Dropping a Tokio JoinHandle detaches rather than aborts it.
            let shutdown = tokio::spawn(async move {
                let result = inner.managed.shutdown().await;
                let _ = inner.closed.send_replace(true);
                result
            });
            let result = shutdown.await.map_err(|error| {
                MeshcoreError::new_err(format!(
                    "the client shutdown worker stopped unexpectedly: {error}"
                ))
            })?;
            match result {
                Ok(()) | Err(CoreError::ActorStopped) => Ok(()),
                Err(error) => Err(core_error(error, Operation::Read)),
            }
        })
    }
}

fn connected_awaitable<'py, T>(
    py: Python<'py>,
    transport: T,
    request_timeout: Duration,
    command_capacity: usize,
    profile_name: Option<String>,
    transport_name: &'static str,
) -> PyResult<Bound<'py, PyAny>>
where
    T: ReconnectableTransport + Send + 'static,
{
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let client = connect_transport(
            transport,
            request_timeout,
            command_capacity,
            profile_name,
            transport_name,
        )
        .await?;
        Ok(client)
    })
}

async fn connect_transport<T>(
    transport: T,
    request_timeout: Duration,
    command_capacity: usize,
    profile_name: Option<String>,
    transport_name: &'static str,
) -> PyResult<PyClient>
where
    T: ReconnectableTransport + Send + 'static,
{
    let core = CoreClient::with_timeout(transport, request_timeout)
        .map_err(|error| core_error(error, Operation::Read))?;
    let managed = ManagedClient::spawn_with_capacity(core, command_capacity)
        .map_err(|error| core_error(error, Operation::Read))?;
    let (closed, _) = watch::channel(false);
    // Register the sole core receiver and start the bounded relay before connect publishes the
    // SelfInfo/Connected handshake or any intervening message packets.
    let event_hub = EventHub::spawn(managed.subscribe(), closed.subscribe());
    let self_info = match managed.connect().await {
        Ok(info) => info,
        Err(error) => {
            let _ = closed.send_replace(true);
            return Err(core_error(error, Operation::Read));
        }
    };
    if let Err(message) = event_hub.wait_for_initial_replay().await {
        let _ = closed.send_replace(true);
        return Err(MeshcoreError::new_err(message));
    }
    Ok(PyClient {
        inner: Arc::new(ClientInner {
            managed,
            event_hub,
            self_info: RwLock::new(self_info),
            closed,
            profile_name,
            transport: transport_name,
            request_timeout,
        }),
    })
}

fn contacts_into_py(contacts: Vec<Contact>) -> Vec<PyContact> {
    contacts.into_iter().map(PyContact::from).collect()
}

fn receipt_into_py(tracking: CommandTracking) -> PySendReceipt {
    PySendReceipt::from(tracking)
}

fn resolve_contact(contacts: &[Contact], query: &str) -> PyResult<[u8; 6]> {
    let named = contacts
        .iter()
        .filter(|contact| contact.adv_name == query)
        .collect::<Vec<_>>();
    match named.as_slice() {
        [contact] => return Ok(first_six(contact)),
        [] => {}
        _ => {
            return Err(AmbiguousContactError::new_err(format!(
                "more than one contact is named {query:?}; use an unambiguous public-key prefix"
            )));
        }
    }

    let key_query = query
        .strip_prefix("0x")
        .unwrap_or(query)
        .to_ascii_lowercase();
    if key_query.len() < 12
        || key_query.len() > 64
        || !key_query.len().is_multiple_of(2)
        || !key_query.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(InvalidArgumentError::new_err(format!(
            "no contact named {query:?}; a key selector must be 12 to 64 hexadecimal characters"
        )));
    }
    let keyed = contacts
        .iter()
        .filter(|contact| contact.public_key.to_hex().starts_with(&key_query))
        .collect::<Vec<_>>();
    match keyed.as_slice() {
        [contact] => Ok(first_six(contact)),
        [] => Err(InvalidArgumentError::new_err(format!(
            "no contact matches public-key prefix {key_query:?}"
        ))),
        _ => Err(AmbiguousContactError::new_err(format!(
            "public-key prefix {key_query:?} matches more than one contact"
        ))),
    }
}

fn first_six(contact: &Contact) -> [u8; 6] {
    let mut prefix = [0_u8; 6];
    prefix.copy_from_slice(&contact.public_key.as_bytes()[..6]);
    prefix
}

fn destination_prefix(value: &Bound<'_, PyAny>) -> PyResult<[u8; 6]> {
    if let Ok(text) = value.extract::<String>() {
        let text = text.strip_prefix("0x").unwrap_or(&text);
        if !matches!(text.len(), 12 | 64) || !text.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(InvalidArgumentError::new_err(
                "destination must be 12 hex digits (a six-byte prefix) or 64 hex digits (a full public key)",
            ));
        }
        let decoded = hex::decode(text).map_err(|_| {
            InvalidArgumentError::new_err("destination contains invalid hexadecimal digits")
        })?;
        return decoded[..6].try_into().map_err(|_| {
            InvalidArgumentError::new_err("destination must contain at least six bytes")
        });
    }
    if let Ok(bytes) = value.extract::<Vec<u8>>() {
        if !matches!(bytes.len(), 6 | 32) {
            return Err(InvalidArgumentError::new_err(
                "destination bytes must contain a six-byte prefix or a full 32-byte public key",
            ));
        }
        return bytes[..6].try_into().map_err(|_| {
            InvalidArgumentError::new_err("destination must contain at least six bytes")
        });
    }
    Err(PyTypeError::new_err(
        "destination must be a hex string or bytes-like object",
    ))
}

fn ack_code(value: &Bound<'_, PyAny>) -> PyResult<([u8; 4], Option<Duration>)> {
    if let Ok(receipt) = value.extract::<PyRef<'_, PySendReceipt>>() {
        return Ok((
            receipt.code,
            Some(Duration::from_millis(u64::from(
                receipt.suggested_timeout_ms,
            ))),
        ));
    }
    if let Ok(text) = value.extract::<String>() {
        let text = text.strip_prefix("0x").unwrap_or(&text);
        if text.len() != 8 || !text.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(InvalidArgumentError::new_err(
                "ACK code must contain exactly eight hexadecimal digits",
            ));
        }
        let decoded = hex::decode(text).map_err(|_| {
            InvalidArgumentError::new_err("ACK code contains invalid hexadecimal digits")
        })?;
        let code = decoded.as_slice().try_into().map_err(|_| {
            InvalidArgumentError::new_err("ACK code must contain exactly four bytes")
        })?;
        return Ok((code, None));
    }
    if let Ok(bytes) = value.extract::<Vec<u8>>() {
        let code = bytes.as_slice().try_into().map_err(|_| {
            InvalidArgumentError::new_err("ACK code must contain exactly four bytes")
        })?;
        return Ok((code, None));
    }
    Err(PyTypeError::new_err(
        "receipt_or_code must be a SendReceipt, eight-digit hex string, or four-byte value",
    ))
}

fn read_self_info(lock: &RwLock<SelfInfo>) -> SelfInfo {
    match lock.read() {
        Ok(info) => info.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    }
}

fn write_self_info(lock: &RwLock<SelfInfo>, value: SelfInfo) {
    match lock.write() {
        Ok(mut info) => *info = value,
        Err(poisoned) => *poisoned.into_inner() = value,
    }
}

#[derive(Clone)]
enum SelectedTransport {
    Ble { selector: String },
    Serial { port: String, baud: u32 },
    Tcp { host: String, port: u16 },
}

struct AutoSelection {
    profile_name: String,
    transport: SelectedTransport,
    connect_timeout: Duration,
    request_timeout: Duration,
    command_capacity: usize,
}

fn load_auto_selection(
    explicit_profile: Option<&str>,
    config_path: Option<PathBuf>,
) -> PyResult<AutoSelection> {
    let store = match config_path {
        Some(path) => ConfigStore::new(path),
        None => {
            ConfigStore::from_default_path(current_platform(), APP_NAME).map_err(store_error)?
        }
    };
    let path = store.path().to_owned();
    let overrides = config_overrides();
    let config = match store.load_with_overrides(&overrides).map_err(store_error)? {
        LoadOutcome::Loaded(config) | LoadOutcome::NeedsMigration(config) => config,
        LoadOutcome::Missing => {
            return Err(ConfigurationError::new_err(format!(
                "Meshquill configuration was not found at '{}'; create a default device profile or pass an explicit transport constructor",
                path.display()
            )));
        }
    };
    select_profile(&config, explicit_profile)
}

fn select_profile(config: &Config, explicit_profile: Option<&str>) -> PyResult<AutoSelection> {
    let profile_name = explicit_profile
        .map(str::to_owned)
        .or_else(|| config.default_profile.clone())
        .ok_or_else(|| {
            ConfigurationError::new_err(
                "Meshquill configuration has no default_profile; select one in config or pass profile=",
            )
        })?;
    let profile = config.device_profiles.get(&profile_name).ok_or_else(|| {
        ConfigurationError::new_err(format!(
            "Meshquill profile {profile_name:?} does not exist in device_profiles"
        ))
    })?;
    let transport = match &profile.transport {
        TransportConfig::Ble { id, .. } => SelectedTransport::Ble {
            selector: id.clone(),
        },
        TransportConfig::Serial { port, baud } => SelectedTransport::Serial {
            port: port.clone(),
            baud: *baud,
        },
        TransportConfig::Tcp { host, port } => SelectedTransport::Tcp {
            host: host.clone(),
            port: *port,
        },
        TransportConfig::Mock { .. } => {
            return Err(ConfigurationError::new_err(
                "mock profiles are test-only and are not selected by Client.auto(); use the explicit Client.demo() constructor",
            ));
        }
    };
    let request_timeout_ms = profile
        .transport_overrides
        .as_ref()
        .and_then(|overrides| overrides.request_timeout_ms)
        .unwrap_or(config.timeout.request_timeout_ms);
    let command_capacity = usize::try_from(config.queues.outbound_capacity).map_err(|_| {
        ConfigurationError::new_err("configured outbound queue capacity is unsupported")
    })?;
    Ok(AutoSelection {
        profile_name,
        transport,
        connect_timeout: Duration::from_millis(config.timeout.connect_timeout_ms),
        request_timeout: Duration::from_millis(request_timeout_ms),
        command_capacity,
    })
}

async fn connect_selected(selection: AutoSelection) -> PyResult<PyClient> {
    let profile_name = Some(selection.profile_name);
    match selection.transport {
        SelectedTransport::Ble { selector } => {
            let transport = BleTransport::new(selector, selection.connect_timeout)
                .map_err(|error| target_error(&error))?;
            connect_transport(
                transport,
                selection.request_timeout,
                selection.command_capacity,
                profile_name,
                "ble",
            )
            .await
        }
        SelectedTransport::Serial { port, baud } => {
            let transport = SerialTransport::new(port, baud, selection.connect_timeout)
                .map_err(|error| target_error(&error))?;
            connect_transport(
                transport,
                selection.request_timeout,
                selection.command_capacity,
                profile_name,
                "serial",
            )
            .await
        }
        SelectedTransport::Tcp { host, port } => {
            let transport = TcpTransport::new(host, port, selection.connect_timeout)
                .map_err(|error| target_error(&error))?;
            connect_transport(
                transport,
                selection.request_timeout,
                selection.command_capacity,
                profile_name,
                "tcp",
            )
            .await
        }
    }
}

fn config_overrides() -> HashMap<String, String> {
    const KEYS: [&str; 5] = [
        "MESHQUILL_DEFAULT_PROFILE",
        "MESHQUILL_TIMEOUT_CONNECT_MS",
        "MESHQUILL_TIMEOUT_REQUEST_MS",
        "MESHQUILL_TIMEOUT_RETRY_MS",
        "MESHQUILL_QUEUES_OUTBOUND",
    ];
    KEYS.into_iter()
        .filter_map(|key| std::env::var(key).ok().map(|value| (key.to_owned(), value)))
        .collect()
}

#[cfg(target_os = "linux")]
const fn current_platform() -> Platform {
    Platform::Linux
}

#[cfg(target_os = "macos")]
const fn current_platform() -> Platform {
    Platform::Macos
}

#[cfg(target_os = "windows")]
const fn current_platform() -> Platform {
    Platform::Windows
}

pub(crate) fn add_class(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyClient>()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use meshquill_store::{
        CONFIG_VERSION, DeviceProfile, QueueSettings, TimeoutSettings, TransportOverrides,
    };

    use super::*;

    fn config_with_profiles() -> Config {
        let mut profiles = BTreeMap::new();
        profiles.insert(
            "backup".to_owned(),
            DeviceProfile {
                transport: TransportConfig::Serial {
                    port: "/dev/backup".to_owned(),
                    baud: 9_600,
                },
                transport_overrides: None,
                secret: None,
            },
        );
        profiles.insert(
            "primary".to_owned(),
            DeviceProfile {
                transport: TransportConfig::Tcp {
                    host: "127.0.0.1".to_owned(),
                    port: 5_001,
                },
                transport_overrides: Some(TransportOverrides {
                    request_timeout_ms: Some(777),
                }),
                secret: None,
            },
        );
        Config {
            version: CONFIG_VERSION,
            default_profile: Some("primary".to_owned()),
            device_profiles: profiles,
            timeout: TimeoutSettings {
                connect_timeout_ms: 123,
                request_timeout_ms: 456,
                retry_timeout_ms: 789,
            },
            queues: QueueSettings {
                inbound_capacity: 11,
                outbound_capacity: 12,
                event_capacity: 13,
            },
            ..Config::default()
        }
    }

    #[test]
    fn auto_uses_default_profile_and_profile_timeout_override() {
        let selected = select_profile(&config_with_profiles(), None).expect("profile selection");
        assert_eq!(selected.profile_name, "primary");
        assert!(matches!(
            selected.transport,
            SelectedTransport::Tcp { ref host, port } if host == "127.0.0.1" && port == 5_001
        ));
        assert_eq!(selected.connect_timeout, Duration::from_millis(123));
        assert_eq!(selected.request_timeout, Duration::from_millis(777));
        assert_eq!(selected.command_capacity, 12);
    }

    #[test]
    fn explicit_profile_overrides_default() {
        let selected =
            select_profile(&config_with_profiles(), Some("backup")).expect("profile selection");
        assert_eq!(selected.profile_name, "backup");
        assert!(matches!(
            selected.transport,
            SelectedTransport::Serial { ref port, baud } if port == "/dev/backup" && baud == 9_600
        ));
        assert_eq!(selected.request_timeout, Duration::from_millis(456));
    }

    #[tokio::test]
    async fn demo_constructor_retains_the_initial_handshake() {
        let transport = DemoTransport::seeded().expect("demo transport");
        let client = tokio::time::timeout(
            Duration::from_secs(1),
            connect_transport(
                transport,
                Duration::from_secs(1),
                MANAGED_CLIENT_COMMAND_CAPACITY,
                None,
                "demo",
            ),
        )
        .await
        .expect("constructor timeout")
        .expect("demo constructor");
        let mut events = client.inner.event_hub.subscribe();
        assert!(matches!(
            events.recv().await,
            Ok(meshquill_core::Event::SelfInfo(_))
        ));
        assert!(matches!(
            events.recv().await,
            Ok(meshquill_core::Event::Connected)
        ));
        client
            .inner
            .managed
            .shutdown()
            .await
            .expect("demo shutdown");
        let _ = client.inner.closed.send_replace(true);
    }
}

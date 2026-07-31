//! Bluetooth Low Energy discovery and Nordic UART transport.

use std::{collections::BTreeMap, fmt, future::Future, io, pin::Pin, time::Duration};

use async_trait::async_trait;
use btleplug::{
    api::{
        Central, CentralEvent, CentralState, CharPropFlags, Characteristic, Manager as _,
        Peripheral as _, ScanFilter, ValueNotification, WriteType,
    },
    platform::{Adapter, Manager, Peripheral},
};
use futures::{Stream, StreamExt, future};
use meshquill_core::{
    TransportError,
    protocol::MAX_INNER_PAYLOAD,
    transport::{ReconnectableTransport, Transport, TransportKind},
};
use tokio::time::{self, Instant};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::discovery::{
    DiscoveredDevice, DiscoveryError, TargetError, TransportTarget, validate_nonempty,
    validate_timeout,
};
use crate::framed::validate_payload;

/// Nordic UART service used by `MeshCore` companions.
pub const NORDIC_UART_SERVICE: Uuid = Uuid::from_u128(0x6e40_0001_b5a3_f393_e0a9_e50e_24dc_ca9e);
/// App-to-device Nordic UART write characteristic.
pub const NORDIC_UART_WRITE_CHARACTERISTIC: Uuid =
    Uuid::from_u128(0x6e40_0002_b5a3_f393_e0a9_e50e_24dc_ca9e);
/// Device-to-app Nordic UART notification characteristic.
pub const NORDIC_UART_NOTIFY_CHARACTERISTIC: Uuid =
    Uuid::from_u128(0x6e40_0003_b5a3_f393_e0a9_e50e_24dc_ca9e);

const TARGET_POLL_INTERVAL: Duration = Duration::from_millis(100);

type NotificationStream = Pin<Box<dyn Stream<Item = ValueNotification> + Send>>;
type AdapterEventStream = Pin<Box<dyn Stream<Item = CentralEvent> + Send>>;

/// Scans for peripherals advertising the `MeshCore` Nordic UART service.
///
/// The scan runs for exactly the supplied observation window, then explicitly stops every adapter
/// before returning the sorted results. `timeout` separately bounds the provider setup, scan-stop,
/// and result-collection phases, so a stalled provider cannot make discovery hang indefinitely. An
/// observation timeout is normal scan completion; a provider phase timeout is actionable error.
///
/// # Errors
/// Returns an actionable [`DiscoveryError`] when no adapter exists, Bluetooth is off, permissions
/// or provider operations fail, or the timeout is zero or greater than 24 hours.
pub async fn discover_ble(timeout: Duration) -> Result<Vec<DiscoveredDevice>, DiscoveryError> {
    discover_ble_with_cancellation(timeout, &CancellationToken::new()).await
}

/// Scans for `MeshCore` BLE companions with explicit cooperative cancellation.
///
/// Cancelling `cancellation` stops all active adapter scans before returning
/// [`DiscoveryError::Cancelled`]. Dropping the future also schedules best-effort scan cleanup.
///
/// # Errors
/// Returns the same provider errors as [`discover_ble`], or [`DiscoveryError::Cancelled`] when the
/// token is cancelled.
pub async fn discover_ble_with_cancellation(
    timeout: Duration,
    cancellation: &CancellationToken,
) -> Result<Vec<DiscoveredDevice>, DiscoveryError> {
    validate_scan_timeout(timeout)?;
    let setup_deadline = Instant::now() + timeout;

    let manager = with_discovery_deadline(
        setup_deadline,
        timeout,
        cancellation,
        Manager::new(),
        "manager initialization",
    )
    .await?;
    let adapters = with_discovery_deadline(
        setup_deadline,
        timeout,
        cancellation,
        manager.adapters(),
        "adapter enumeration",
    )
    .await?;
    if adapters.is_empty() {
        return Err(DiscoveryError::NoBleAdapter);
    }

    let mut usable_adapters = Vec::new();
    let mut powered_off = 0_usize;
    for adapter in adapters {
        let state = with_discovery_deadline(
            setup_deadline,
            timeout,
            cancellation,
            adapter.adapter_state(),
            "adapter state query",
        )
        .await?;
        if state == CentralState::PoweredOff {
            powered_off += 1;
        } else {
            usable_adapters.push(adapter);
        }
    }
    if usable_adapters.is_empty() && powered_off > 0 {
        return Err(DiscoveryError::BlePoweredOff);
    }

    let mut scans = ActiveScans::new();
    for adapter in &usable_adapters {
        scans.register(adapter.clone());
        let filter = ScanFilter {
            services: vec![NORDIC_UART_SERVICE],
        };
        if let Err(error) = with_discovery_deadline(
            setup_deadline,
            timeout,
            cancellation,
            adapter.start_scan(filter),
            "scan start",
        )
        .await
        {
            drop(scans.stop(timeout).await);
            return Err(error);
        }
    }

    tokio::select! {
        biased;
        () = cancellation.cancelled() => {
            drop(scans.stop(timeout).await);
            return Err(DiscoveryError::Cancelled);
        }
        () = time::sleep(timeout) => {}
    }
    scans
        .stop(timeout)
        .await
        .map_err(|error| discovery_scan_stop_error(error, timeout))?;

    collect_discovered_devices(
        &usable_adapters,
        cancellation,
        Instant::now() + timeout,
        timeout,
    )
    .await
}

/// A raw logical-packet BLE transport over the `MeshCore` Nordic UART service.
///
/// The transport retains its stable selector for reconnects, but retains no outbound packet queue
/// and never replays writes automatically. The configured timeout bounds each target-selection,
/// provider-connect, write, and whole disconnect-cleanup operation.
pub struct BleTransport {
    selector: String,
    connect_timeout: Duration,
    connection: Option<BleConnection>,
}

impl BleTransport {
    /// Creates a disconnected BLE transport for a discovered peripheral identifier or address.
    /// `connect_timeout` is retained for API compatibility and bounds selection, provider connect,
    /// write, and disconnect-cleanup operations.
    ///
    /// The constructor also accepts the `ble:`-qualified `id` exposed by [`DiscoveredDevice`].
    ///
    /// # Errors
    /// Returns [`TargetError`] when the selector is blank or the timeout is zero or greater than
    /// 24 hours.
    pub fn new(
        selector: impl Into<String>,
        connect_timeout: Duration,
    ) -> Result<Self, TargetError> {
        let selector = selector.into();
        validate_nonempty("selector", &selector)?;
        validate_timeout("connect_timeout", connect_timeout)?;
        Ok(Self {
            selector,
            connect_timeout,
            connection: None,
        })
    }

    /// Returns the configured peripheral selector.
    #[must_use]
    pub fn selector(&self) -> &str {
        &self.selector
    }

    /// Returns the configured BLE provider-operation timeout.
    #[must_use]
    pub const fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    /// Returns a persistable copy of the connection target.
    #[must_use]
    pub fn target(&self) -> TransportTarget {
        TransportTarget::Ble {
            selector: self.selector.clone(),
        }
    }

    /// Reports whether this value owns a subscribed provider connection.
    ///
    /// BLE disconnects are asynchronous; the next read, write, or application-level probe confirms
    /// current liveness.
    #[must_use]
    pub const fn is_connected(&self) -> bool {
        self.connection.is_some()
    }
}

impl fmt::Debug for BleTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BleTransport")
            .field("selector", &self.selector)
            .field("connect_timeout", &self.connect_timeout)
            .field("connected", &self.connection.is_some())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl Transport for BleTransport {
    fn kind(&self) -> TransportKind {
        TransportKind::Ble
    }

    async fn connect(&mut self) -> Result<(), TransportError> {
        if let Some(connection) = self.connection.take() {
            let still_connected = match with_ble_timeout(
                self.connect_timeout,
                connection.peripheral.is_connected(),
                "connection-state query",
                &self.selector,
            )
            .await
            {
                Ok(still_connected) => still_connected,
                Err(error) => {
                    // The local state stays invalid while provider cleanup runs. Cleanup has its
                    // own whole-operation deadline and never replays an outbound packet.
                    drop(close_connection(connection, &self.selector, self.connect_timeout).await);
                    return Err(error);
                }
            };
            if still_connected {
                self.connection = Some(connection);
                return Err(TransportError::Io(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!(
                        "BLE target '{}' is already connected; disconnect before connecting again",
                        self.selector
                    ),
                )));
            }
        }

        let (adapter, peripheral) = find_peripheral(&self.selector, self.connect_timeout).await?;
        match establish_connection(
            adapter,
            peripheral.clone(),
            &self.selector,
            self.connect_timeout,
        )
        .await
        {
            Ok(connection) => {
                self.connection = Some(connection);
                Ok(())
            }
            Err(error) => {
                best_effort_disconnect(&peripheral, self.connect_timeout).await;
                Err(error)
            }
        }
    }

    async fn disconnect(&mut self) -> Result<(), TransportError> {
        let Some(connection) = self.connection.take() else {
            return Ok(());
        };
        close_connection(connection, &self.selector, self.connect_timeout).await
    }

    async fn write(&mut self, payload: &[u8]) -> Result<(), TransportError> {
        validate_payload(payload)?;
        let (peripheral, write_characteristic, write_type, max_write_payload) = self
            .connection
            .as_ref()
            .map(|connection| {
                (
                    connection.peripheral.clone(),
                    connection.write_characteristic.clone(),
                    connection.write_type,
                    connection.max_write_payload,
                )
            })
            .ok_or(TransportError::NotConnected)?;
        validate_negotiated_payload(payload, max_write_payload)?;

        let connected = match with_ble_timeout(
            self.connect_timeout,
            peripheral.is_connected(),
            "pre-write connection-state query",
            &self.selector,
        )
        .await
        {
            Ok(connected) => connected,
            Err(error) => {
                self.connection = None;
                best_effort_disconnect(&peripheral, self.connect_timeout).await;
                return Err(error);
            }
        };
        if !connected {
            self.connection = None;
            return Err(TransportError::NotConnected);
        }

        let write_peripheral = peripheral.clone();
        let result = with_ble_timeout(
            self.connect_timeout,
            provider_write_once(payload, write_type, move |packet, write_type| async move {
                write_peripheral
                    .write(&write_characteristic, &packet, write_type)
                    .await
            }),
            "Nordic UART write (packet not retried)",
            &self.selector,
        )
        .await;
        if result.is_err() {
            self.connection = None;
            best_effort_disconnect(&peripheral, self.connect_timeout).await;
        }
        result
    }

    async fn read(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
        loop {
            let event = {
                let connection = self
                    .connection
                    .as_mut()
                    .ok_or(TransportError::NotConnected)?;
                if let Some(events) = &mut connection.events {
                    tokio::select! {
                        notification = connection.notifications.next() => {
                            BleReadEvent::Notification(notification)
                        }
                        adapter_event = events.next() => BleReadEvent::Adapter(adapter_event),
                    }
                } else {
                    BleReadEvent::Notification(connection.notifications.next().await)
                }
            };

            match event {
                BleReadEvent::Notification(Some(notification)) => {
                    if notification.uuid != NORDIC_UART_NOTIFY_CHARACTERISTIC {
                        continue;
                    }
                    validate_payload(&notification.value)?;
                    return Ok(Some(notification.value));
                }
                BleReadEvent::Notification(None) => {
                    self.connection = None;
                    return Ok(None);
                }
                BleReadEvent::Adapter(Some(CentralEvent::DeviceDisconnected(id))) => {
                    let matches_connection = self
                        .connection
                        .as_ref()
                        .is_some_and(|connection| id.to_string() == connection.peripheral_id);
                    if matches_connection {
                        self.connection = None;
                        return Ok(None);
                    }
                }
                BleReadEvent::Adapter(Some(_)) => {}
                BleReadEvent::Adapter(None) => {
                    if let Some(connection) = &mut self.connection {
                        connection.events = None;
                    }
                }
            }
        }
    }
}

#[async_trait]
impl ReconnectableTransport for BleTransport {
    async fn reconnect(&mut self) -> Result<(), TransportError> {
        if let Some(connection) = self.connection.take() {
            // Reconnect must still be possible after a link-loss cleanup error. No logical write is
            // retained or replayed, and a successful new connection is the authoritative result.
            drop(close_connection(connection, &self.selector, self.connect_timeout).await);
        }
        self.connect().await
    }
}

struct BleConnection {
    peripheral_id: String,
    peripheral: Peripheral,
    write_characteristic: Characteristic,
    notify_characteristic: Characteristic,
    write_type: WriteType,
    max_write_payload: usize,
    notifications: NotificationStream,
    events: Option<AdapterEventStream>,
}

struct UartCharacteristics {
    write: Characteristic,
    notify: Characteristic,
    write_type: WriteType,
}

enum BleReadEvent {
    Notification(Option<ValueNotification>),
    Adapter(Option<CentralEvent>),
}

async fn collect_discovered_devices(
    adapters: &[Adapter],
    cancellation: &CancellationToken,
    deadline: Instant,
    timeout: Duration,
) -> Result<Vec<DiscoveredDevice>, DiscoveryError> {
    let mut records = BTreeMap::<String, DiscoveredDevice>::new();
    for adapter in adapters {
        let adapter_name = with_discovery_deadline(
            deadline,
            timeout,
            cancellation,
            adapter.adapter_info(),
            "adapter information query",
        )
        .await?;
        let peripherals = with_discovery_deadline(
            deadline,
            timeout,
            cancellation,
            adapter.peripherals(),
            "scan result enumeration",
        )
        .await?;
        for peripheral in peripherals {
            let properties = with_discovery_deadline(
                deadline,
                timeout,
                cancellation,
                peripheral.properties(),
                "peripheral property read",
            )
            .await?;
            let Some(properties) = properties else {
                continue;
            };
            let advertises_uart = properties.services.contains(&NORDIC_UART_SERVICE)
                || properties.service_data.contains_key(&NORDIC_UART_SERVICE);
            if !advertises_uart {
                continue;
            }

            let peripheral_id = peripheral.id().to_string();
            let address = properties.address.to_string();
            let name = properties
                .local_name
                .or(properties.advertisement_name)
                .unwrap_or_else(|| format!("MeshCore BLE ({address})"));
            let record = ble_record(peripheral_id, address, name, properties.rssi, &adapter_name);

            records
                .entry(record.id.clone())
                .and_modify(|current| {
                    if record.rssi > current.rssi {
                        *current = record.clone();
                    }
                })
                .or_insert(record);
        }
    }
    Ok(records.into_values().collect())
}

fn ble_record(
    peripheral_id: String,
    address: String,
    display_name: String,
    rssi: Option<i16>,
    adapter_name: &str,
) -> DiscoveredDevice {
    DiscoveredDevice {
        id: format!("ble:{peripheral_id}"),
        display_name,
        transport: TransportKind::Ble,
        target: TransportTarget::Ble {
            selector: peripheral_id,
        },
        address: Some(address),
        port: None,
        rssi,
        notes: vec![
            "Advertises the MeshCore Nordic UART service.".to_string(),
            format!("Discovered via Bluetooth adapter: {adapter_name}."),
        ],
    }
}

async fn find_peripheral(
    requested_selector: &str,
    timeout: Duration,
) -> Result<(Adapter, Peripheral), TransportError> {
    let selector = requested_selector
        .strip_prefix("ble:")
        .unwrap_or(requested_selector);
    let manager = with_ble_timeout(
        timeout,
        Manager::new(),
        "manager initialization",
        requested_selector,
    )
    .await?;
    let adapters = with_ble_timeout(
        timeout,
        manager.adapters(),
        "adapter enumeration",
        requested_selector,
    )
    .await?;
    if adapters.is_empty() {
        return Err(TransportError::Io(io::Error::new(
            io::ErrorKind::NotFound,
            "no Bluetooth adapter is available; enable Bluetooth and retry",
        )));
    }

    if let Some(found) = find_cached(
        &adapters,
        selector,
        requested_selector,
        Instant::now() + timeout,
    )
    .await?
    {
        return Ok(found);
    }

    let mut scans = ActiveScans::new();
    let scan_setup_deadline = Instant::now() + timeout;
    for adapter in &adapters {
        scans.register(adapter.clone());
        if let Err(error) = with_ble_deadline(
            scan_setup_deadline,
            adapter.start_scan(ScanFilter {
                services: vec![NORDIC_UART_SERVICE],
            }),
            "scan start",
            requested_selector,
        )
        .await
        {
            drop(scans.stop(timeout).await);
            return Err(error);
        }
    }

    let deadline = Instant::now() + timeout;
    let found_result = loop {
        let now = Instant::now();
        if now >= deadline {
            break Ok(None);
        }

        match find_cached(&adapters, selector, requested_selector, deadline).await {
            Ok(Some(found)) => break Ok(Some(found)),
            Ok(None) => {}
            Err(error) => break Err(error),
        }
        time::sleep(TARGET_POLL_INTERVAL.min(deadline.saturating_duration_since(Instant::now())))
            .await;
    };
    let stop_result = scans
        .stop(timeout)
        .await
        .map_err(|error| transport_scan_stop_error(error, requested_selector));
    let found = match found_result {
        Ok(found) => {
            stop_result?;
            found
        }
        Err(error) => {
            drop(stop_result);
            return Err(error);
        }
    };

    found.ok_or_else(|| {
        TransportError::Io(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "BLE target '{requested_selector}' was not found before the scan timeout; ensure it is powered on, nearby, and not in firmware update mode"
            ),
        ))
    })
}

async fn find_cached(
    adapters: &[Adapter],
    selector: &str,
    requested_selector: &str,
    deadline: Instant,
) -> Result<Option<(Adapter, Peripheral)>, TransportError> {
    for adapter in adapters {
        let peripherals = with_ble_deadline(
            deadline,
            adapter.peripherals(),
            "peripheral enumeration",
            requested_selector,
        )
        .await?;
        for peripheral in peripherals {
            if peripheral.id().to_string().eq_ignore_ascii_case(selector)
                || peripheral
                    .address()
                    .to_string()
                    .eq_ignore_ascii_case(selector)
            {
                return Ok(Some((adapter.clone(), peripheral)));
            }
        }
    }
    Ok(None)
}

async fn establish_connection(
    adapter: Adapter,
    peripheral: Peripheral,
    selector: &str,
    timeout: Duration,
) -> Result<BleConnection, TransportError> {
    let already_connected = with_ble_timeout(
        timeout,
        peripheral.is_connected(),
        "connection-state query",
        selector,
    )
    .await?;
    if !already_connected {
        with_ble_timeout(timeout, peripheral.connect(), "connect", selector).await?;
    }
    let connected = with_ble_timeout(
        timeout,
        peripheral.is_connected(),
        "post-connect state query",
        selector,
    )
    .await?;
    if !connected {
        return Err(TransportError::Io(io::Error::new(
            io::ErrorKind::NotConnected,
            format!(
                "BLE provider returned from connect for '{selector}' without an active connection"
            ),
        )));
    }

    with_ble_timeout(
        timeout,
        peripheral.discover_services(),
        "GATT service discovery",
        selector,
    )
    .await?;
    let uart = resolve_uart_characteristics(&peripheral, selector)?;

    let notifications = with_ble_timeout(
        timeout,
        peripheral.notifications(),
        "notification stream creation",
        selector,
    )
    .await?;
    let events = with_ble_timeout(
        timeout,
        adapter.events(),
        "adapter event stream creation",
        selector,
    )
    .await?;
    with_ble_timeout(
        timeout,
        peripheral.subscribe(&uart.notify),
        "Nordic UART notification subscription",
        selector,
    )
    .await?;

    Ok(BleConnection {
        peripheral_id: peripheral.id().to_string(),
        max_write_payload: negotiated_payload_limit(peripheral.mtu(), uart.write_type),
        peripheral,
        write_characteristic: uart.write,
        notify_characteristic: uart.notify,
        write_type: uart.write_type,
        notifications,
        events: Some(events),
    })
}

fn resolve_uart_characteristics(
    peripheral: &Peripheral,
    selector: &str,
) -> Result<UartCharacteristics, TransportError> {
    if !peripheral
        .services()
        .iter()
        .any(|service| service.uuid == NORDIC_UART_SERVICE)
    {
        return Err(TransportError::Io(io::Error::new(
            io::ErrorKind::Unsupported,
            format!(
                "BLE target '{selector}' does not expose Nordic UART service {NORDIC_UART_SERVICE}"
            ),
        )));
    }

    let characteristics = peripheral.characteristics();
    let write_characteristic = characteristics
        .iter()
        .find(|characteristic| {
            characteristic.service_uuid == NORDIC_UART_SERVICE
                && characteristic.uuid == NORDIC_UART_WRITE_CHARACTERISTIC
        })
        .cloned()
        .ok_or_else(|| {
            missing_characteristic("write", NORDIC_UART_WRITE_CHARACTERISTIC, selector)
        })?;
    let notify_characteristic = characteristics
        .iter()
        .find(|characteristic| {
            characteristic.service_uuid == NORDIC_UART_SERVICE
                && characteristic.uuid == NORDIC_UART_NOTIFY_CHARACTERISTIC
        })
        .cloned()
        .ok_or_else(|| {
            missing_characteristic("notification", NORDIC_UART_NOTIFY_CHARACTERISTIC, selector)
        })?;
    let write_type = preferred_write_type(write_characteristic.properties).ok_or_else(|| {
        TransportError::Io(io::Error::new(
            io::ErrorKind::Unsupported,
            format!(
                "BLE Nordic UART write characteristic on '{selector}' supports neither write-without-response nor write-with-response"
            ),
        ))
    })?;
    if !notify_characteristic
        .properties
        .intersects(CharPropFlags::NOTIFY | CharPropFlags::INDICATE)
    {
        return Err(TransportError::Io(io::Error::new(
            io::ErrorKind::Unsupported,
            format!(
                "BLE Nordic UART notification characteristic on '{selector}' supports neither notify nor indicate"
            ),
        )));
    }

    Ok(UartCharacteristics {
        write: write_characteristic,
        notify: notify_characteristic,
        write_type,
    })
}

async fn close_connection(
    connection: BleConnection,
    selector: &str,
    timeout: Duration,
) -> Result<(), TransportError> {
    with_ble_cleanup_timeout(
        timeout,
        close_connection_provider(connection, selector),
        selector,
    )
    .await
}

async fn close_connection_provider(
    connection: BleConnection,
    selector: &str,
) -> Result<(), TransportError> {
    let mut first_error = None;
    let connected = match connection.peripheral.is_connected().await {
        Ok(true) => true,
        Ok(false) => false,
        Err(error) => {
            first_error = Some(ble_io(&error, "connection-state query", selector));
            true
        }
    };
    if !connected {
        return Ok(());
    }

    if let Err(error) = connection
        .peripheral
        .unsubscribe(&connection.notify_characteristic)
        .await
        && first_error.is_none()
    {
        first_error = Some(ble_io(
            &error,
            "Nordic UART notification unsubscribe",
            selector,
        ));
    }
    if let Err(error) = connection.peripheral.disconnect().await
        && first_error.is_none()
    {
        first_error = Some(ble_io(&error, "disconnect", selector));
    }

    first_error.map_or(Ok(()), Err)
}

async fn with_ble_cleanup_timeout<F>(
    timeout: Duration,
    cleanup: F,
    selector: &str,
) -> Result<(), TransportError>
where
    F: Future<Output = Result<(), TransportError>>,
{
    time::timeout(timeout, cleanup)
        .await
        .map_err(|_elapsed| ble_timeout("disconnect cleanup", selector))?
}

async fn best_effort_disconnect(peripheral: &Peripheral, timeout: Duration) {
    drop(time::timeout(timeout, peripheral.disconnect()).await);
}

fn preferred_write_type(properties: CharPropFlags) -> Option<WriteType> {
    if properties.contains(CharPropFlags::WRITE) {
        Some(WriteType::WithResponse)
    } else if properties.contains(CharPropFlags::WRITE_WITHOUT_RESPONSE) {
        Some(WriteType::WithoutResponse)
    } else {
        None
    }
}

fn negotiated_payload_limit(mtu: u16, write_type: WriteType) -> usize {
    match write_type {
        // A write request may use ATT Prepare/Execute Write below the provider API. It must be one
        // provider call so firmware observes one Nordic UART callback and one companion frame.
        WriteType::WithResponse => MAX_INNER_PAYLOAD,
        // ATT has no long-write procedure for write commands (without response).
        WriteType::WithoutResponse => usize::from(mtu.saturating_sub(3)).min(MAX_INNER_PAYLOAD),
    }
}

async fn provider_write_once<E, F, Fut>(
    payload: &[u8],
    write_type: WriteType,
    write: F,
) -> Result<(), E>
where
    F: FnOnce(Zeroizing<Vec<u8>>, WriteType) -> Fut,
    Fut: Future<Output = Result<(), E>>,
{
    write(Zeroizing::new(payload.to_vec()), write_type).await
}

fn validate_negotiated_payload(
    payload: &[u8],
    max_write_payload: usize,
) -> Result<(), TransportError> {
    if payload.len() > max_write_payload {
        return Err(TransportError::PayloadTooLarge {
            maximum: max_write_payload,
            actual: payload.len(),
        });
    }
    Ok(())
}

fn missing_characteristic(role: &str, uuid: Uuid, selector: &str) -> TransportError {
    TransportError::Io(io::Error::new(
        io::ErrorKind::Unsupported,
        format!("BLE target '{selector}' is missing Nordic UART {role} characteristic {uuid}"),
    ))
}

async fn with_ble_timeout<T, F>(
    timeout: Duration,
    future: F,
    operation: &str,
    selector: &str,
) -> Result<T, TransportError>
where
    F: Future<Output = btleplug::Result<T>>,
{
    with_ble_deadline(Instant::now() + timeout, future, operation, selector).await
}

async fn with_ble_deadline<T, F>(
    deadline: Instant,
    future: F,
    operation: &str,
    selector: &str,
) -> Result<T, TransportError>
where
    F: Future<Output = btleplug::Result<T>>,
{
    time::timeout_at(deadline, future)
        .await
        .map_err(|_elapsed| ble_timeout(operation, selector))?
        .map_err(|error| ble_io(&error, operation, selector))
}

async fn with_discovery_deadline<T, F>(
    deadline: Instant,
    timeout: Duration,
    cancellation: &CancellationToken,
    future: F,
    operation: &'static str,
) -> Result<T, DiscoveryError>
where
    F: Future<Output = btleplug::Result<T>>,
{
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(DiscoveryError::Cancelled),
        result = time::timeout_at(deadline, future) => {
            result
                .map_err(|_elapsed| DiscoveryError::BleTimeout { operation, timeout })?
                .map_err(|source| DiscoveryError::Ble { operation, source })
        }
    }
}

fn ble_timeout(operation: &str, selector: &str) -> TransportError {
    TransportError::Io(io::Error::new(
        io::ErrorKind::TimedOut,
        format!("BLE {operation} for '{selector}' timed out"),
    ))
}

fn ble_io(error: &btleplug::Error, operation: &str, selector: &str) -> TransportError {
    TransportError::Io(io::Error::other(format!(
        "BLE {operation} for '{selector}' failed: {error}; check Bluetooth permissions, radio state, range, and whether another application owns the device"
    )))
}

fn validate_scan_timeout(timeout: Duration) -> Result<(), DiscoveryError> {
    validate_timeout("scan_timeout", timeout).map_err(DiscoveryError::InvalidTarget)
}

enum ScanStopError {
    Provider(btleplug::Error),
    TimedOut,
}

fn discovery_scan_stop_error(error: ScanStopError, timeout: Duration) -> DiscoveryError {
    match error {
        ScanStopError::Provider(source) => DiscoveryError::Ble {
            operation: "scan stop",
            source,
        },
        ScanStopError::TimedOut => DiscoveryError::BleTimeout {
            operation: "scan stop",
            timeout,
        },
    }
}

fn transport_scan_stop_error(error: ScanStopError, selector: &str) -> TransportError {
    match error {
        ScanStopError::Provider(error) => ble_io(&error, "scan stop", selector),
        ScanStopError::TimedOut => ble_timeout("scan stop", selector),
    }
}

struct ActiveScans {
    adapters: Vec<Adapter>,
    stopped: bool,
}

impl ActiveScans {
    const fn new() -> Self {
        Self {
            adapters: Vec::new(),
            stopped: false,
        }
    }

    fn register(&mut self, adapter: Adapter) {
        self.adapters.push(adapter);
    }

    async fn stop(&mut self, timeout: Duration) -> Result<(), ScanStopError> {
        if self.adapters.is_empty() {
            self.stopped = true;
            return Ok(());
        }

        // Keep the originals in the guard while awaiting clones. If this future is cancelled, the
        // guard's Drop implementation still owns every active adapter and schedules cleanup.
        let adapters = self.adapters.clone();
        let stop_all = future::join_all(
            adapters
                .iter()
                .cloned()
                .map(|adapter| async move { adapter.stop_scan().await }),
        );
        let result = match time::timeout(timeout, stop_all).await {
            Ok(results) => results
                .into_iter()
                .find_map(Result::err)
                .map_or(Ok(()), |error| Err(ScanStopError::Provider(error))),
            Err(_elapsed) => Err(ScanStopError::TimedOut),
        };

        self.adapters.clear();
        self.stopped = true;
        if result.is_err() {
            schedule_best_effort_scan_stop(adapters);
        }
        result
    }
}

impl Drop for ActiveScans {
    fn drop(&mut self) {
        if self.stopped {
            return;
        }
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        schedule_best_effort_scan_stop_with_handle(&handle, self.adapters.drain(..));
    }
}

fn schedule_best_effort_scan_stop(adapters: Vec<Adapter>) {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        return;
    };
    schedule_best_effort_scan_stop_with_handle(&handle, adapters);
}

fn schedule_best_effort_scan_stop_with_handle(
    handle: &tokio::runtime::Handle,
    adapters: impl IntoIterator<Item = Adapter>,
) {
    for adapter in adapters {
        drop(handle.spawn(async move {
            drop(adapter.stop_scan().await);
        }));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nordic_uart_uuids_match_meshcore_contract() {
        assert_eq!(
            NORDIC_UART_SERVICE.to_string(),
            "6e400001-b5a3-f393-e0a9-e50e24dcca9e"
        );
        assert_eq!(
            NORDIC_UART_WRITE_CHARACTERISTIC.to_string(),
            "6e400002-b5a3-f393-e0a9-e50e24dcca9e"
        );
        assert_eq!(
            NORDIC_UART_NOTIFY_CHARACTERISTIC.to_string(),
            "6e400003-b5a3-f393-e0a9-e50e24dcca9e"
        );
    }

    #[test]
    fn write_with_response_is_preferred_for_reliability() {
        assert_eq!(
            preferred_write_type(CharPropFlags::WRITE | CharPropFlags::WRITE_WITHOUT_RESPONSE),
            Some(WriteType::WithResponse)
        );
        assert_eq!(
            preferred_write_type(CharPropFlags::WRITE),
            Some(WriteType::WithResponse)
        );
        assert_eq!(
            preferred_write_type(CharPropFlags::WRITE_WITHOUT_RESPONSE),
            Some(WriteType::WithoutResponse)
        );
        assert_eq!(preferred_write_type(CharPropFlags::READ), None);
    }

    #[test]
    fn negotiated_mtu_bounds_only_write_commands() {
        assert_eq!(negotiated_payload_limit(2, WriteType::WithoutResponse), 0);
        assert_eq!(negotiated_payload_limit(23, WriteType::WithoutResponse), 20);
        assert_eq!(
            negotiated_payload_limit(23, WriteType::WithResponse),
            MAX_INNER_PAYLOAD
        );
        assert_eq!(
            negotiated_payload_limit(517, WriteType::WithResponse),
            MAX_INNER_PAYLOAD
        );

        for length in [21, 50, MAX_INNER_PAYLOAD] {
            let payload = vec![0_u8; length];
            assert!(
                validate_negotiated_payload(
                    &payload,
                    negotiated_payload_limit(23, WriteType::WithResponse),
                )
                .is_ok()
            );
        }
        assert!(
            validate_negotiated_payload(
                &[0_u8; 20],
                negotiated_payload_limit(23, WriteType::WithoutResponse),
            )
            .is_ok()
        );
        let payload = vec![0_u8; 21];
        assert!(matches!(
            validate_negotiated_payload(
                &payload,
                negotiated_payload_limit(23, WriteType::WithoutResponse),
            ),
            Err(TransportError::PayloadTooLarge {
                maximum: 20,
                actual: 21,
            })
        ));
    }

    #[test]
    fn ble_metadata_is_actionable_and_reusable() {
        let record = ble_record(
            "platform-id".to_string(),
            "01:23:45:67:89:ab".to_string(),
            "MeshCore Field".to_string(),
            Some(-47),
            "host adapter",
        );
        assert_eq!(record.id, "ble:platform-id");
        assert_eq!(record.display_name, "MeshCore Field");
        assert_eq!(record.address.as_deref(), Some("01:23:45:67:89:ab"));
        assert_eq!(record.rssi, Some(-47));
        assert!(record.notes.iter().any(|note| note.contains("Nordic UART")));
        assert_eq!(
            record.target,
            TransportTarget::Ble {
                selector: "platform-id".to_string(),
            }
        );
        assert!(record.validate().is_ok());
    }

    #[test]
    fn constructor_accepts_discovery_id_and_rejects_invalid_inputs() {
        assert!(BleTransport::new("", Duration::from_secs(1)).is_err());
        assert!(BleTransport::new("device", Duration::ZERO).is_err());
        assert!(
            BleTransport::new(
                "device",
                meshquill_core::MAX_OPERATION_TIMEOUT.saturating_add(Duration::from_nanos(1)),
            )
            .is_err()
        );
        assert!(validate_scan_timeout(meshquill_core::MAX_OPERATION_TIMEOUT).is_ok());
        assert!(
            validate_scan_timeout(
                meshquill_core::MAX_OPERATION_TIMEOUT.saturating_add(Duration::from_nanos(1)),
            )
            .is_err()
        );
        let transport =
            BleTransport::new("ble:platform-id", Duration::from_secs(2)).expect("valid BLE target");
        assert_eq!(transport.selector(), "ble:platform-id");
        assert_eq!(
            transport.target(),
            TransportTarget::Ble {
                selector: "ble:platform-id".to_string(),
            }
        );
    }

    #[test]
    fn oversized_ble_packet_is_rejected_before_provider_io() {
        let payload = vec![0_u8; MAX_INNER_PAYLOAD + 1];
        assert!(matches!(
            validate_payload(&payload),
            Err(TransportError::PayloadTooLarge {
                maximum: MAX_INNER_PAYLOAD,
                actual,
            }) if actual == MAX_INNER_PAYLOAD + 1
        ));
    }

    #[tokio::test]
    async fn provider_write_submits_one_complete_frame_without_chunking_or_replay() {
        use std::sync::{Arc, Mutex};

        let calls = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&calls);
        let payload = vec![0xa5_u8; MAX_INNER_PAYLOAD];
        provider_write_once(
            &payload,
            WriteType::WithResponse,
            move |packet, write_type| {
                let observed = Arc::clone(&observed);
                async move {
                    observed
                        .lock()
                        .expect("write observation mutex")
                        .push((packet, write_type));
                    Ok::<(), io::Error>(())
                }
            },
        )
        .await
        .expect("provider write");

        let calls = calls.lock().expect("write observation mutex");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0.as_slice(), payload);
        assert_eq!(calls[0].1, WriteType::WithResponse);
    }

    #[tokio::test]
    async fn provider_write_failure_is_returned_after_one_attempt() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };

        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let result = provider_write_once(
            &[0x5a; 50],
            WriteType::WithResponse,
            move |_packet, _write_type| async move {
                observed.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "injected failure",
                ))
            },
        )
        .await;

        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn whole_disconnect_cleanup_has_one_deadline() {
        let cleanup = std::future::pending::<Result<(), TransportError>>();
        let error = with_ble_cleanup_timeout(Duration::ZERO, cleanup, "test-device")
            .await
            .expect_err("a pending cleanup must respect the whole-operation deadline");

        match error {
            TransportError::Io(error) => {
                assert_eq!(error.kind(), io::ErrorKind::TimedOut);
                assert!(error.to_string().contains("disconnect cleanup"));
                assert!(error.to_string().contains("test-device"));
            }
            other => panic!("unexpected cleanup timeout error: {other}"),
        }
    }

    #[tokio::test]
    async fn discovery_provider_deadline_is_bounded_and_cancellable() {
        let cancellation = CancellationToken::new();
        let error = with_discovery_deadline(
            Instant::now(),
            Duration::from_millis(25),
            &cancellation,
            std::future::pending::<btleplug::Result<()>>(),
            "test provider operation",
        )
        .await
        .expect_err("a pending provider call must respect its phase deadline");
        assert!(matches!(
            error,
            DiscoveryError::BleTimeout {
                operation: "test provider operation",
                timeout
            } if timeout == Duration::from_millis(25)
        ));

        cancellation.cancel();
        let error = with_discovery_deadline(
            Instant::now(),
            Duration::from_millis(25),
            &cancellation,
            std::future::pending::<btleplug::Result<()>>(),
            "test provider operation",
        )
        .await
        .expect_err("explicit cancellation must win over an elapsed provider deadline");
        assert!(matches!(error, DiscoveryError::Cancelled));
    }
}

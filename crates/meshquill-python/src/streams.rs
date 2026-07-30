use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex as StdMutex, MutexGuard as StdMutexGuard};

use meshquill_core::Event;
use pyo3::exceptions::PyStopAsyncIteration;
use pyo3::prelude::*;
use tokio::sync::{Mutex, broadcast, watch};

use crate::errors::StreamLaggedError;
use crate::models::{PyEvent, PyMessage};

const EVENT_REPLAY_CAPACITY: usize = 256;
const EVENT_LIVE_CAPACITY: usize = 256;

#[derive(Clone)]
enum HubItem {
    Event(Event),
    Lagged(u64),
}

#[derive(Clone)]
struct HubRecord {
    sequence: u64,
    item: HubItem,
}

struct HubState {
    next_sequence: u64,
    capturing_initial: bool,
    initial_dropped: u64,
    initial: VecDeque<HubRecord>,
    recent: VecDeque<HubRecord>,
}

impl HubState {
    fn new() -> Self {
        Self {
            next_sequence: 0,
            capturing_initial: true,
            initial_dropped: 0,
            initial: VecDeque::with_capacity(EVENT_REPLAY_CAPACITY),
            recent: VecDeque::with_capacity(EVENT_REPLAY_CAPACITY),
        }
    }

    fn record(&mut self, item: HubItem) -> HubRecord {
        let completes_initial = matches!(&item, HubItem::Event(Event::Connected));
        let record = HubRecord {
            sequence: self.next_sequence,
            item,
        };
        self.next_sequence = self.next_sequence.saturating_add(1);

        if self.capturing_initial {
            if self.initial.len() == EVENT_REPLAY_CAPACITY {
                let _ = self.initial.pop_front();
                self.initial_dropped = self.initial_dropped.saturating_add(1);
            }
            self.initial.push_back(record.clone());
        }

        if self.recent.len() == EVENT_REPLAY_CAPACITY {
            let _ = self.recent.pop_front();
        }
        self.recent.push_back(record.clone());

        if completes_initial {
            self.capturing_initial = false;
        }
        record
    }

    fn snapshot(&self) -> VecDeque<HubRecord> {
        let mut records = BTreeMap::new();
        for record in self.initial.iter().chain(&self.recent) {
            records.insert(record.sequence, record.clone());
        }
        records.into_values().collect()
    }
}

struct EventHubInner {
    state: StdMutex<HubState>,
    live_tx: broadcast::Sender<HubRecord>,
}

impl EventHubInner {
    fn publish(&self, item: HubItem) {
        // Publishing and subscription registration use the same short synchronous lock. A new
        // receiver therefore starts strictly after its replay snapshot, with no gap or overlap.
        let mut state = lock_state(&self.state);
        let record = state.record(item);
        let _ = self.live_tx.send(record);
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum RelayStatus {
    Pending,
    InitialComplete,
    Stopped,
}

/// Bounded replay/live relay owned by a Python client session.
pub(crate) struct EventHub {
    inner: Arc<EventHubInner>,
    relay_status: watch::Receiver<RelayStatus>,
}

impl EventHub {
    /// Start relaying an already-registered core receiver.
    pub(crate) fn spawn(
        upstream: broadcast::Receiver<Event>,
        closed: watch::Receiver<bool>,
    ) -> Self {
        let (live_tx, _) = broadcast::channel(EVENT_LIVE_CAPACITY);
        let inner = Arc::new(EventHubInner {
            state: StdMutex::new(HubState::new()),
            live_tx,
        });
        let (relay_status_tx, relay_status) = watch::channel(RelayStatus::Pending);
        std::mem::drop(tokio::spawn(relay_events(
            Arc::clone(&inner),
            upstream,
            closed,
            relay_status_tx,
        )));
        Self {
            inner,
            relay_status,
        }
    }

    /// Wait until the relay has retained every event preceding the initial `Connected` marker.
    pub(crate) async fn wait_for_initial_replay(&self) -> Result<(), &'static str> {
        let mut status = self.relay_status.clone();
        loop {
            let current = *status.borrow_and_update();
            match current {
                RelayStatus::InitialComplete => return Ok(()),
                RelayStatus::Stopped => return Err("the event relay stopped during the handshake"),
                RelayStatus::Pending => {}
            }
            status
                .changed()
                .await
                .map_err(|_| "the event relay stopped during the handshake")?;
        }
    }

    /// Atomically pair a bounded replay snapshot with its gap-free live receiver.
    pub(crate) fn subscribe(&self) -> EventSubscription {
        let state = lock_state(&self.inner.state);
        let receiver = self.inner.live_tx.subscribe();
        let replay = state.snapshot();
        let initial_lag = (state.initial_dropped > 0).then_some(state.initial_dropped);
        EventSubscription {
            initial_lag,
            replay,
            receiver,
        }
    }
}

async fn relay_events(
    inner: Arc<EventHubInner>,
    mut upstream: broadcast::Receiver<Event>,
    mut closed: watch::Receiver<bool>,
    relay_status: watch::Sender<RelayStatus>,
) {
    loop {
        if *closed.borrow() {
            break;
        }
        tokio::select! {
            biased;
            _ = closed.changed() => break,
            result = upstream.recv() => match result {
                Ok(event) => {
                    let completes_initial = matches!(&event, Event::Connected);
                    inner.publish(HubItem::Event(event));
                    if completes_initial {
                        let _ = relay_status.send_replace(RelayStatus::InitialComplete);
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    inner.publish(HubItem::Lagged(skipped));
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    }

    let stopped_during_initial = { *relay_status.borrow() == RelayStatus::Pending };
    if stopped_during_initial {
        let _ = relay_status.send_replace(RelayStatus::Stopped);
    }
}

fn lock_state(state: &StdMutex<HubState>) -> StdMutexGuard<'_, HubState> {
    match state.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

pub(crate) struct EventSubscription {
    initial_lag: Option<u64>,
    replay: VecDeque<HubRecord>,
    receiver: broadcast::Receiver<HubRecord>,
}

impl EventSubscription {
    pub(crate) async fn recv(&mut self) -> Result<Event, HubReceiveError> {
        if let Some(skipped) = self.initial_lag.take() {
            return Err(HubReceiveError::Lagged(skipped));
        }
        if let Some(record) = self.replay.pop_front() {
            return record.into_event();
        }
        match self.receiver.recv().await {
            Ok(record) => record.into_event(),
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                Err(HubReceiveError::Lagged(skipped))
            }
            Err(broadcast::error::RecvError::Closed) => Err(HubReceiveError::Closed),
        }
    }
}

impl HubRecord {
    fn into_event(self) -> Result<Event, HubReceiveError> {
        match self.item {
            HubItem::Event(event) => Ok(event),
            HubItem::Lagged(skipped) => Err(HubReceiveError::Lagged(skipped)),
        }
    }
}

pub(crate) enum HubReceiveError {
    Lagged(u64),
    Closed,
}

type SharedSubscription = Arc<Mutex<EventSubscription>>;

/// An independent asynchronous iterator over all client events.
#[pyclass(name = "EventStream", module = "meshcore_sdk._native")]
pub(crate) struct PyEventStream {
    subscription: SharedSubscription,
    closed: watch::Receiver<bool>,
}

impl PyEventStream {
    pub(crate) fn new(subscription: EventSubscription, closed: watch::Receiver<bool>) -> Self {
        Self {
            subscription: Arc::new(Mutex::new(subscription)),
            closed,
        }
    }
}

#[pymethods]
impl PyEventStream {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let subscription = Arc::clone(&self.subscription);
        let mut closed = self.closed.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if *closed.borrow() {
                return Err(PyStopAsyncIteration::new_err(()));
            }
            let mut subscription = subscription.lock().await;
            let event = tokio::select! {
                biased;
                changed = closed.changed() => {
                    let _ = changed;
                    return Err(PyStopAsyncIteration::new_err(()));
                }
                event = subscription.recv() => receive_event(event)?,
            };
            Ok(PyEvent { event })
        })
    }
}

/// An independent asynchronous iterator filtered to inbound messages.
#[pyclass(name = "MessageStream", module = "meshcore_sdk._native")]
pub(crate) struct PyMessageStream {
    subscription: SharedSubscription,
    closed: watch::Receiver<bool>,
}

impl PyMessageStream {
    pub(crate) fn new(subscription: EventSubscription, closed: watch::Receiver<bool>) -> Self {
        Self {
            subscription: Arc::new(Mutex::new(subscription)),
            closed,
        }
    }
}

#[pymethods]
impl PyMessageStream {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let subscription = Arc::clone(&self.subscription);
        let mut closed = self.closed.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if *closed.borrow() {
                return Err(PyStopAsyncIteration::new_err(()));
            }
            let mut subscription = subscription.lock().await;
            loop {
                let event = tokio::select! {
                    biased;
                    changed = closed.changed() => {
                        let _ = changed;
                        return Err(PyStopAsyncIteration::new_err(()));
                    }
                    event = subscription.recv() => receive_event(event)?,
                };
                if let Event::Message(message) = event {
                    return Ok(PyMessage::from(message));
                }
            }
        })
    }
}

fn receive_event(result: Result<Event, HubReceiveError>) -> PyResult<Event> {
    match result {
        Ok(event) => Ok(event),
        Err(HubReceiveError::Lagged(skipped)) => Err(StreamLaggedError::new_err(format!(
            "event subscriber lagged and lost {skipped} event(s); create a fresh stream or consume this one faster"
        ))),
        Err(HubReceiveError::Closed) => Err(PyStopAsyncIteration::new_err(())),
    }
}

pub(crate) fn add_classes(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyEventStream>()?;
    module.add_class::<PyMessageStream>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn replay_and_live_registration_has_no_gap_or_duplicate() {
        let (source, upstream) = broadcast::channel(32);
        let (closed, closed_rx) = watch::channel(false);
        let hub = EventHub::spawn(upstream, closed_rx);

        source
            .send(Event::MessagesWaiting)
            .expect("initial event receiver");
        source.send(Event::Connected).expect("connected receiver");
        hub.wait_for_initial_replay().await.expect("initial replay");

        let mut subscription = hub.subscribe();
        assert!(matches!(
            subscription.recv().await,
            Ok(Event::MessagesWaiting)
        ));
        assert!(matches!(subscription.recv().await, Ok(Event::Connected)));

        source
            .send(Event::CurrentTime(7))
            .expect("live event receiver");
        assert!(matches!(
            subscription.recv().await,
            Ok(Event::CurrentTime(7))
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(10), subscription.recv())
                .await
                .is_err()
        );
        let _ = closed.send_replace(true);
    }

    #[tokio::test]
    async fn live_overflow_remains_an_explicit_nonterminal_lag_error() {
        let (source, upstream) = broadcast::channel(512);
        let (closed, closed_rx) = watch::channel(false);
        let hub = EventHub::spawn(upstream, closed_rx);
        source.send(Event::Connected).expect("connected receiver");
        hub.wait_for_initial_replay().await.expect("initial replay");

        let mut subscription = hub.subscribe();
        assert!(matches!(subscription.recv().await, Ok(Event::Connected)));
        for value in 0..300 {
            source
                .send(Event::CurrentTime(value))
                .expect("live event receiver");
        }
        loop {
            let relayed = { lock_state(&hub.inner.state).next_sequence };
            if relayed >= 301 {
                break;
            }
            tokio::task::yield_now().await;
        }

        assert!(matches!(
            subscription.recv().await,
            Err(HubReceiveError::Lagged(_))
        ));
        assert!(matches!(
            subscription.recv().await,
            Ok(Event::CurrentTime(_))
        ));
        let _ = closed.send_replace(true);
    }
}

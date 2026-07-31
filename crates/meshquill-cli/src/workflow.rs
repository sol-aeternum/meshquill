use std::{collections::VecDeque, ops::Deref, sync::Arc};

use crate::config::SelectedProfile;
use meshquill_core::{ManagedClient, Message, SelfInfo, domain::MessageSource};
use meshquill_hooks::{
    AfterSendPayload, BeforeSendInput, ContactChange, HookEvent, HookRuntime, OnAckPayload,
    OnConnectPayload, OnContactUpdatePayload, OnDisconnectPayload, OnErrorPayload,
    OnMessagePayload, OnTimeoutPayload,
};
use meshquill_store::{
    HistoryDirection, HistoryEntry, HistoryStatus, HistoryStore, TransportConfig,
};
use tokio::{sync::Mutex, task};

use crate::error::CliError;
use crate::output::ExitStatus;

pub(crate) struct WorkflowServices {
    hook_runtime: Option<HookRuntime>,
    history_store: Option<Arc<HistoryStore>>,
    incoming_tracker: Arc<Mutex<IncomingTracker>>,
    transport_kind: &'static str,
    profile_name: String,
}

/// One ordinary, non-streaming companion connection with a balanced hook lifecycle.
///
/// Streaming and reconnecting commands keep their explicit state machines; this wrapper is for
/// commands whose device work fits inside one successful handshake and one terminal result.
pub(crate) struct CompanionSession {
    client: ManagedClient,
    workflow: WorkflowServices,
    operation: &'static str,
}

const INCOMING_CORRELATION_CAPACITY: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IncomingOrigin {
    Live,
    Queue,
}

#[derive(Debug)]
struct IncomingOccurrence {
    observation_id: u64,
    origin: IncomingOrigin,
}

#[derive(Debug, Default)]
struct IncomingTracker {
    occurrences: VecDeque<IncomingOccurrence>,
}

impl IncomingTracker {
    fn observe(&mut self, observation_id: Option<u64>, origin: IncomingOrigin) -> bool {
        let Some(observation_id) = observation_id else {
            return true;
        };
        if let Some(position) = self.occurrences.iter().position(|occurrence| {
            occurrence.observation_id == observation_id && occurrence.origin != origin
        }) {
            self.occurrences.remove(position);
            return false;
        }

        if self.occurrences.len() == INCOMING_CORRELATION_CAPACITY {
            self.occurrences.pop_front();
        }
        self.occurrences.push_back(IncomingOccurrence {
            observation_id,
            origin,
        });
        true
    }

    fn clear(&mut self) {
        self.occurrences.clear();
    }
}

#[derive(Debug)]
pub(crate) struct PreparedSend {
    pub(crate) destination: String,
    pub(crate) text: String,
}

#[derive(Debug)]
pub(crate) struct OutgoingRecord {
    message_id: uuid::Uuid,
    history_message_id: Option<uuid::Uuid>,
}

impl OutgoingRecord {
    pub(crate) fn message_id(&self) -> uuid::Uuid {
        self.message_id
    }

    pub(crate) fn history_message_id(&self) -> Option<uuid::Uuid> {
        self.history_message_id
    }
}

impl CompanionSession {
    /// Initialize workflow services, complete the handshake, and then emit `on_connect`.
    pub(crate) async fn connect(
        selected: &SelectedProfile,
        client: ManagedClient,
        operation: &'static str,
    ) -> Result<(Self, SelfInfo), CliError> {
        let workflow = WorkflowServices::from_selected(selected)?;
        let info = match client.connect().await {
            Ok(info) => info,
            Err(error) => {
                let _ = client.shutdown().await;
                return Err(CliError::from(error));
            }
        };

        if let Err(primary) = workflow.connected(&info.name).await {
            if workflow
                .disconnected(Some("connection hook failed"))
                .await
                .is_err()
            {
                tracing::warn!("secondary disconnect hook failure; details omitted");
            }
            if client.shutdown().await.is_err() {
                tracing::warn!("secondary client shutdown failure; details omitted");
            }
            return Err(primary);
        }

        Ok((
            Self {
                client,
                workflow,
                operation,
            },
            info,
        ))
    }

    pub(crate) const fn client(&self) -> &ManagedClient {
        &self.client
    }

    pub(crate) const fn workflow(&self) -> &WorkflowServices {
        &self.workflow
    }

    /// Finish a post-connect result without allowing hook or shutdown cleanup to mask it.
    pub(crate) async fn finish<T>(&self, operation: Result<T, CliError>) -> Result<T, CliError> {
        let mut primary = operation;

        if let Err(error) = &primary {
            self.emit_primary_error(error).await;
        }

        let shutdown = self.client.shutdown().await.map_err(CliError::from);
        match (&primary, shutdown) {
            (Err(_), Err(_)) => {
                tracing::warn!("secondary client shutdown failure; details omitted");
            }
            (Ok(_), Err(error)) => {
                self.emit_primary_error(&error).await;
                primary = Err(error);
            }
            (_, Ok(())) => {}
        }

        let reason = match &primary {
            Ok(_) => "command completed",
            Err(error) if error.status() == ExitStatus::Interrupted => "command interrupted",
            Err(_) => "command failed",
        };
        let disconnected = self.workflow.disconnected(Some(reason)).await;

        match primary {
            Err(error) => {
                if disconnected.is_err() {
                    tracing::warn!("secondary disconnect hook failure; details omitted");
                }
                Err(error)
            }
            Ok(value) => {
                disconnected?;
                Ok(value)
            }
        }
    }

    async fn emit_primary_error(&self, error: &CliError) {
        if should_emit_error(error.status())
            && self
                .workflow
                .error(self.operation, error.message())
                .await
                .is_err()
        {
            tracing::warn!("secondary error hook failure; details omitted");
        }
    }
}

impl Deref for CompanionSession {
    type Target = ManagedClient;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

const fn should_emit_error(status: ExitStatus) -> bool {
    !matches!(
        status,
        ExitStatus::Success | ExitStatus::Hook | ExitStatus::Mqtt | ExitStatus::Interrupted
    )
}

impl WorkflowServices {
    pub(crate) fn from_selected(selected: &SelectedProfile) -> Result<Self, CliError> {
        let hook_runtime = selected
            .config
            .hook
            .runtime_config()
            .map_err(CliError::from)?
            .map(|config| HookRuntime::new(config).map_err(CliError::from))
            .transpose()?;
        let history_store = if selected.config.history.enabled {
            Some(Arc::new(selected.history_store()?))
        } else {
            None
        };

        Ok(Self {
            hook_runtime,
            history_store,
            incoming_tracker: Arc::new(Mutex::new(IncomingTracker::default())),
            transport_kind: transport_kind(&selected.profile.transport),
            profile_name: selected.name.clone(),
        })
    }

    pub(crate) async fn prepare_send(
        &self,
        destination: String,
        text: String,
    ) -> Result<PreparedSend, CliError> {
        let Some(runtime) = &self.hook_runtime else {
            return Ok(PreparedSend { destination, text });
        };

        let input = BeforeSendInput {
            destination: destination.clone(),
            text: text.clone(),
        };
        let outcome = runtime.before_send(input).await.map_err(CliError::from)?;
        match outcome.require_allowed().map_err(CliError::from)? {
            meshquill_hooks::BeforeSendOutcome::Modify { destination, text } => {
                Ok(PreparedSend { destination, text })
            }
            meshquill_hooks::BeforeSendOutcome::Allow => Ok(PreparedSend { destination, text }),
            meshquill_hooks::BeforeSendOutcome::Reject { .. } => Err(CliError::new(
                ExitStatus::Protocol,
                "hook rejection was not converted into a typed hook error",
            )),
        }
    }

    pub(crate) async fn load_history(&self) -> Result<Option<Vec<HistoryEntry>>, CliError> {
        let Some(store) = &self.history_store else {
            return Ok(None);
        };
        load_history(store).await.map(Some)
    }

    pub(crate) async fn connected(&self, peer: &str) -> Result<(), CliError> {
        self.incoming_tracker.lock().await.clear();
        let peer = if peer.is_empty() {
            Some(self.profile_name.clone())
        } else {
            Some(peer.to_owned())
        };
        if let Some(runtime) = &self.hook_runtime {
            runtime
                .dispatch(HookEvent::OnConnect(OnConnectPayload {
                    transport: self.transport_kind.to_owned(),
                    peer,
                }))
                .await
                .map_err(CliError::from)?;
        }
        Ok(())
    }

    pub(crate) async fn disconnected(&self, reason: Option<&str>) -> Result<(), CliError> {
        if let Some(runtime) = &self.hook_runtime {
            runtime
                .dispatch(HookEvent::OnDisconnect(OnDisconnectPayload {
                    transport: self.transport_kind.to_owned(),
                    reason: reason.map(str::to_owned),
                }))
                .await
                .map_err(CliError::from)?;
        }
        Ok(())
    }

    pub(crate) async fn begin_outgoing(
        &self,
        peer: &str,
        channel: Option<u8>,
        text: &str,
    ) -> Result<OutgoingRecord, CliError> {
        let message_id = uuid::Uuid::now_v7();
        let history_message_id = if let Some(store) = &self.history_store {
            let mut entry = HistoryEntry::new(
                HistoryDirection::Outgoing,
                peer,
                channel,
                text,
                HistoryStatus::Pending,
                None,
            )
            .map_err(CliError::from)?;
            entry.id = message_id;
            store_upsert(store, &entry).await?;
            Some(message_id)
        } else {
            None
        };

        Ok(OutgoingRecord {
            message_id,
            history_message_id,
        })
    }

    pub(crate) async fn sent(
        &self,
        record: &mut OutgoingRecord,
        destination: &str,
        text: &str,
        message_id: &str,
        ack_code: Option<[u8; 4]>,
    ) -> Result<(), CliError> {
        if let Some(store) = &self.history_store
            && let Some(history_id) = record.history_message_id()
            && let Some(mut entry) = load_history_entry(store, history_id).await?
        {
            entry.peer = destination.to_owned();
            entry.acknowledgement = ack_code;
            store_upsert(store, &entry).await?;
        }

        if let Some(runtime) = &self.hook_runtime {
            runtime
                .dispatch(HookEvent::AfterSend(AfterSendPayload {
                    destination: destination.to_owned(),
                    text: text.to_owned(),
                    message_id: Some(message_id.to_owned()),
                }))
                .await
                .map_err(CliError::from)?;
        }

        Ok(())
    }

    pub(crate) async fn acknowledged(
        &self,
        record: &mut OutgoingRecord,
        message_id: &str,
        source: Option<&str>,
        round_trip_ms: Option<u32>,
        ack_code: [u8; 4],
    ) -> Result<(), CliError> {
        if let Some(store) = &self.history_store
            && let Some(history_id) = record.history_message_id()
            && let Some(mut entry) = load_history_entry(store, history_id).await?
        {
            if entry.acknowledgement != Some(ack_code) {
                return Err(CliError::new(
                    ExitStatus::Protocol,
                    "outgoing message acknowledgement did not match stored correlation",
                ));
            }
            entry.status = HistoryStatus::Acknowledged;
            store_upsert(store, &entry).await?;
        }

        if let Some(runtime) = &self.hook_runtime {
            runtime
                .dispatch(HookEvent::OnAck(OnAckPayload {
                    message_id: message_id.to_owned(),
                    source: source.map(str::to_owned),
                    round_trip_ms: round_trip_ms.map(u64::from),
                }))
                .await
                .map_err(CliError::from)?;
        }

        Ok(())
    }

    pub(crate) async fn timed_out(
        &self,
        record: &mut OutgoingRecord,
        operation: &str,
        message_id: &str,
    ) -> Result<(), CliError> {
        if let Some(store) = &self.history_store
            && let Some(history_id) = record.history_message_id()
            && let Some(mut entry) = load_history_entry(store, history_id).await?
        {
            entry.status = HistoryStatus::TimedOut;
            store_upsert(store, &entry).await?;
        }

        if let Some(runtime) = &self.hook_runtime {
            runtime
                .dispatch(HookEvent::OnTimeout(OnTimeoutPayload {
                    operation: operation.to_owned(),
                    message_id: Some(message_id.to_owned()),
                }))
                .await
                .map_err(CliError::from)?;
        }

        Ok(())
    }

    pub(crate) async fn failed(&self, record: &mut OutgoingRecord) -> Result<(), CliError> {
        if let Some(store) = &self.history_store
            && let Some(history_id) = record.history_message_id()
            && let Some(mut entry) = load_history_entry(store, history_id).await?
        {
            entry.status = HistoryStatus::Failed;
            store_upsert(store, &entry).await?;
        }

        Ok(())
    }

    /// Records one incoming-message observation unless it is the exact broadcast/return clone
    /// already consumed through the opposite path.
    ///
    /// Correlation uses the ephemeral observation ID assigned by the core client to clones of one
    /// decoded packet. Payload equality is never used, so distinct identical messages remain
    /// distinct. Parser-only values without an observation ID are always accepted.
    pub(crate) async fn incoming(
        &self,
        message: &Message,
        origin: IncomingOrigin,
    ) -> Result<Option<String>, CliError> {
        let is_new = self
            .incoming_tracker
            .lock()
            .await
            .observe(message.observation_id, origin);
        if !is_new {
            return Ok(None);
        }

        let id = uuid::Uuid::now_v7().to_string();
        if let Some(store) = &self.history_store {
            let peer = message_source(message);
            let channel = match message.source {
                MessageSource::Direct { .. } => None,
                MessageSource::Channel { channel_idx } => Some(channel_idx),
            };
            let mut entry = HistoryEntry::new(
                HistoryDirection::Incoming,
                &peer,
                channel,
                &message.text,
                HistoryStatus::Received,
                None,
            )
            .map_err(CliError::from)?;
            entry.id = uuid::Uuid::parse_str(&id).map_err(|_| {
                CliError::new(ExitStatus::Protocol, "failed to prepare message identifier")
            })?;
            store_upsert(store, &entry).await?;
        }

        if let Some(runtime) = &self.hook_runtime {
            runtime
                .dispatch(HookEvent::OnMessage(OnMessagePayload {
                    source: message_source(message),
                    text: message.text.clone(),
                    message_id: Some(id.clone()),
                }))
                .await
                .map_err(CliError::from)?;
        }

        Ok(Some(id))
    }

    pub(crate) async fn contact_updated(
        &self,
        contact_id: String,
        display_name: Option<String>,
        change: ContactChange,
    ) -> Result<(), CliError> {
        if let Some(runtime) = &self.hook_runtime {
            runtime
                .dispatch(HookEvent::OnContactUpdate(OnContactUpdatePayload {
                    contact_id,
                    display_name,
                    change,
                }))
                .await
                .map_err(CliError::from)?;
        }
        Ok(())
    }

    pub(crate) async fn error(&self, operation: &str, message: &str) -> Result<(), CliError> {
        if let Some(runtime) = &self.hook_runtime {
            runtime
                .dispatch(HookEvent::OnError(OnErrorPayload {
                    operation: operation.to_owned(),
                    message: message.to_owned(),
                }))
                .await
                .map_err(CliError::from)?;
        }
        Ok(())
    }
}

fn transport_kind(transport: &TransportConfig) -> &'static str {
    match transport {
        TransportConfig::Ble { .. } => "ble",
        TransportConfig::Serial { .. } => "serial",
        TransportConfig::Tcp { .. } => "tcp",
        TransportConfig::Mock { .. } => "mock",
    }
}

fn message_source(message: &Message) -> String {
    match &message.source {
        MessageSource::Direct { pubkey_prefix } => format!("direct:{pubkey_prefix}"),
        MessageSource::Channel { channel_idx } => format!("channel:{channel_idx}"),
    }
}

async fn load_history_entry(
    store: &Arc<HistoryStore>,
    id: uuid::Uuid,
) -> Result<Option<HistoryEntry>, CliError> {
    let entries = load_history(store).await?;
    Ok(entries.into_iter().find(|entry| entry.id == id))
}

pub(crate) async fn load_history(store: &Arc<HistoryStore>) -> Result<Vec<HistoryEntry>, CliError> {
    let store = Arc::clone(store);
    task::spawn_blocking(move || store.load())
        .await
        .map_err(|_| {
            CliError::new(
                ExitStatus::Configuration,
                "workflow history storage task was interrupted",
            )
        })?
        .map_err(CliError::from)
}

pub(crate) async fn clear_history(store: &Arc<HistoryStore>) -> Result<(), CliError> {
    let store = Arc::clone(store);
    task::spawn_blocking(move || store.clear())
        .await
        .map_err(|_| {
            CliError::new(
                ExitStatus::Configuration,
                "workflow history cleanup task was interrupted",
            )
        })?
        .map_err(CliError::from)
}

async fn store_upsert(store: &Arc<HistoryStore>, entry: &HistoryEntry) -> Result<(), CliError> {
    let store = Arc::clone(store);
    let entry = entry.clone();
    task::spawn_blocking(move || store.upsert(&entry))
        .await
        .map_err(|_| {
            CliError::new(
                ExitStatus::Configuration,
                "workflow history storage task was interrupted",
            )
        })?
        .map_err(CliError::from)
}

pub(crate) fn history_store_for_selected(
    selected: &SelectedProfile,
    include_disabled: bool,
) -> Result<Option<Arc<HistoryStore>>, CliError> {
    if !selected.config.history.enabled && !include_disabled {
        return Ok(None);
    }

    selected.history_store().map(Arc::new).map(Some)
}

#[cfg(test)]
mod tests {
    use meshquill_core::{MessageRoute, MessageStatus};
    use meshquill_store::{
        Config, DeviceProfile, HistoryDirection, HistoryStatus, TransportConfig,
    };
    use tempfile::tempdir;

    use super::{
        INCOMING_CORRELATION_CAPACITY, IncomingOrigin, IncomingTracker, WorkflowServices,
        history_store_for_selected,
    };
    use crate::config::SelectedProfile;

    fn selected_profile(history_enabled: bool) -> (tempfile::TempDir, SelectedProfile) {
        let dir = tempdir().expect("temporary directory");
        let mut config = Config::default();
        config.history.enabled = history_enabled;

        let profile = DeviceProfile {
            transport: TransportConfig::Mock {
                scenario: "demo".to_owned(),
            },
            transport_overrides: None,
            secret: None,
        };

        let selected = SelectedProfile {
            config,
            name: "unit_profile".to_owned(),
            profile,
            path: dir.path().join("config.toml"),
            needs_migration: false,
            data_dir: Some(dir.path().join("data")),
            namespaced_history: true,
        };

        (dir, selected)
    }

    #[tokio::test]
    async fn prepare_send_with_hooks_and_history_disabled_is_side_effect_free() {
        let (dir, selected) = selected_profile(false);
        let services = WorkflowServices::from_selected(&selected).expect("service");

        let prepared = services
            .prepare_send("alice".to_owned(), "ping".to_owned())
            .await
            .expect("prepare send");
        assert_eq!(prepared.destination, "alice");
        assert_eq!(prepared.text, "ping");
        assert!(
            services
                .load_history()
                .await
                .expect("disabled history query")
                .is_none()
        );

        assert!(dir.path().read_dir().expect("read dir").next().is_none());
    }

    #[tokio::test]
    async fn repeated_same_origin_messages_are_preserved_as_distinct_occurrences() {
        let (_dir, selected) = selected_profile(true);
        let services = WorkflowServices::from_selected(&selected).expect("service");

        let message = meshquill_core::Message {
            observation_id: Some(1),
            source: meshquill_core::domain::MessageSource::Direct {
                pubkey_prefix: "peer123".to_owned(),
            },
            route: MessageRoute::Direct,
            txt_type: 0,
            sender_timestamp: 9,
            signature: Some([0xde, 0xad, 0xbe, 0xef]),
            text: "hello".to_owned(),
            snr: None,
            status: MessageStatus::Received,
        };

        let first = services
            .incoming(&message, IncomingOrigin::Live)
            .await
            .expect("first incoming")
            .expect("first occurrence");
        let second = services
            .incoming(&message, IncomingOrigin::Live)
            .await
            .expect("second incoming")
            .expect("second occurrence");

        let store = history_store_for_selected(&selected, true)
            .expect("history configuration")
            .expect("history");
        let entries = store.load().expect("load history");

        assert_ne!(first, second);
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().all(|entry| {
            entry.direction == HistoryDirection::Incoming && entry.id.as_bytes()[6] >> 4 == 7
        }));
    }

    #[tokio::test]
    async fn live_and_queued_views_of_one_occurrence_are_correlated_once() {
        let (_dir, selected) = selected_profile(true);
        let services = WorkflowServices::from_selected(&selected).expect("service");
        let direct = meshquill_core::Message {
            observation_id: Some(1),
            source: meshquill_core::domain::MessageSource::Direct {
                pubkey_prefix: "peer123".to_owned(),
            },
            route: MessageRoute::Direct,
            txt_type: 0,
            sender_timestamp: 9,
            signature: Some([0xde, 0xad, 0xbe, 0xef]),
            text: "hello".to_owned(),
            snr: None,
            status: MessageStatus::Received,
        };
        let rerouted = meshquill_core::Message {
            route: MessageRoute::Path {
                hash_mode: 1,
                hop_count: 3,
            },
            snr: Some(-2.5),
            ..direct.clone()
        };

        let first = services
            .incoming(&direct, IncomingOrigin::Live)
            .await
            .expect("first incoming");
        let second = services
            .incoming(&rerouted, IncomingOrigin::Queue)
            .await
            .expect("rerouted incoming");
        assert!(first.is_some());
        assert!(second.is_none());

        let store = history_store_for_selected(&selected, true)
            .expect("history configuration")
            .expect("history");
        assert_eq!(store.load().expect("load history").len(), 1);
    }

    #[tokio::test]
    async fn identical_payloads_with_distinct_observation_ids_are_both_preserved() {
        let (_dir, selected) = selected_profile(true);
        let services = WorkflowServices::from_selected(&selected).expect("service");
        let live = meshquill_core::Message {
            observation_id: Some(10),
            source: meshquill_core::domain::MessageSource::Channel { channel_idx: 1 },
            route: MessageRoute::Direct,
            txt_type: 0,
            sender_timestamp: 9,
            signature: None,
            text: "same text".to_owned(),
            snr: None,
            status: MessageStatus::Received,
        };
        let queued = meshquill_core::Message {
            observation_id: Some(11),
            ..live.clone()
        };

        assert!(
            services
                .incoming(&live, IncomingOrigin::Live)
                .await
                .expect("live message")
                .is_some()
        );
        assert!(
            services
                .incoming(&queued, IncomingOrigin::Queue)
                .await
                .expect("queued message")
                .is_some()
        );

        let store = history_store_for_selected(&selected, true)
            .expect("history configuration")
            .expect("history");
        assert_eq!(store.load().expect("load history").len(), 2);
    }

    #[tokio::test]
    async fn reconnect_starts_a_fresh_incoming_correlation_scope() {
        let (_dir, selected) = selected_profile(true);
        let services = WorkflowServices::from_selected(&selected).expect("service");
        let message = meshquill_core::Message {
            observation_id: Some(1),
            source: meshquill_core::domain::MessageSource::Channel { channel_idx: 1 },
            route: MessageRoute::Direct,
            txt_type: 0,
            sender_timestamp: 9,
            signature: None,
            text: "same text".to_owned(),
            snr: None,
            status: MessageStatus::Received,
        };

        assert!(
            services
                .incoming(&message, IncomingOrigin::Live)
                .await
                .expect("live message")
                .is_some()
        );
        services
            .connected("reconnected peer")
            .await
            .expect("connect");
        assert!(
            services
                .incoming(&message, IncomingOrigin::Queue)
                .await
                .expect("queued message")
                .is_some()
        );
    }

    #[test]
    fn incoming_tracker_pairs_only_exact_opposite_origin_observations() {
        let mut tracker = IncomingTracker::default();
        assert!(tracker.observe(Some(0x11), IncomingOrigin::Queue));
        assert!(!tracker.observe(Some(0x11), IncomingOrigin::Live));

        assert!(tracker.observe(Some(0x22), IncomingOrigin::Live));
        assert!(tracker.observe(Some(0x22), IncomingOrigin::Live));
        assert!(!tracker.observe(Some(0x22), IncomingOrigin::Queue));
        assert!(!tracker.observe(Some(0x22), IncomingOrigin::Queue));
        assert!(tracker.observe(None, IncomingOrigin::Live));
        assert!(tracker.observe(None, IncomingOrigin::Queue));
    }

    #[test]
    fn incoming_tracker_preserves_distinct_ids_and_evicts_bounded_state() {
        let mut tracker = IncomingTracker::default();
        assert!(tracker.observe(Some(0x33), IncomingOrigin::Live));
        assert!(tracker.observe(Some(0x34), IncomingOrigin::Queue));

        tracker.clear();
        for value in 0..=INCOMING_CORRELATION_CAPACITY {
            assert!(tracker.observe(
                Some(u64::try_from(value).expect("test value fits u64")),
                IncomingOrigin::Live
            ));
        }
        assert_eq!(tracker.occurrences.len(), INCOMING_CORRELATION_CAPACITY);
        assert!(tracker.observe(Some(0), IncomingOrigin::Queue));
    }

    #[tokio::test]
    async fn outgoing_query_is_canonicalized_then_acknowledged_without_duplicates() {
        let (_dir, selected) = selected_profile(true);
        let services = WorkflowServices::from_selected(&selected).expect("service");

        let mut record = services
            .begin_outgoing("2222", None, "outbound")
            .await
            .expect("begin outgoing");
        let ack = [0x01, 0x02, 0x03, 0x04];
        let store = history_store_for_selected(&selected, true)
            .expect("history configuration")
            .expect("history");
        let pending = store.load().expect("load pending history");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].peer, "2222");
        assert_eq!(pending[0].text, "outbound");
        assert_eq!(pending[0].acknowledgement, None);

        services
            .sent(&mut record, "Alice", "outbound", "message-id-1", Some(ack))
            .await
            .expect("sent");

        let after_send = store.load().expect("load history");
        assert_eq!(after_send.len(), 1);
        assert_eq!(after_send[0].status, HistoryStatus::Pending);
        assert_eq!(after_send[0].acknowledgement, Some(ack));
        assert_eq!(after_send[0].peer, "Alice");
        assert_eq!(after_send[0].text, "outbound");

        services
            .acknowledged(&mut record, "message-id-1", Some("demo"), Some(17), ack)
            .await
            .expect("ack");

        services
            .acknowledged(&mut record, "message-id-1", Some("demo"), Some(17), ack)
            .await
            .expect("ack");

        let after_ack = store.load().expect("load history");
        assert_eq!(after_ack.len(), 1);
        assert_eq!(after_ack[0].status, HistoryStatus::Acknowledged);
    }

    #[tokio::test]
    async fn channel_send_keeps_acknowledgement_empty() {
        let (_dir, selected) = selected_profile(true);
        let services = WorkflowServices::from_selected(&selected).expect("service");

        let mut record = services
            .begin_outgoing("7", Some(7), "channel outbound")
            .await
            .expect("begin outgoing");
        services
            .sent(&mut record, "7", "channel outbound", "message-id-2", None)
            .await
            .expect("sent");

        let store = history_store_for_selected(&selected, true)
            .expect("history configuration")
            .expect("history");
        let entries = store.load().expect("load history");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].channel, Some(7));
        assert_eq!(entries[0].acknowledgement, None);
        assert_eq!(entries[0].text, "channel outbound");
    }

    #[tokio::test]
    async fn failed_and_timed_out_outgoing_statuses_are_recorded() {
        let (_dir, selected) = selected_profile(true);
        let services = WorkflowServices::from_selected(&selected).expect("service");

        let mut failed = services
            .begin_outgoing("alice", None, "failing")
            .await
            .expect("begin outgoing");
        services.failed(&mut failed).await.expect("failed");

        let mut timed_out = services
            .begin_outgoing("alice", None, "timeout")
            .await
            .expect("begin outgoing");
        services
            .timed_out(&mut timed_out, "send", "message-id-2")
            .await
            .expect("timed out");

        let store = history_store_for_selected(&selected, true)
            .expect("history configuration")
            .expect("history");
        let entries = store.load().expect("load history");

        assert_eq!(entries.len(), 2);
        assert!(
            entries
                .iter()
                .any(|entry| entry.status == HistoryStatus::Failed)
        );
        assert!(
            entries
                .iter()
                .any(|entry| entry.status == HistoryStatus::TimedOut)
        );
    }
}

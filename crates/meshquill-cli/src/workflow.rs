use std::sync::Arc;

use crate::config::SelectedProfile;
use meshquill_core::{Message, domain::MessageSource};
use meshquill_hooks::{
    AfterSendPayload, BeforeSendInput, ContactChange, HookEvent, HookRuntime, OnAckPayload,
    OnConnectPayload, OnContactUpdatePayload, OnDisconnectPayload, OnErrorPayload,
    OnMessagePayload, OnTimeoutPayload,
};
use meshquill_store::{
    HistoryDirection, HistoryEntry, HistoryStatus, HistoryStore, TransportConfig,
};
use sha2::{Digest, Sha256};
use tokio::task;

use crate::error::CliError;
use crate::output::ExitStatus;

pub(crate) struct WorkflowServices {
    hook_runtime: Option<HookRuntime>,
    history_store: Option<Arc<HistoryStore>>,
    transport_kind: &'static str,
    profile_name: String,
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
            Some(Arc::new(
                meshquill_store::HistoryStore::for_config(
                    &selected.path,
                    &selected.name,
                    selected.config.history.max_messages,
                )
                .map_err(CliError::from)?,
            ))
        } else {
            None
        };

        Ok(Self {
            hook_runtime,
            history_store,
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

    pub(crate) async fn connected(&self, peer: &str) -> Result<(), CliError> {
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
        ack_code: [u8; 4],
    ) -> Result<(), CliError> {
        if let Some(store) = &self.history_store
            && let Some(history_id) = record.history_message_id()
            && let Some(mut entry) = load_history_entry(store, history_id).await?
        {
            entry.acknowledgement = Some(ack_code);
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

    pub(crate) async fn incoming(&self, message: &Message) -> Result<String, CliError> {
        let id = incoming_message_id(message);
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

        Ok(id)
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

fn incoming_message_id(message: &Message) -> String {
    let mut hasher = Sha256::new();
    hasher.update(message_source(message).as_bytes());
    hasher.update([message.txt_type]);
    match message.route {
        meshquill_core::MessageRoute::Direct => hasher.update([0]),
        meshquill_core::MessageRoute::Path {
            hash_mode,
            hop_count,
        } => hasher.update([1, hash_mode, hop_count]),
    }
    hasher.update(message.sender_timestamp.to_le_bytes());
    hasher.update([u8::from(message.signature.is_some())]);
    if let Some(signature) = message.signature {
        hasher.update(signature);
    }
    hasher.update(message.text.as_bytes());

    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&hasher.finalize()[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(bytes).to_string()
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

    HistoryStore::for_config(
        &selected.path,
        &selected.name,
        selected.config.history.max_messages,
    )
    .map(Arc::new)
    .map(Some)
    .map_err(CliError::from)
}

#[cfg(test)]
mod tests {
    use meshquill_core::{MessageRoute, MessageStatus};
    use meshquill_store::{
        Config, DeviceProfile, HistoryDirection, HistoryStatus, TransportConfig,
    };
    use tempfile::tempdir;

    use super::{WorkflowServices, history_store_for_selected};
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

        assert!(dir.path().read_dir().expect("read dir").next().is_none());
    }

    #[tokio::test]
    async fn incoming_messages_are_deduplicated_by_deterministic_uuid() {
        let (_dir, selected) = selected_profile(true);
        let services = WorkflowServices::from_selected(&selected).expect("service");

        let message = meshquill_core::Message {
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

        let first = services.incoming(&message).await.expect("first incoming");
        let second = services.incoming(&message).await.expect("second incoming");

        let store = history_store_for_selected(&selected, true)
            .expect("history configuration")
            .expect("history");
        let entries = store.load().expect("load history");

        assert_eq!(first, second);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id.to_string(), first);
        assert_eq!(entries[0].direction, HistoryDirection::Incoming);
        assert_eq!(entries[0].id.as_bytes()[6] >> 4, 8);
    }

    #[tokio::test]
    async fn outgoing_pending_then_acknowledged_updates_without_duplicates() {
        let (_dir, selected) = selected_profile(true);
        let services = WorkflowServices::from_selected(&selected).expect("service");

        let mut record = services
            .begin_outgoing("alice", None, "outbound")
            .await
            .expect("begin outgoing");
        let ack = [0x01, 0x02, 0x03, 0x04];

        services
            .sent(&mut record, "alice", "outbound", "message-id-1", ack)
            .await
            .expect("sent");

        let store = history_store_for_selected(&selected, true)
            .expect("history configuration")
            .expect("history");
        let after_send = store.load().expect("load history");
        assert_eq!(after_send.len(), 1);
        assert_eq!(after_send[0].status, HistoryStatus::Pending);
        assert_eq!(after_send[0].acknowledgement, Some(ack));

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

#![warn(missing_docs, unreachable_pub)]
#![allow(clippy::module_name_repetitions)]

//! Failure-isolated, opt-in local Python hook execution for Meshquill.
//!
//! Hooks are **trusted local code**. They execute as separate Python processes for crash and
//! timeout isolation, but they are not sandboxed: a hook has the operating-system permissions of
//! the Meshquill process. Timeout cleanup targets the direct Python child; descendants created by
//! a hook are not guaranteed to be terminated on every operating system. Only install and run
//! scripts you trust.
//!
//! Constructing a [`HookRuntime`] is side-effect free. Python is not discovered, started, or
//! imported until [`HookRuntime::validate`], [`HookRuntime::dispatch`], or
//! [`HookRuntime::before_send`] is awaited. Every such operation starts a fresh subprocess and
//! communicates through a bounded, versioned JSON protocol.
//!
//! # Python contract
//!
//! A script may define any of `on_connect`, `on_disconnect`, `on_message`, `before_send`,
//! `after_send`, `on_ack`, `on_timeout`, `on_contact_update`, or `on_error`. Each handler must
//! accept exactly one positional argument. The argument is a dictionary with these stable fields:
//! `schema` (`meshquill.hook/v1`), `event_id` (string), `timestamp` (Unix epoch milliseconds),
//! `event` (one of the names above), and `payload` (the matching typed payload serialized as an
//! object). Synchronous and asynchronous handlers are supported.
//!
//! Observational return values are discarded. `before_send` may return `None` or
//! `{"action": "allow"}`, `{"action": "modify", "destination": ..., "text": ...}` (either
//! replacement field may be omitted), or `{"action": "reject", "reason": ...}`. Rust validates
//! every returned value against [`HookConfig`] limits before exposing it to the caller.
//!
//! Hook and module output is never interpreted as application data. It is captured through bounded
//! pipes and discarded; direct writes to protocol stdout cause a protocol error. Exception details
//! are likewise omitted because they may contain message text or other secrets.

mod error;
mod protocol;
mod runtime;

pub use error::{
    ConfigurationError, HookError, HookErrorCategory, HookRejection, ModificationField,
    ModificationIssue, ProcessError, ProtocolError, StreamKind, ValidationIssue,
};
pub use protocol::{
    AfterSendPayload, BeforeSendInput, BeforeSendOutcome, ContactChange, HookEvent, HookEventKind,
    OnAckPayload, OnConnectPayload, OnContactUpdatePayload, OnDisconnectPayload, OnErrorPayload,
    OnMessagePayload, OnTimeoutPayload, PROTOCOL_SCHEMA,
};
pub use runtime::{
    EnvironmentPolicy, FailurePolicy, HookConfig, HookExecutionStatus, HookReport, HookRuntime,
    HookValidation,
};

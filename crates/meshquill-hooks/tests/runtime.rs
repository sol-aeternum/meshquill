//! Contract and subprocess-isolation tests for the trusted local hook runtime.

use std::{
    env,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::Duration,
};

use meshquill_hooks::{
    BeforeSendInput, BeforeSendOutcome, ConfigurationError, EnvironmentPolicy, FailurePolicy,
    HookConfig, HookError, HookErrorCategory, HookEvent, HookEventKind, HookExecutionStatus,
    HookRuntime, OnMessagePayload, OnTimeoutPayload, ProtocolError, StreamKind, ValidationIssue,
};
use tempfile::tempdir;

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

fn detect_python() -> Option<PathBuf> {
    let executable_names: &[&str] = if cfg!(windows) {
        &["python.exe", "python3.exe"]
    } else {
        &["python3", "python"]
    };
    let mut candidates = Vec::new();
    if let Some(path) = env::var_os("PATH") {
        for directory in env::split_paths(&path) {
            for name in executable_names {
                candidates.push(directory.join(name));
            }
        }
    }
    candidates.into_iter().find(|candidate| {
        candidate.is_file()
            && Command::new(candidate)
                .arg("--version")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .is_ok_and(|status| status.success())
    })
}

fn runtime_with_python(script: &str, python: PathBuf) -> HookRuntime {
    let mut config = HookConfig::new(fixture(script));
    config.python_executable = python;
    HookRuntime::new(config).expect("valid hook test configuration")
}

fn message(text: &str) -> HookEvent {
    HookEvent::OnMessage(OnMessagePayload {
        source: "contact-secret".to_owned(),
        text: text.to_owned(),
        message_id: Some("message-secret".to_owned()),
    })
}

#[test]
fn defaults_contract_and_debug_output_are_safe() {
    let config = HookConfig::new("/private/hooks/example.py");
    assert_eq!(config.observational_failure, FailurePolicy::Open);
    assert_eq!(config.before_send_failure, FailurePolicy::Closed);

    let serialized =
        serde_json::to_value(message("message-secret")).expect("event contract must serialize");
    assert_eq!(serialized["event"], "on_message");
    assert_eq!(serialized["payload"]["text"], "message-secret");

    let debug_event = format!("{:?}", message("message-secret"));
    assert!(!debug_event.contains("message-secret"));
    assert!(!debug_event.contains("contact-secret"));
    let debug_config = format!("{config:?}");
    assert!(!debug_config.contains("/private/hooks/example.py"));

    let mut absent_python = config;
    absent_python.python_executable = PathBuf::from("definitely-absent-python");
    assert!(HookRuntime::new(absent_python).is_ok());
}

#[test]
fn invalid_configuration_is_rejected_without_python() {
    let mut config = HookConfig::new("script.py");
    config.max_concurrency = 0;
    let error = HookRuntime::new(config).expect_err("zero concurrency must fail");
    assert_eq!(error.category(), HookErrorCategory::Configuration);
    assert!(matches!(
        error,
        HookError::Configuration(ConfigurationError::InvalidConcurrency)
    ));

    let mut invalid_environment = HookConfig::new("script.py");
    invalid_environment.environment =
        EnvironmentPolicy::AllowList(["INVALID=NAME".to_owned()].into_iter().collect());
    let error = HookRuntime::new(invalid_environment)
        .expect_err("invalid environment variable names must fail");
    assert!(matches!(
        error,
        HookError::Configuration(ConfigurationError::InvalidEnvironmentName)
    ));
}

#[tokio::test]
async fn before_send_requires_its_dedicated_method_and_can_fail_open() {
    let mut config = HookConfig::new(fixture("working.py"));
    config.python_executable = PathBuf::from("definitely-absent-python");
    config.before_send_failure = FailurePolicy::Open;
    let runtime = HookRuntime::new(config).expect("valid configuration");

    let dispatch_error = runtime
        .dispatch(HookEvent::BeforeSend(BeforeSendInput {
            destination: "destination".to_owned(),
            text: "text".to_owned(),
        }))
        .await
        .expect_err("dispatch cannot discard a mutating result");
    assert!(matches!(
        dispatch_error,
        HookError::Configuration(ConfigurationError::BeforeSendRequiresDedicatedMethod)
    ));

    let outcome = runtime
        .before_send(BeforeSendInput {
            destination: "destination".to_owned(),
            text: "text".to_owned(),
        })
        .await
        .expect("explicit fail-open permits a process failure");
    assert_eq!(outcome, BeforeSendOutcome::Allow);
}

#[tokio::test]
async fn oversized_input_is_rejected_without_starting_python() {
    let mut config = HookConfig::new(fixture("working.py"));
    config.python_executable = PathBuf::from("definitely-absent-python");
    config.max_input_bytes = 128;
    config.observational_failure = FailurePolicy::Closed;
    let runtime = HookRuntime::new(config).expect("valid configuration");
    let error = runtime
        .dispatch(message(&"x".repeat(1024)))
        .await
        .expect_err("input cap must be checked before spawn");
    assert!(matches!(
        error,
        HookError::Protocol(ProtocolError::InputTooLarge { .. })
    ));
}

#[tokio::test]
async fn missing_directory_and_large_scripts_fail_validation_without_python() {
    let directory = tempdir().expect("temporary directory");
    let mut missing_config = HookConfig::new(directory.path().join("absent.py"));
    missing_config.python_executable = PathBuf::from("definitely-absent-python");
    let missing = HookRuntime::new(missing_config)
        .expect("valid runtime")
        .validate()
        .await
        .expect_err("missing script must fail");
    assert!(matches!(
        missing,
        HookError::Configuration(ConfigurationError::ScriptNotFound)
    ));

    let mut directory_config = HookConfig::new(directory.path());
    directory_config.python_executable = PathBuf::from("definitely-absent-python");
    let not_file = HookRuntime::new(directory_config)
        .expect("valid runtime")
        .validate()
        .await
        .expect_err("directory must fail");
    assert!(matches!(
        not_file,
        HookError::Configuration(ConfigurationError::ScriptNotRegularFile)
    ));

    let mut large_config = HookConfig::new(fixture("working.py"));
    large_config.python_executable = PathBuf::from("definitely-absent-python");
    large_config.max_script_bytes = 8;
    let too_large = HookRuntime::new(large_config)
        .expect("valid runtime")
        .validate()
        .await
        .expect_err("large script must fail");
    assert!(matches!(
        too_large,
        HookError::Configuration(ConfigurationError::ScriptTooLarge { .. })
    ));
}

#[tokio::test]
async fn validates_and_dispatches_on_message() {
    let Some(python) = detect_python() else {
        return;
    };
    let runtime = runtime_with_python("working.py", python);
    let validation = runtime.validate().await.expect("working hook validates");
    assert_eq!(
        validation.handlers,
        [HookEventKind::OnMessage].into_iter().collect()
    );
    let report = runtime
        .dispatch(message("hello"))
        .await
        .expect("working handler completes");
    assert_eq!(report.event, HookEventKind::OnMessage);
    assert_eq!(report.status, HookExecutionStatus::Completed);
}

#[tokio::test]
async fn before_send_modifies_allows_and_rejects() {
    let Some(python) = detect_python() else {
        return;
    };
    let runtime = runtime_with_python("before_send.py", python.clone());
    let modified = runtime
        .before_send(BeforeSendInput {
            destination: "original".to_owned(),
            text: "modify".to_owned(),
        })
        .await
        .expect("modify response");
    assert_eq!(
        modified,
        BeforeSendOutcome::Modify {
            destination: "new-destination".to_owned(),
            text: "replacement".to_owned(),
        }
    );

    let partial = runtime
        .before_send(BeforeSendInput {
            destination: "original".to_owned(),
            text: "partial".to_owned(),
        })
        .await
        .expect("partial modify response");
    assert_eq!(
        partial,
        BeforeSendOutcome::Modify {
            destination: "original".to_owned(),
            text: "replacement-only".to_owned(),
        }
    );

    let rejected = runtime
        .before_send(BeforeSendInput {
            destination: "original".to_owned(),
            text: "reject".to_owned(),
        })
        .await
        .expect("reject outcome");
    assert!(matches!(rejected, BeforeSendOutcome::Reject { .. }));
    let rejection_error = rejected
        .require_allowed()
        .expect_err("rejection maps to typed error");
    assert_eq!(rejection_error.category(), HookErrorCategory::Rejected);
    assert!(!format!("{rejection_error:?}").contains("blocked locally"));

    let invalid = runtime
        .before_send(BeforeSendInput {
            destination: "original".to_owned(),
            text: "invalid".to_owned(),
        })
        .await
        .expect_err("default closed policy rejects an invalid modification");
    assert!(matches!(
        invalid,
        HookError::Protocol(ProtocolError::InvalidModification { .. })
    ));

    let mut open_config = HookConfig::new(fixture("before_send.py"));
    open_config.python_executable = python;
    open_config.before_send_failure = FailurePolicy::Open;
    let open_runtime = HookRuntime::new(open_config).expect("valid open-policy runtime");
    let fail_open = open_runtime
        .before_send(BeforeSendInput {
            destination: "original".to_owned(),
            text: "invalid".to_owned(),
        })
        .await
        .expect("invalid modification fails open when explicitly configured");
    assert_eq!(fail_open, BeforeSendOutcome::Allow);
}

#[tokio::test]
async fn timeout_is_bounded_and_typed() {
    let Some(python) = detect_python() else {
        return;
    };
    let mut config = HookConfig::new(fixture("timeout.py"));
    config.python_executable = python;
    config.timeout = Duration::from_millis(100);
    config.observational_failure = FailurePolicy::Closed;
    let runtime = HookRuntime::new(config).expect("valid runtime");
    let error = runtime
        .dispatch(HookEvent::OnTimeout(OnTimeoutPayload {
            operation: "test".to_owned(),
            message_id: None,
        }))
        .await
        .expect_err("slow hook must time out");
    assert_eq!(error.category(), HookErrorCategory::Timeout);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrency_queue_is_included_in_each_operation_deadline() {
    let Some(python) = detect_python() else {
        return;
    };
    let mut config = HookConfig::new(fixture("timeout.py"));
    config.python_executable = python;
    config.timeout = Duration::from_millis(600);
    config.max_concurrency = 1;
    config.observational_failure = FailurePolicy::Closed;
    let runtime = HookRuntime::new(config).expect("valid runtime");

    let first_runtime = runtime.clone();
    let first = tokio::spawn(async move {
        first_runtime
            .dispatch(HookEvent::OnTimeout(OnTimeoutPayload {
                operation: "first".to_owned(),
                message_id: None,
            }))
            .await
    });
    tokio::time::sleep(Duration::from_millis(250)).await;

    let second_started = std::time::Instant::now();
    let second_error = runtime
        .dispatch(HookEvent::OnTimeout(OnTimeoutPayload {
            operation: "second".to_owned(),
            message_id: None,
        }))
        .await
        .expect_err("queued invocation must retain its own original deadline");
    let second_elapsed = second_started.elapsed();
    assert_eq!(second_error.category(), HookErrorCategory::Timeout);
    assert!(
        second_elapsed < Duration::from_millis(800),
        "permit wait must not be followed by a fresh execution timeout"
    );

    let first_error = first
        .await
        .expect("first dispatch task must join")
        .expect_err("first slow invocation must time out");
    assert_eq!(first_error.category(), HookErrorCategory::Timeout);

    let after = runtime
        .dispatch(message("after-timeouts"))
        .await
        .expect("timeouts must release the permit and isolate child processes");
    assert_eq!(after.status, HookExecutionStatus::Missing);
}

#[tokio::test]
async fn malformed_and_oversized_output_are_isolated() {
    let Some(python) = detect_python() else {
        return;
    };
    let mut malformed_config = HookConfig::new(fixture("malformed.py"));
    malformed_config.python_executable = python.clone();
    malformed_config.observational_failure = FailurePolicy::Closed;
    let malformed = HookRuntime::new(malformed_config)
        .expect("valid runtime")
        .dispatch(message("hello"))
        .await
        .expect_err("corrupt stdout must fail");
    assert!(matches!(
        malformed,
        HookError::Protocol(ProtocolError::MalformedOutput)
    ));

    let mut oversized_config = HookConfig::new(fixture("oversized.py"));
    oversized_config.python_executable = python.clone();
    oversized_config.max_output_bytes = 1024;
    oversized_config.observational_failure = FailurePolicy::Closed;
    let oversized = HookRuntime::new(oversized_config)
        .expect("valid runtime")
        .dispatch(message("hello"))
        .await
        .expect_err("oversized stdout must fail");
    assert!(matches!(
        oversized,
        HookError::Protocol(ProtocolError::OutputTooLarge {
            stream: StreamKind::Stdout,
            ..
        })
    ));

    let mut stderr_config = HookConfig::new(fixture("oversized_stderr.py"));
    stderr_config.python_executable = python;
    stderr_config.max_stderr_bytes = 1024;
    stderr_config.observational_failure = FailurePolicy::Closed;
    let oversized_stderr = HookRuntime::new(stderr_config)
        .expect("valid runtime")
        .dispatch(message("hello"))
        .await
        .expect_err("oversized stderr must fail");
    assert!(matches!(
        oversized_stderr,
        HookError::Protocol(ProtocolError::OutputTooLarge {
            stream: StreamKind::Stderr,
            ..
        })
    ));
}

#[tokio::test]
async fn crashing_handler_does_not_poison_the_runtime() {
    let Some(python) = detect_python() else {
        return;
    };
    let mut config = HookConfig::new(fixture("working.py"));
    config.python_executable = python;
    config.observational_failure = FailurePolicy::Closed;
    let runtime = HookRuntime::new(config).expect("valid runtime");
    let crash = runtime
        .dispatch(message("crash"))
        .await
        .expect_err("child process crash must be contained");
    assert_eq!(crash.category(), HookErrorCategory::Process);

    let next = runtime
        .dispatch(message("still-alive"))
        .await
        .expect("a fresh child works after a crash");
    assert_eq!(next.status, HookExecutionStatus::Completed);
}

#[tokio::test]
async fn validation_reports_loadability_and_signature_failures() {
    let Some(python) = detect_python() else {
        return;
    };
    for (fixture_name, expected) in [
        ("load_error.py", ValidationIssue::LoadFailed),
        (
            "bad_signature.py",
            ValidationIssue::InvalidSignature(HookEventKind::OnMessage),
        ),
        (
            "not_callable.py",
            ValidationIssue::HandlerNotCallable(HookEventKind::OnMessage),
        ),
        ("no_handlers.py", ValidationIssue::NoHandlers),
    ] {
        let error = runtime_with_python(fixture_name, python.clone())
            .validate()
            .await
            .expect_err("invalid fixture must fail validation");
        assert!(matches!(
            error,
            HookError::Configuration(ConfigurationError::Validation(actual)) if actual == expected
        ));
    }
}

#[tokio::test]
async fn observational_failure_defaults_to_fail_open_without_leaking_payload() {
    let Some(python) = detect_python() else {
        return;
    };
    let runtime = runtime_with_python("working.py", python);
    let report = runtime
        .dispatch(message("raise-secret"))
        .await
        .expect("default policy fails open");
    assert_eq!(
        report.status,
        HookExecutionStatus::FailedOpen {
            category: HookErrorCategory::Protocol,
        }
    );
    assert!(!format!("{report:?}").contains("raise-secret"));
}

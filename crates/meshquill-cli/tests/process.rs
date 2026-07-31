//! Process-level contracts for the native CLI binary.

use std::{
    fs,
    io::Write as _,
    path::Path,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use serde_json::Value;
use tempfile::TempDir;

#[cfg(unix)]
use std::{
    io::{BufRead as _, BufReader, Read as _},
    process::{Child, Output},
    sync::mpsc::{self, Receiver},
};

#[cfg(unix)]
struct InterruptibleChild {
    child: Child,
    first_stdout: Receiver<Vec<u8>>,
    stdout_task: thread::JoinHandle<Vec<u8>>,
    stderr_task: thread::JoinHandle<Vec<u8>>,
}

#[cfg(unix)]
impl InterruptibleChild {
    fn spawn(arguments: &[&str]) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_meshquill"))
            .args(arguments)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|error| panic!("failed to spawn interruptible meshquill: {error}"));
        let stdout = child.stdout.take().expect("piped child stdout");
        let stderr = child.stderr.take().expect("piped child stderr");
        let (first_tx, first_stdout) = mpsc::sync_channel(1);
        let stdout_task = thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            let mut output = Vec::new();
            reader
                .read_until(b'\n', &mut output)
                .expect("read first child stdout line");
            let _ = first_tx.send(output.clone());
            reader
                .read_to_end(&mut output)
                .expect("read remaining child stdout");
            output
        });
        let stderr_task = thread::spawn(move || {
            let mut reader = BufReader::new(stderr);
            let mut output = Vec::new();
            reader.read_to_end(&mut output).expect("read child stderr");
            output
        });
        Self {
            child,
            first_stdout,
            stdout_task,
            stderr_task,
        }
    }

    fn wait_for_first_stdout(&mut self, timeout: Duration) -> Vec<u8> {
        match self.first_stdout.recv_timeout(timeout) {
            Ok(line) => line,
            Err(error) => {
                let _ = self.child.kill();
                let _ = self.child.wait();
                panic!("meshquill did not emit a first line: {error}");
            }
        }
    }

    fn interrupt(&mut self) {
        let signal = Command::new("kill")
            .args(["-INT", &self.child.id().to_string()])
            .output()
            .unwrap_or_else(|error| panic!("failed to invoke kill: {error}"));
        assert!(
            signal.status.success(),
            "failed to interrupt meshquill: {}",
            text(&signal.stderr)
        );
    }

    fn wait(mut self, timeout: Duration) -> Output {
        let deadline = Instant::now() + timeout;
        let mut timed_out = false;
        let status = loop {
            match self.child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) if Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(5));
                }
                Ok(None) => {
                    timed_out = true;
                    let _ = self.child.kill();
                    break self
                        .child
                        .wait()
                        .unwrap_or_else(|error| panic!("failed to reap meshquill: {error}"));
                }
                Err(error) => panic!("failed to poll interrupted meshquill: {error}"),
            }
        };
        let stdout = self
            .stdout_task
            .join()
            .unwrap_or_else(|_| panic!("child stdout collector panicked"));
        let stderr = self
            .stderr_task
            .join()
            .unwrap_or_else(|_| panic!("child stderr collector panicked"));
        assert!(
            !timed_out,
            "interrupted meshquill did not exit within {timeout:?}; stdout: {}; stderr: {}",
            text(&stdout),
            text(&stderr)
        );
        Output {
            status,
            stdout,
            stderr,
        }
    }
}

fn invoke(arguments: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_meshquill"))
        .args(arguments)
        .output()
        .unwrap_or_else(|error| panic!("failed to run meshquill: {error}"))
}

fn invoke_with_env(arguments: &[&str], name: &str, value: &str) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_meshquill"))
        .args(arguments)
        .env(name, value)
        .output()
        .unwrap_or_else(|error| panic!("failed to run meshquill: {error}"))
}

fn invoke_with_envs(arguments: &[&str], environment: &[(&str, &str)]) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_meshquill"));
    command
        .args(arguments)
        .env_remove("MESHQUILL_CONFIG")
        .env_remove("MESHQUILL_DATA_DIR");
    for (name, value) in environment {
        command.env(name, value);
    }
    command
        .output()
        .unwrap_or_else(|error| panic!("failed to run meshquill: {error}"))
}

fn invoke_in_dir(arguments: &[&str], directory: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_meshquill"))
        .args(arguments)
        .current_dir(directory)
        .output()
        .unwrap_or_else(|error| panic!("failed to run meshquill: {error}"))
}

fn invoke_with_env_timeout(
    arguments: &[&str],
    name: &str,
    value: &str,
    command_timeout: Duration,
) -> std::process::Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_meshquill"))
        .args(arguments)
        .env(name, value)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|error| panic!("failed to spawn meshquill: {error}"));
    let deadline = Instant::now() + command_timeout;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => {
                return child
                    .wait_with_output()
                    .unwrap_or_else(|error| panic!("failed to collect meshquill output: {error}"));
            }
            Ok(None) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(5));
            }
            Ok(None) => {
                let _ = child.kill();
                let output = child.wait_with_output().unwrap_or_else(|error| {
                    panic!("failed to reap timed-out meshquill process: {error}")
                });
                panic!(
                    "meshquill exceeded {command_timeout:?}; stdout: {}; stderr: {}",
                    text(&output.stdout),
                    text(&output.stderr)
                );
            }
            Err(error) => panic!("failed to poll meshquill process: {error}"),
        }
    }
}

fn invoke_with_input_env_timeout(
    arguments: &[&str],
    input: &[u8],
    name: &str,
    value: &str,
    command_timeout: Duration,
) -> std::process::Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_meshquill"))
        .args(arguments)
        .env(name, value)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|error| panic!("failed to spawn meshquill: {error}"));
    let mut stdin = child.stdin.take().expect("piped child stdin");
    stdin
        .write_all(input)
        .unwrap_or_else(|error| panic!("failed to write child stdin: {error}"));
    drop(stdin);

    let deadline = Instant::now() + command_timeout;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => {
                return child
                    .wait_with_output()
                    .unwrap_or_else(|error| panic!("failed to collect meshquill output: {error}"));
            }
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(5)),
            Ok(None) => {
                let _ = child.kill();
                let output = child.wait_with_output().unwrap_or_else(|error| {
                    panic!("failed to reap timed-out meshquill process: {error}")
                });
                panic!(
                    "meshquill exceeded {command_timeout:?}; stdout: {}; stderr: {}",
                    text(&output.stdout),
                    text(&output.stderr)
                );
            }
            Err(error) => panic!("failed to poll meshquill process: {error}"),
        }
    }
}

fn invoke_with_open_stdin_env_timeout(
    arguments: &[&str],
    name: &str,
    value: &str,
    command_timeout: Duration,
) -> std::process::Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_meshquill"))
        .args(arguments)
        .env(name, value)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|error| panic!("failed to spawn meshquill: {error}"));
    let _stdin = child.stdin.take().expect("piped child stdin");
    let deadline = Instant::now() + command_timeout;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => {
                return child
                    .wait_with_output()
                    .unwrap_or_else(|error| panic!("failed to collect meshquill output: {error}"));
            }
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(5)),
            Ok(None) => {
                let _ = child.kill();
                let output = child.wait_with_output().unwrap_or_else(|error| {
                    panic!("failed to reap timed-out meshquill process: {error}")
                });
                panic!(
                    "meshquill exceeded {command_timeout:?}; stdout: {}; stderr: {}",
                    text(&output.stdout),
                    text(&output.stderr)
                );
            }
            Err(error) => panic!("failed to poll meshquill process: {error}"),
        }
    }
}

fn invoke_with_input(arguments: &[&str], input: &[u8]) -> std::process::Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_meshquill"))
        .args(arguments)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|error| panic!("failed to spawn meshquill: {error}"));
    let mut stdin = child.stdin.take().expect("piped child stdin");
    stdin
        .write_all(input)
        .unwrap_or_else(|error| panic!("failed to write child stdin: {error}"));
    drop(stdin);
    child
        .wait_with_output()
        .unwrap_or_else(|error| panic!("failed to wait for meshquill: {error}"))
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8(bytes.to_vec())
        .unwrap_or_else(|error| panic!("process emitted invalid UTF-8: {error}"))
}

fn config_path(directory: &TempDir) -> String {
    directory.path().join("config.toml").display().to_string()
}

fn data_dir_path(directory: &TempDir) -> String {
    directory.path().join("data").display().to_string()
}

fn history_path(directory: &TempDir, config: &str, profile: &str) -> std::path::PathBuf {
    meshquill_store::history_paths(
        &directory.path().join("data"),
        Path::new(config),
        true,
        profile,
    )
    .expect("isolated history paths")
    .canonical
}

fn hook_fixture(name: &str) -> String {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../meshquill-hooks/tests/fixtures")
        .join(name)
        .display()
        .to_string()
}

fn quoted_toml(value: &str) -> String {
    format!("{value:?}")
}

fn add_hook_config(path: &str, script: &str, python_executable: Option<&str>) {
    let mut config = fs::read_to_string(path).expect("read existing configuration");
    let mut section = String::from("[hook]\nenabled = true\nscript = ");
    section.push_str(&quoted_toml(script));
    section.push('\n');
    if let Some(python_executable) = python_executable {
        section.push_str("python_executable = ");
        section.push_str(&quoted_toml(python_executable));
        section.push('\n');
    }
    if let Some(start) = config.find("[hook]\n") {
        let end = config[start + 1..]
            .find("\n[")
            .map_or(config.len(), |offset| start + offset + 2);
        config.replace_range(start..end, &section);
    } else {
        config.push('\n');
        config.push_str(&section);
    }
    fs::write(path, config).expect("write configuration with hook section");
}

fn add_recording_hook(directory: &TempDir, config: &str) -> std::path::PathBuf {
    let script = directory.path().join("record_hook.py");
    fs::write(
        &script,
        concat!(
            "import json\n",
            "from pathlib import Path\n",
            "\n",
            "def _record(event):\n",
            "    marker = Path(__file__).with_suffix('.jsonl')\n",
            "    with marker.open('a', encoding='utf-8') as stream:\n",
            "        stream.write(json.dumps(event, sort_keys=True) + '\\n')\n",
            "\n",
            "def on_connect(event):\n",
            "    _record(event)\n",
            "\n",
            "def on_disconnect(event):\n",
            "    _record(event)\n",
            "\n",
            "def on_message(event):\n",
            "    _record(event)\n",
            "\n",
            "def on_contact_update(event):\n",
            "    _record(event)\n",
            "\n",
            "def on_error(event):\n",
            "    _record(event)\n",
        ),
    )
    .expect("write recording hook");
    add_hook_config(config, &script.display().to_string(), None);
    script
}

fn add_closed_failing_hook(directory: &TempDir, config: &str, handler: &str) -> std::path::PathBuf {
    assert!(matches!(handler, "after_send" | "on_ack"));
    let script = directory.path().join(format!("fail_{handler}.py"));
    fs::write(
        &script,
        format!(
            concat!(
                "from pathlib import Path\n",
                "\n",
                "def {handler}(event):\n",
                "    marker = Path(__file__).with_suffix('.count')\n",
                "    with marker.open('a', encoding='utf-8') as stream:\n",
                "        stream.write('called\\n')\n",
                "    raise RuntimeError('deliberate post-acceptance failure')\n",
            ),
            handler = handler,
        ),
    )
    .expect("write failing hook");
    add_hook_config(config, &script.display().to_string(), None);
    let configured = fs::read_to_string(config).expect("read hook configuration");
    let closed = configured.replacen(
        "[hook]\nenabled = true\n",
        "[hook]\nenabled = true\nobservational_failure = \"closed\"\n",
        1,
    );
    assert_ne!(closed, configured, "hook section was not found");
    fs::write(config, closed).expect("enable fail-closed observational hooks");
    script
}

fn failing_hook_call_count(script: &Path) -> usize {
    fs::read_to_string(script.with_extension("count"))
        .expect("failing hook marker")
        .lines()
        .count()
}

fn recorded_hook_events(script: &Path) -> Vec<Value> {
    let records =
        fs::read_to_string(script.with_extension("jsonl")).expect("recording hook JSONL marker");
    records
        .lines()
        .map(|line| serde_json::from_str(line).expect("recorded hook event JSON"))
        .collect()
}

fn enable_history(path: &str) {
    let config = fs::read_to_string(path).expect("read existing configuration");
    let updated = config.replacen("[history]\nenabled = false", "[history]\nenabled = true", 1);
    assert_ne!(updated, config, "history section was not found");
    fs::write(path, updated).expect("enable local history");
}

fn init_demo(config: &str) {
    let output = invoke(&[
        "--config",
        config,
        "--non-interactive",
        "init",
        "--name",
        "demo",
        "--demo",
        "--set-default",
    ]);
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    assert!(output.stderr.is_empty());
}

fn set_mock_scenario(config: &str, scenario: &str) {
    let current = fs::read_to_string(config).expect("demo configuration");
    let updated = current.replace("scenario = \"demo\"", &format!("scenario = {scenario:?}"));
    assert_ne!(updated, current, "demo mock scenario was not found");
    fs::write(config, updated).expect("mock scenario configuration");
}

#[test]
fn noninteractive_init_creates_an_atomic_demo_profile() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    let output = invoke(&[
        "--config",
        &config,
        "--non-interactive",
        "--output",
        "json",
        "init",
        "--name",
        "field",
        "--demo",
    ]);
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    let parsed: Value = serde_json::from_slice(&output.stdout).expect("JSON init result");
    assert_eq!(parsed["type"], "configuration_initialized");
    let saved = fs::read_to_string(&config).expect("saved configuration");
    assert!(saved.contains("default_profile = \"field\""));
    assert!(saved.contains("scenario = \"demo\""));
}

#[test]
fn noninteractive_init_requires_name_and_one_transport() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    let output = invoke(&["--config", &config, "--non-interactive", "init"]);
    assert_eq!(output.status.code(), Some(2));
    assert!(text(&output.stderr).contains("requires --name"));
    assert!(!Path::new(&config).exists());
}

#[test]
fn output_shape_is_rejected_before_init_writes() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    let output = invoke(&[
        "--config",
        &config,
        "--non-interactive",
        "--output",
        "jsonl",
        "init",
        "--name",
        "demo",
        "--demo",
    ]);
    assert_eq!(output.status.code(), Some(2));
    assert!(!Path::new(&config).exists());
}

#[test]
fn missing_config_is_actionable_and_stable() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    let output = invoke(&["--config", &config, "status"]);
    assert_eq!(output.status.code(), Some(3));
    let stderr = text(&output.stderr);
    assert!(stderr.contains("configuration is missing"));
    assert!(stderr.contains("meshquill init"));
}

#[test]
fn documented_environment_overrides_are_effective_but_not_persisted_by_init() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    let show = invoke_with_env(
        &["--config", &config, "--output", "json", "config", "show"],
        "MESHQUILL_TIMEOUT_REQUEST_MS",
        "77",
    );
    assert_eq!(show.status.code(), Some(0), "{}", text(&show.stderr));
    let effective: Value = serde_json::from_slice(&show.stdout).expect("effective config JSON");
    assert_eq!(
        effective["data"]["effective"]["timeout"]["request_timeout_ms"],
        77
    );

    let second = invoke_with_env(
        &[
            "--config",
            &config,
            "--non-interactive",
            "init",
            "--name",
            "second",
            "--demo",
        ],
        "MESHQUILL_TIMEOUT_REQUEST_MS",
        "77",
    );
    assert_eq!(second.status.code(), Some(0), "{}", text(&second.stderr));
    let saved = fs::read_to_string(&config).expect("configuration after init");
    assert!(saved.contains("request_timeout_ms = 3000"));
    assert!(!saved.contains("request_timeout_ms = 77"));
}

#[test]
fn demo_connect_uses_the_real_core_handshake() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    let output = invoke(&["--config", &config, "--output", "json", "connect"]);
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    let parsed: Value = serde_json::from_slice(&output.stdout).expect("JSON connection result");
    assert_eq!(parsed["schema"], "meshquill.cli/v1");
    assert_eq!(parsed["type"], "connection");
    assert_eq!(parsed["data"]["connected"], true);
}

#[test]
fn verbose_diagnostics_are_stderr_only_and_terminal_safe_when_redirected() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    let output = invoke(&["--config", &config, "-vv", "connect"]);
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    let stderr = text(&output.stderr);
    assert!(stderr.contains("creating bounded CLI client"));
    assert!(!stderr.contains('\u{1b}'));
    assert!(!text(&output.stdout).contains('\u{1b}'));
}

#[test]
fn demo_contacts_support_show_search_and_unique_prefixes() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    for arguments in [
        vec!["--config", &config, "contacts", "--search", "ali"],
        vec!["--config", &config, "contacts", "show", "2222"],
    ] {
        let output = invoke(&arguments);
        assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
        assert!(text(&output.stdout).contains("Alice"));
    }

    let output = invoke(&[
        "--config", &config, "--output", "json", "contacts", "show", "Alice",
    ]);
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    let parsed: Value = serde_json::from_slice(&output.stdout).expect("contact JSON");
    assert_eq!(parsed["type"], "contact");
    assert_eq!(parsed["data"]["profile"], "demo");
    assert_eq!(parsed["data"]["name"], "Alice");
    assert_eq!(
        parsed["data"]["public_key"],
        "2222222222222222222222222222222222222222222222222222222222222222"
    );
}

#[test]
fn contacts_refresh_reports_the_already_mandatory_fresh_device_query() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);

    for (refresh_requested, extra) in [(false, None), (true, Some("--refresh"))] {
        let mut arguments = vec!["--config", &config, "--output", "json", "contacts"];
        if let Some(extra) = extra {
            arguments.push(extra);
        }
        let output = invoke(&arguments);
        assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
        let parsed: Value = serde_json::from_slice(&output.stdout).expect("contacts JSON");
        assert_eq!(parsed["type"], "contacts");
        assert_eq!(parsed["data"]["refreshed"], true);
        assert_eq!(parsed["data"]["refresh_requested"], refresh_requested);
    }
}

#[test]
fn explicit_contact_path_is_validated_and_confirmed() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);

    let refused = invoke(&[
        "--config", &config, "contacts", "path", "set", "Alice", "12,ab,ff",
    ]);
    assert_eq!(refused.status.code(), Some(9));

    let updated = invoke(&[
        "--config", &config, "--yes", "--output", "json", "contacts", "path", "set", "Alice",
        "12,ab,ff",
    ]);
    assert_eq!(updated.status.code(), Some(0), "{}", text(&updated.stderr));
    let parsed: Value = serde_json::from_slice(&updated.stdout).expect("path set JSON");
    assert_eq!(parsed["type"], "contact_path_set");
    assert_eq!(parsed["data"]["path"], "12abff");
    assert_eq!(parsed["data"]["hash_bytes"], 3);
    assert_eq!(parsed["data"]["hop_count"], 1);

    let malformed = invoke(&[
        "--config", &config, "--yes", "contacts", "path", "set", "Alice", "zz",
    ]);
    assert_eq!(malformed.status.code(), Some(2));
}

#[test]
fn demo_direct_send_waits_for_deterministic_ack() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    let output = invoke(&[
        "--config", &config, "--output", "json", "send", "Alice", "hello", "--wait",
    ]);
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    let parsed: Value = serde_json::from_slice(&output.stdout).expect("JSON send result");
    assert_eq!(parsed["data"]["acknowledged"], true);
    assert_eq!(parsed["data"]["ack_code"], "12345678");
}

#[test]
fn ordinary_send_reports_accepted_delivery_before_fail_closed_post_send_hooks() {
    for handler in ["after_send", "on_ack"] {
        let directory = TempDir::new().expect("temporary directory");
        let config = config_path(&directory);
        init_demo(&config);
        enable_history(&config);
        let data_dir = data_dir_path(&directory);
        let script = add_closed_failing_hook(&directory, &config, handler);

        let output = invoke(&[
            "--config",
            &config,
            "--data-dir",
            &data_dir,
            "--output",
            "json",
            "send",
            "Alice",
            "hello",
            "--wait",
        ]);
        assert_eq!(
            output.status.code(),
            Some(11),
            "{handler}: {}",
            text(&output.stderr)
        );
        let report: Value = serde_json::from_slice(&output.stdout)
            .unwrap_or_else(|error| panic!("{handler}: authoritative send JSON: {error}"));
        assert_eq!(report["type"], "send");
        assert_eq!(report["data"]["queued"], true);
        assert_eq!(report["data"]["acknowledged"], true);
        assert!(
            text(&output.stderr).contains("device already accepted the message"),
            "{handler}: {}",
            text(&output.stderr)
        );
        assert_eq!(failing_hook_call_count(&script), 1, "{handler}");

        let history = invoke(&[
            "--config",
            &config,
            "--data-dir",
            &data_dir,
            "--output",
            "json",
            "history",
            "list",
        ]);
        assert_eq!(history.status.code(), Some(0), "{handler}");
        let history: Value = serde_json::from_slice(&history.stdout).expect("history JSON");
        let outgoing = history["data"]["entries"]
            .as_array()
            .expect("history entries")
            .iter()
            .filter(|entry| entry["direction"] == "outgoing")
            .count();
        assert_eq!(outgoing, 1, "{handler}: accepted send must not be replayed");
    }
}

#[test]
fn remote_login_reads_only_stdin_and_reports_authentication_failure_safely() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);

    let failed = invoke_with_input(
        &[
            "--config",
            &config,
            "--non-interactive",
            "remote",
            "login",
            "Alice",
            "--password-stdin",
        ],
        b"wrong-password\n",
    );
    assert_eq!(failed.status.code(), Some(8));
    let stderr = text(&failed.stderr);
    assert!(!stderr.contains("wrong-password"));
    assert!(!text(&failed.stdout).contains("wrong-password"));

    let authenticated = invoke_with_input(
        &[
            "--config",
            &config,
            "--non-interactive",
            "--output",
            "json",
            "remote",
            "login",
            "Alice",
            "--password-stdin",
        ],
        b"meshquill-demo\n",
    );
    assert_eq!(
        authenticated.status.code(),
        Some(0),
        "{}",
        text(&authenticated.stderr)
    );
    let parsed: Value = serde_json::from_slice(&authenticated.stdout).expect("remote login JSON");
    assert_eq!(parsed["type"], "remote_login");
    assert_eq!(parsed["data"]["authenticated"], true);
    assert!(parsed["data"].get("password").is_none());
}

#[test]
fn demo_remote_and_sensor_queries_use_correlated_protocol_responses() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);

    for (arguments, expected_type) in [
        (vec!["remote", "status", "Alice"], "remote_status"),
        (
            vec!["remote", "neighbours", "Alice", "--prefix-length", "6"],
            "remote_neighbours",
        ),
        (vec!["remote", "regions", "Alice"], "remote_regions"),
        (vec!["remote", "owner", "Alice"], "remote_owner"),
        (vec!["remote", "clock", "Alice"], "remote_clock"),
        (vec!["sensor", "telemetry", "Alice"], "sensor_telemetry"),
        (vec!["sensor", "summary", "Alice"], "sensor_summary"),
        (vec!["sensor", "acl", "Alice"], "sensor_acl"),
    ] {
        let mut command = vec!["--config", &config, "--output", "json"];
        command.extend(arguments);
        let output = invoke(&command);
        assert_eq!(
            output.status.code(),
            Some(0),
            "{expected_type}: {}",
            text(&output.stderr)
        );
        let parsed: Value = serde_json::from_slice(&output.stdout).expect("remote query JSON");
        assert_eq!(parsed["type"], expected_type);
    }
}

#[test]
fn arbitrary_remote_commands_require_explicit_destructive_intent() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);

    let denied = invoke(&["--config", &config, "remote", "run", "Alice", "reboot"]);
    assert_eq!(denied.status.code(), Some(9));
    assert!(text(&denied.stderr).contains("read-only allowlist"));

    let still_unconfirmed = invoke(&[
        "--config",
        &config,
        "remote",
        "run",
        "Alice",
        "reboot",
        "--destructive",
    ]);
    assert_eq!(still_unconfirmed.status.code(), Some(9));

    let confirmed = invoke(&[
        "--config",
        &config,
        "--yes",
        "--output",
        "json",
        "remote",
        "run",
        "Alice",
        "reboot",
        "--destructive",
    ]);
    assert_eq!(
        confirmed.status.code(),
        Some(0),
        "{}",
        text(&confirmed.stderr)
    );
    let parsed: Value = serde_json::from_slice(&confirmed.stdout).expect("remote command JSON");
    assert_eq!(parsed["type"], "remote_command");
    assert_eq!(parsed["data"]["destructive"], true);
}

#[test]
fn explicit_ack_timeout_maps_to_timeout_without_success_output() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    let current = fs::read_to_string(&config).expect("demo configuration");
    fs::write(
        &config,
        current.replace("scenario = \"demo\"", "scenario = \"ack-timeout\""),
    )
    .expect("timeout scenario configuration");
    let output = invoke(&[
        "--config",
        &config,
        "--timeout",
        "20ms",
        "--output",
        "json",
        "send",
        "Alice",
        "hello",
        "--wait",
    ]);
    assert_eq!(output.status.code(), Some(7));
    assert!(output.stdout.is_empty());
    assert!(text(&output.stderr).contains("timed out"));
}

#[test]
fn numeric_channel_send_is_supported_but_named_channel_is_not_guessed() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    let numeric = invoke(&["--config", &config, "send", "2", "hello", "--channel"]);
    assert_eq!(numeric.status.code(), Some(0), "{}", text(&numeric.stderr));
    let named = invoke(&["--config", &config, "send", "field", "hello", "--channel"]);
    assert_eq!(named.status.code(), Some(2));
}

#[test]
fn device_reboot_requires_confirmation() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    let refused = invoke(&["--config", &config, "device", "reboot"]);
    assert_eq!(refused.status.code(), Some(9));
    assert!(text(&refused.stderr).contains("confirmation is required"));

    let rebooted = invoke(&[
        "--config", &config, "--yes", "--output", "json", "device", "reboot",
    ]);
    assert_eq!(
        rebooted.status.code(),
        Some(0),
        "{}",
        text(&rebooted.stderr)
    );
    let parsed: Value = serde_json::from_slice(&rebooted.stdout).expect("JSON reboot result");
    assert_eq!(parsed["type"], "device_reboot");
    assert_eq!(parsed["data"]["disconnected"], true);
}

#[test]
fn network_scope_query_and_set_default_are_reported_without_key_material() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);

    let before = invoke(&["--config", &config, "--output", "json", "network", "scope"]);
    assert_eq!(before.status.code(), Some(0), "{}", text(&before.stderr));
    let before_data: Value = serde_json::from_slice(&before.stdout).expect("query scope output");
    assert_eq!(before_data["type"], "network_scope");
    assert_eq!(before_data["data"]["action"], "query");

    let set_default = invoke(&[
        "--config",
        &config,
        "--output",
        "json",
        "network",
        "scope",
        "#field",
        "--set-default",
    ]);
    assert_eq!(
        set_default.status.code(),
        Some(0),
        "{}",
        text(&set_default.stderr)
    );
    let set_json: Value =
        serde_json::from_slice(&set_default.stdout).expect("JSON scope set_default result");
    assert_eq!(set_json["type"], "network_scope");
    assert_eq!(set_json["data"]["scope"], "#field");
    assert!(set_json["data"].get("public_key").is_none());
}

#[test]
fn network_discover_collects_correlated_mock_responses() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    let output = invoke(&[
        "--config",
        &config,
        "--timeout",
        "20ms",
        "--output",
        "json",
        "network",
        "discover",
        "--kind",
        "room",
        "--scope",
        "#field",
    ]);
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    let parsed: Value = serde_json::from_slice(&output.stdout).expect("JSON node discovery");
    assert_eq!(parsed["type"], "network_discovery");
    assert_eq!(parsed["data"]["filter"], "room");
    assert_eq!(parsed["data"]["scope"], "#field");
    assert_eq!(
        parsed["data"]["nodes"][0]["public_key_prefix"],
        "42".repeat(8)
    );
    assert_eq!(parsed["data"]["nodes"][0]["key_bytes"], 8);
    assert_eq!(parsed["data"]["nodes"][0]["node_type"], 2);
    assert_eq!(parsed["data"]["nodes"][0]["kind"], "room");
    assert_eq!(parsed["data"]["nodes"][0]["snr_qdb"], 20);
    assert_eq!(parsed["data"]["nodes"][0]["inbound_snr_qdb"], 12);
    assert_eq!(parsed["data"]["nodes"][0]["rssi_dbm"], -91);
    assert_eq!(parsed["data"]["nodes"][0]["path_len"], 0);
}

#[test]
fn batch_command_files_execute_bounded_non_streaming_commands() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    let commands = directory.path().join("commands.meshquill");
    fs::write(
        &commands,
        "# one command per line\nstatus\ncontacts --search Alice\n",
    )
    .expect("batch command file");

    let output = invoke(&[
        "--config",
        &config,
        "--output",
        "json",
        "batch",
        "run",
        &commands.display().to_string(),
    ]);
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    let parsed: Value = serde_json::from_slice(&output.stdout).expect("JSON batch output");
    assert_eq!(parsed["type"], "batch_run");
    assert_eq!(parsed["data"]["command_count"], 2);
    assert_eq!(parsed["data"]["results"][0]["line"], 2);
    assert_eq!(parsed["data"]["results"][0]["result"]["type"], "status");
    assert_eq!(parsed["data"]["results"][1]["result"]["type"], "contacts");
}

#[test]
fn batch_contact_filters_support_dry_run_and_single_destructive_confirmation() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);

    let dry_run = invoke(&[
        "--config",
        &config,
        "--output",
        "json",
        "batch",
        "contacts",
        "--filter",
        "type=client,name~ali",
        "remote-status",
        "--dry-run",
    ]);
    assert_eq!(dry_run.status.code(), Some(0), "{}", text(&dry_run.stderr));
    let parsed: Value = serde_json::from_slice(&dry_run.stdout).expect("batch dry-run JSON");
    assert_eq!(parsed["type"], "batch_contacts");
    assert_eq!(parsed["data"]["dry_run"], true);
    assert_eq!(parsed["data"]["target_count"], 1);
    assert_eq!(parsed["data"]["targets"][0]["name"], "Alice");
    assert!(parsed["data"]["targets"][0].get("result").is_none());

    let refused = invoke(&[
        "--config",
        &config,
        "batch",
        "contacts",
        "--filter",
        "type=client",
        "path-reset",
    ]);
    assert_eq!(refused.status.code(), Some(9));

    let confirmed = invoke(&[
        "--config",
        &config,
        "--yes",
        "--output",
        "json",
        "batch",
        "contacts",
        "--filter",
        "type=client",
        "path-reset",
    ]);
    assert_eq!(
        confirmed.status.code(),
        Some(0),
        "{}",
        text(&confirmed.stderr)
    );
    let parsed: Value = serde_json::from_slice(&confirmed.stdout).expect("batch reset JSON");
    assert_eq!(parsed["data"]["target_count"], 1);
    assert_eq!(
        parsed["data"]["targets"][0]["result"]["type"],
        "contact_path_reset"
    );
}

#[test]
fn network_trace_reports_contact() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    let output = invoke(&[
        "--config", &config, "--output", "json", "network", "trace", "Alice",
    ]);
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    let parsed: Value = serde_json::from_slice(&output.stdout).expect("JSON network trace");
    assert_eq!(parsed["type"], "network_trace");
    assert_eq!(parsed["data"]["target"], "Alice");
}

#[test]
fn network_trace_rejects_reserved_four_byte_hash_mode_before_connecting() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    let output = invoke(&[
        "--config",
        &config,
        "network",
        "trace",
        "Alice",
        "--hash-bytes",
        "4",
    ]);
    assert_eq!(output.status.code(), Some(2));
    assert!(text(&output.stderr).contains("reserved"));
}

#[test]
fn scoped_send_and_advertise_work_and_cleanup_scope() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);

    let sent = invoke(&[
        "--config",
        &config,
        "--output",
        "json",
        "send",
        "Alice",
        "scoped hello",
        "--scope",
        "#field",
    ]);
    assert_eq!(sent.status.code(), Some(0), "{}", text(&sent.stderr));
    let parsed: Value = serde_json::from_slice(&sent.stdout).expect("JSON scoped send result");
    assert_eq!(parsed["type"], "send");

    let advertise = invoke(&[
        "--config",
        &config,
        "--output",
        "json",
        "device",
        "advertise",
        "--flood",
        "--scope",
        "#field",
    ]);
    assert_eq!(
        advertise.status.code(),
        Some(0),
        "{}",
        text(&advertise.stderr)
    );
    let parsed: Value = serde_json::from_slice(&advertise.stdout).expect("JSON advertise result");
    assert_eq!(parsed["type"], "advertise");
    assert_eq!(parsed["data"]["flood"], true);
}

#[cfg(unix)]
#[test]
fn scoped_ack_wait_handles_sigint_without_success_or_replay() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    enable_history(&config);
    set_mock_scenario(&config, "ack-timeout");
    let data_dir = data_dir_path(&directory);
    let history = history_path(&directory, &config, "demo");
    let mut child = InterruptibleChild::spawn(&[
        "--config",
        &config,
        "--data-dir",
        &data_dir,
        "--timeout",
        "5s",
        "--output",
        "json",
        "send",
        "Alice",
        "interrupt once",
        "--wait",
        "--scope",
        "#field",
    ]);

    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if fs::read_to_string(&history).is_ok_and(|value| value.contains("\"status\":\"pending\""))
        {
            break;
        }
        thread::sleep(Duration::from_millis(5));
    }
    let observed_history = fs::read_to_string(&history);
    if !observed_history
        .as_ref()
        .is_ok_and(|value| value.contains("\"status\":\"pending\""))
    {
        let _ = child.child.kill();
        let _ = child.child.wait();
        panic!("send did not reach its bounded acknowledgement wait: {observed_history:?}");
    }
    child.interrupt();
    let output = child.wait(Duration::from_secs(3));
    assert_eq!(output.status.code(), Some(130), "{}", text(&output.stderr));
    assert!(output.stdout.is_empty());
    assert!(text(&output.stderr).contains("interrupted by user"));

    let entries: Vec<Value> = fs::read_to_string(history)
        .expect("interrupted history")
        .lines()
        .map(|line| serde_json::from_str(line).expect("history entry"))
        .collect();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["status"], "pending");
    assert_eq!(entries[0]["text"], "interrupt once");
}

#[test]
fn invalid_network_scope_length_is_rejected_before_default_is_mutated() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    let before = fs::read(&config).expect("read config before rejected scope");
    let failed = invoke(&[
        "--config",
        &config,
        "network",
        "scope",
        "#aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "--set-default",
    ]);
    assert_eq!(failed.status.code(), Some(2), "{}", text(&failed.stderr));
    assert!(text(&failed.stderr).contains("1..=30"));

    let after = fs::read(&config).expect("read config after rejected scope");
    assert_eq!(after, before);
}

#[test]
fn inbox_limit_and_drain_return_real_demo_message() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    let output = invoke(&[
        "--config", &config, "--output", "json", "inbox", "--limit", "1",
    ]);
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    let parsed: Value = serde_json::from_slice(&output.stdout).expect("JSON inbox result");
    assert_eq!(
        parsed["data"]["messages"][0]["text"],
        "Demo direct packet for deterministic CLI tests"
    );
    assert_eq!(parsed["data"]["drained"], false);
}

#[test]
fn opt_in_history_tracks_delivery_receive_and_confirmed_clear() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    enable_history(&config);
    let data_dir = data_dir_path(&directory);

    let sent = invoke(&[
        "--config",
        &config,
        "--data-dir",
        &data_dir,
        "send",
        "Alice",
        "retained",
        "--wait",
    ]);
    assert_eq!(sent.status.code(), Some(0), "{}", text(&sent.stderr));
    let received = invoke(&[
        "--config",
        &config,
        "--data-dir",
        &data_dir,
        "inbox",
        "--limit",
        "1",
    ]);
    assert_eq!(
        received.status.code(),
        Some(0),
        "{}",
        text(&received.stderr)
    );

    let listed = invoke(&[
        "--config",
        &config,
        "--data-dir",
        &data_dir,
        "--output",
        "json",
        "history",
        "list",
    ]);
    assert_eq!(listed.status.code(), Some(0), "{}", text(&listed.stderr));
    let parsed: Value = serde_json::from_slice(&listed.stdout).expect("history JSON");
    assert_eq!(parsed["data"]["storage"], "plaintext_opt_in");
    let entries = parsed["data"]["entries"]
        .as_array()
        .expect("history entries");
    assert_eq!(entries.len(), 2);
    assert!(
        entries
            .iter()
            .any(|entry| entry["status"] == "acknowledged")
    );
    assert!(entries.iter().any(|entry| entry["status"] == "received"));

    let refused = invoke(&[
        "--config",
        &config,
        "--data-dir",
        &data_dir,
        "history",
        "clear",
    ]);
    assert_eq!(refused.status.code(), Some(9));
    let cleared = invoke(&[
        "--config",
        &config,
        "--data-dir",
        &data_dir,
        "--yes",
        "--output",
        "json",
        "history",
        "clear",
    ]);
    assert_eq!(cleared.status.code(), Some(0), "{}", text(&cleared.stderr));
    assert!(!history_path(&directory, &config, "demo").exists());
}

#[test]
fn explicit_default_config_uses_and_clears_the_implicit_history_namespace() {
    let directory = TempDir::new().expect("temporary directory");
    let home = directory.path().join("home");
    let platform_root = home.join("Library").join("Application Support");
    let home_text = home.display().to_string();
    let platform_root_text = platform_root.display().to_string();
    let environment = [
        ("HOME", home_text.as_str()),
        ("USERPROFILE", home_text.as_str()),
        ("XDG_CONFIG_HOME", platform_root_text.as_str()),
        ("XDG_DATA_HOME", platform_root_text.as_str()),
        ("APPDATA", platform_root_text.as_str()),
        ("LOCALAPPDATA", platform_root_text.as_str()),
    ];
    let app_root = platform_root.join("meshquill");
    let default_config = app_root.join("config.toml");

    let initialized = invoke_with_envs(
        &[
            "--non-interactive",
            "init",
            "--name",
            "demo",
            "--demo",
            "--set-default",
        ],
        &environment,
    );
    assert_eq!(
        initialized.status.code(),
        Some(0),
        "{}",
        text(&initialized.stderr)
    );
    enable_history(&default_config.display().to_string());

    let sent = invoke_with_envs(&["send", "Alice", "same namespace", "--wait"], &environment);
    assert_eq!(sent.status.code(), Some(0), "{}", text(&sent.stderr));
    let canonical = app_root.join("history").join("demo.jsonl");
    assert!(canonical.exists());

    let listed = invoke_with_envs(
        &[
            "--config",
            &default_config.display().to_string(),
            "--output",
            "json",
            "history",
            "list",
        ],
        &environment,
    );
    assert_eq!(listed.status.code(), Some(0), "{}", text(&listed.stderr));
    let listed: Value = serde_json::from_slice(&listed.stdout).expect("history JSON");
    assert_eq!(listed["data"]["path"], canonical.display().to_string());
    assert_eq!(
        listed["data"]["entries"]
            .as_array()
            .expect("history entries")
            .len(),
        1
    );

    let cleared = invoke_with_envs(
        &[
            "--config",
            &default_config.display().to_string(),
            "--yes",
            "history",
            "clear",
        ],
        &environment,
    );
    assert_eq!(cleared.status.code(), Some(0), "{}", text(&cleared.stderr));
    assert!(!canonical.exists());
}

#[test]
fn bounded_watch_emits_jsonl_and_exits() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    let output = invoke(&[
        "--config",
        &config,
        "--output",
        "jsonl",
        "watch",
        "--event",
        "connection",
        "--count",
        "1",
    ]);
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    let lines: Vec<_> = text(&output.stdout).lines().map(str::to_owned).collect();
    assert_eq!(lines.len(), 1);
    let parsed: Value = serde_json::from_str(&lines[0]).expect("JSONL event");
    assert_eq!(parsed["data"]["event"], "connected");
}

#[cfg(unix)]
#[test]
fn watch_handles_sigint_with_stable_interrupted_status() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    let mut child = InterruptibleChild::spawn(&[
        "--config",
        &config,
        "--output",
        "jsonl",
        "watch",
        "--event",
        "connection",
    ]);
    let first = child.wait_for_first_stdout(Duration::from_secs(2));
    let parsed: Value = serde_json::from_slice(&first).expect("first watch event");
    assert_eq!(parsed["data"]["event"], "connected");
    child.interrupt();
    let output = child.wait(Duration::from_secs(2));
    assert_eq!(output.status.code(), Some(130), "{}", text(&output.stderr));
    assert!(text(&output.stderr).contains("interrupted by user"));
}

#[test]
fn watch_recovers_after_bounded_reconnect_failures_and_emits_live_message() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    set_mock_scenario(&config, "reconnect-demo");
    let script = add_recording_hook(&directory, &config);

    let output = invoke_with_env_timeout(
        &[
            "--config", &config, "--output", "jsonl", "watch", "--event", "message", "--count", "1",
        ],
        "MESHQUILL_TIMEOUT_RETRY_MS",
        "1",
        Duration::from_secs(3),
    );
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    let lines: Vec<_> = text(&output.stdout).lines().map(str::to_owned).collect();
    assert_eq!(lines.len(), 1);
    let parsed: Value = serde_json::from_str(&lines[0]).expect("JSONL reconnect event");
    assert_eq!(parsed["schema"], "meshquill.cli/v1");
    assert_eq!(parsed["type"], "event");
    assert_eq!(parsed["data"]["event"], "message");
    assert_eq!(
        parsed["data"]["data"]["message"]["text"],
        "Live direct message after deterministic reconnect"
    );
    let events = recorded_hook_events(&script);
    assert_eq!(
        events
            .iter()
            .filter(|event| event["event"] == "on_connect")
            .count(),
        2
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event["event"] == "on_disconnect")
            .count(),
        2
    );
}

#[test]
fn watch_exits_with_connection_status_after_reconnect_attempts_are_exhausted() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    set_mock_scenario(&config, "reconnect-fail");
    let script = add_recording_hook(&directory, &config);

    let output = invoke_with_env_timeout(
        &[
            "--config", &config, "--output", "jsonl", "watch", "--event", "message", "--count", "1",
        ],
        "MESHQUILL_TIMEOUT_RETRY_MS",
        "1",
        Duration::from_secs(3),
    );
    assert_eq!(output.status.code(), Some(5));
    assert!(output.stdout.is_empty());
    assert!(text(&output.stderr).contains("the companion connection is unavailable"));
    let events = recorded_hook_events(&script);
    assert_eq!(
        events
            .iter()
            .filter(|event| event["event"] == "on_connect")
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event["event"] == "on_disconnect")
            .count(),
        1
    );
}

#[test]
fn mock_devices_are_listed_only_when_explicitly_requested() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    let output = invoke(&[
        "--config",
        &config,
        "--output",
        "json",
        "devices",
        "--transport",
        "mock",
    ]);
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    let parsed: Value = serde_json::from_slice(&output.stdout).expect("JSON devices result");
    assert_eq!(parsed["data"]["mock_profiles"][0], "demo:demo");
}

#[test]
fn repair_never_prompts_in_noninteractive_mode() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    let before = fs::read(&config).expect("configuration before repair");
    let output = invoke(&["--config", &config, "--non-interactive", "config", "repair"]);
    assert_eq!(output.status.code(), Some(9));
    assert_eq!(
        fs::read(&config).expect("configuration after refusal"),
        before
    );
}

#[test]
fn config_show_migrate_and_confirmed_repair_are_real_file_operations() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    fs::write(
        &config,
        concat!(
            "default_profile = \"desk\"\n",
            "[devices.desk]\n",
            "transport = \"serial\"\n",
            "port = \"/dev/ttyUSB0\"\n",
            "baud = 9600\n",
        ),
    )
    .expect("legacy configuration");
    let migration = invoke(&["--config", &config, "--output", "json", "config", "migrate"]);
    assert_eq!(
        migration.status.code(),
        Some(0),
        "{}",
        text(&migration.stderr)
    );
    let migrated: Value = serde_json::from_slice(&migration.stdout).expect("migration JSON");
    assert_eq!(migrated["data"]["changed"], true);
    let backup = migrated["data"]["backup_path"]
        .as_str()
        .expect("migration backup path");
    assert!(Path::new(backup).exists());
    assert!(
        fs::read_to_string(&config)
            .expect("migrated configuration")
            .starts_with("version = 1")
    );

    let show = invoke(&["--config", &config, "--output", "json", "config", "show"]);
    assert_eq!(show.status.code(), Some(0), "{}", text(&show.stderr));
    let shown: Value = serde_json::from_slice(&show.stdout).expect("configuration JSON");
    assert_eq!(shown["data"]["effective"]["default_profile"], "desk");

    let repair = invoke(&[
        "--config", &config, "--yes", "--output", "json", "config", "repair",
    ]);
    assert_eq!(repair.status.code(), Some(0), "{}", text(&repair.stderr));
    let repaired: Value = serde_json::from_slice(&repair.stdout).expect("repair JSON");
    assert_eq!(repaired["data"]["changed"], true);
}

#[test]
fn legacy_default_address_import_is_additive_and_never_overwrites() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    let legacy = directory.path().join("default_address");
    fs::write(&legacy, "AA:BB:CC:DD:EE:FF\n").expect("legacy default address");

    let imported = invoke(&[
        "--config",
        &config,
        "--output",
        "json",
        "config",
        "import-legacy",
        &legacy.display().to_string(),
    ]);
    assert_eq!(
        imported.status.code(),
        Some(0),
        "{}",
        text(&imported.stderr)
    );
    let parsed: Value = serde_json::from_slice(&imported.stdout).expect("legacy import JSON");
    assert_eq!(parsed["type"], "legacy_configuration_import");
    assert_eq!(parsed["data"]["profile"], "legacy");
    let saved = fs::read_to_string(&config).expect("imported configuration");
    assert!(saved.contains("default_profile = \"legacy\""));
    assert!(saved.contains("id = \"AA:BB:CC:DD:EE:FF\""));

    let duplicate = invoke(&[
        "--config",
        &config,
        "config",
        "import-legacy",
        &legacy.display().to_string(),
    ]);
    assert_eq!(duplicate.status.code(), Some(9));
    assert!(text(&duplicate.stderr).contains("never overwrites"));
}

#[test]
fn doctor_without_connect_keeps_host_only_checks() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    let output = invoke(&[
        "--config",
        &config,
        "--timeout",
        "1ms",
        "--output",
        "json",
        "doctor",
    ]);
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    let parsed: Value = serde_json::from_slice(&output.stdout).expect("doctor JSON");
    assert_eq!(parsed["data"]["healthy"], true);
    let checks = parsed["data"]["checks"].as_array().expect("doctor checks");
    assert!(checks.iter().any(|check| check["name"] == "configuration"));
    assert!(
        checks
            .iter()
            .any(|check| check["name"] == "serial_provider")
    );
    assert!(checks.iter().any(|check| check["name"] == "ble_provider"));
    assert!(checks.iter().all(|check| {
        check["name"] != "handshake" && check["name"] != "firmware_compatibility"
    }));
}

#[test]
fn doctor_connect_checks_the_demo_handshake_and_firmware_layout() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    let output = invoke(&[
        "--config",
        &config,
        "--timeout",
        "1ms",
        "--output",
        "json",
        "doctor",
        "--connect",
    ]);
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    let parsed: Value = serde_json::from_slice(&output.stdout).expect("doctor JSON");
    assert_eq!(parsed["data"]["healthy"], true);
    let checks = parsed["data"]["checks"].as_array().expect("doctor checks");
    assert!(
        checks
            .iter()
            .any(|check| check["name"] == "handshake" && check["status"] == "ok")
    );
    let compatibility = checks
        .iter()
        .find(|check| check["name"] == "firmware_compatibility")
        .expect("firmware compatibility check");
    assert_eq!(compatibility["status"], "ok");
    assert!(
        compatibility["detail"]
            .as_str()
            .expect("firmware compatibility detail")
            .contains("protocol level 10")
    );
}

#[test]
fn line_chat_accepts_piped_lines_without_a_tui() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    let output = invoke_with_input(
        &[
            "--config", &config, "--output", "jsonl", "chat", "Alice", "--line",
        ],
        b"hello\n/quit\n",
    );
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    let lines: Vec<Value> = text(&output.stdout)
        .lines()
        .map(|line| serde_json::from_str(line).expect("chat JSONL record"))
        .collect();
    assert_eq!(lines.len(), 4);
    assert_eq!(lines[0]["data"]["state"], "connected");
    assert_eq!(lines[1]["data"]["state"], "incoming");
    assert!(
        lines[1]["data"]["source"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
    );
    assert_eq!(
        lines[1]["data"]["text"],
        "Demo direct packet for deterministic CLI tests"
    );
    assert!(
        lines[1]["data"]["message_id"]
            .as_str()
            .is_some_and(|value| uuid::Uuid::parse_str(value).is_ok())
    );
    assert_eq!(lines[2]["data"]["state"], "sent");
    assert_eq!(lines[3]["data"]["state"], "acknowledged");
}

#[test]
fn line_chat_reports_accepted_delivery_before_fail_closed_post_send_hooks() {
    for handler in ["after_send", "on_ack"] {
        let directory = TempDir::new().expect("temporary directory");
        let config = config_path(&directory);
        init_demo(&config);
        enable_history(&config);
        let data_dir = data_dir_path(&directory);
        let script = add_closed_failing_hook(&directory, &config, handler);

        let output = invoke_with_input(
            &[
                "--config",
                &config,
                "--data-dir",
                &data_dir,
                "--output",
                "jsonl",
                "chat",
                "Alice",
                "--line",
            ],
            b"hello\n/quit\n",
        );
        assert_eq!(
            output.status.code(),
            Some(11),
            "{handler}: {}",
            text(&output.stderr)
        );
        let records: Vec<Value> = text(&output.stdout)
            .lines()
            .map(|line| serde_json::from_str(line).expect("chat JSONL record"))
            .collect();
        let states: Vec<_> = records
            .iter()
            .filter_map(|record| record["data"]["state"].as_str())
            .collect();
        assert_eq!(
            states.iter().filter(|state| **state == "sent").count(),
            1,
            "{handler}: accepted chat message must be reported once"
        );
        assert_eq!(
            states
                .iter()
                .filter(|state| **state == "acknowledged")
                .count(),
            1,
            "{handler}: acknowledgement must be reported before the hook error"
        );
        assert!(
            text(&output.stderr).contains("device already accepted the message"),
            "{handler}: {}",
            text(&output.stderr)
        );
        assert_eq!(failing_hook_call_count(&script), 1, "{handler}");

        let history = invoke(&[
            "--config",
            &config,
            "--data-dir",
            &data_dir,
            "--output",
            "json",
            "history",
            "list",
        ]);
        assert_eq!(history.status.code(), Some(0), "{handler}");
        let history: Value = serde_json::from_slice(&history.stdout).expect("history JSON");
        let outgoing = history["data"]["entries"]
            .as_array()
            .expect("history entries")
            .iter()
            .filter(|entry| entry["direction"] == "outgoing")
            .count();
        assert_eq!(
            outgoing, 1,
            "{handler}: accepted chat send must not be replayed"
        );
    }
}

#[test]
fn line_chat_reconnects_without_losing_or_replaying_piped_text() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    set_mock_scenario(&config, "reconnect-demo");
    let script = add_recording_hook(&directory, &config);

    let output = invoke_with_input_env_timeout(
        &[
            "--config", &config, "--output", "jsonl", "chat", "Alice", "--line",
        ],
        b"hello after reconnect\n/quit\n",
        "MESHQUILL_TIMEOUT_RETRY_MS",
        "1",
        Duration::from_secs(3),
    );
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    let lines: Vec<Value> = text(&output.stdout)
        .lines()
        .map(|line| serde_json::from_str(line).expect("chat reconnect JSONL record"))
        .collect();
    let states: Vec<_> = lines
        .iter()
        .filter_map(|line| line["data"]["state"].as_str())
        .collect();
    assert!(states.contains(&"reconnected"), "{}", text(&output.stdout));
    assert_eq!(
        states.iter().filter(|state| **state == "sent").count(),
        1,
        "{}",
        text(&output.stdout)
    );
    assert_eq!(
        states
            .iter()
            .filter(|state| **state == "acknowledged")
            .count(),
        1,
        "{}",
        text(&output.stdout)
    );
    assert!(lines.iter().any(|line| {
        line["data"]["state"] == "incoming"
            && line["data"]["text"] == "Live direct message after deterministic reconnect"
    }));
    let events = recorded_hook_events(&script);
    assert_eq!(
        events
            .iter()
            .filter(|event| event["event"] == "on_connect")
            .count(),
        2
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event["event"] == "on_disconnect")
            .count(),
        2
    );
}

#[test]
fn line_chat_retains_known_unsent_draft_until_explicit_retry() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    set_mock_scenario(&config, "send-disconnect");

    let output = invoke_with_input_env_timeout(
        &[
            "--config", &config, "--output", "jsonl", "chat", "Alice", "--line",
        ],
        b"known unsent text\n/send\n/quit\n",
        "MESHQUILL_TIMEOUT_RETRY_MS",
        "1",
        Duration::from_secs(3),
    );
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    let lines: Vec<Value> = text(&output.stdout)
        .lines()
        .map(|line| serde_json::from_str(line).expect("chat draft JSONL record"))
        .collect();
    let states: Vec<_> = lines
        .iter()
        .filter_map(|line| line["data"]["state"].as_str())
        .collect();
    assert_eq!(
        states.iter().filter(|state| **state == "sent").count(),
        1,
        "{}",
        text(&output.stdout)
    );
    assert_eq!(
        states
            .iter()
            .filter(|state| **state == "acknowledged")
            .count(),
        1,
        "{}",
        text(&output.stdout)
    );
    assert!(lines.iter().any(|line| {
        line["data"]["state"] == "reconnected" && line["data"]["draft_retained"] == true
    }));
}

#[test]
fn failed_chat_reconnect_emits_one_disconnect_hook() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    set_mock_scenario(&config, "reconnect-fail");
    let script = directory.path().join("disconnect_hook.py");
    fs::write(
        &script,
        concat!(
            "from pathlib import Path\n",
            "\n",
            "def on_disconnect(event):\n",
            "    marker = Path(__file__).with_suffix('.disconnects')\n",
            "    with marker.open('a', encoding='utf-8') as stream:\n",
            "        stream.write(str(event['payload'].get('reason')) + '\\n')\n",
        ),
    )
    .expect("write disconnect hook");
    add_hook_config(&config, &script.display().to_string(), None);

    let output = invoke_with_open_stdin_env_timeout(
        &[
            "--config", &config, "--output", "jsonl", "chat", "Alice", "--line",
        ],
        "MESHQUILL_TIMEOUT_RETRY_MS",
        "1",
        Duration::from_secs(3),
    );
    assert_eq!(output.status.code(), Some(5), "{}", text(&output.stderr));
    let reasons =
        fs::read_to_string(script.with_extension("disconnects")).expect("disconnect hook marker");
    let reasons: Vec<_> = reasons.lines().collect();
    assert_eq!(reasons.len(), 1, "disconnect hook ran more than once");
    assert!(
        matches!(
            reasons[0],
            "chat target lookup transport failed"
                | "initial chat inbox transport failed"
                | "chat device event"
        ),
        "unexpected disconnect reason: {}",
        reasons[0]
    );
}

#[test]
fn line_chat_help_contacts_and_destination_changes_are_chat_events() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    let output = invoke_with_input(
        &[
            "--config", &config, "--output", "jsonl", "chat", "Alice", "--line",
        ],
        b"/help\n/contacts\n/contacts LIc\n/channel 7\n/to 2222\n/quit\n",
    );
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    let lines: Vec<Value> = text(&output.stdout)
        .lines()
        .map(|line| serde_json::from_str(line).expect("chat JSONL record"))
        .collect();
    assert_eq!(lines.len(), 7, "{}", text(&output.stdout));
    assert!(lines.iter().all(|line| line["type"] == "chat"));
    assert_eq!(lines[2]["data"]["state"], "help");
    assert!(
        lines[2]["data"]["commands"]
            .as_array()
            .expect("help commands")
            .iter()
            .any(|command| command.as_str() == Some("/channel <0..255>"))
    );
    assert_eq!(lines[3]["data"]["state"], "contacts");
    assert_eq!(lines[3]["data"]["contacts"][0]["name"], "Alice");
    assert_eq!(
        lines[3]["data"]["contacts"][0]["public_key_prefix"],
        "222222222222"
    );
    assert_eq!(lines[4]["data"]["state"], "contacts");
    assert_eq!(lines[4]["data"]["query"], "LIc");
    assert_eq!(lines[4]["data"]["contacts"][0]["name"], "Alice");
    assert_eq!(lines[5]["data"]["state"], "destination_changed");
    assert_eq!(lines[5]["data"]["destination"], "7");
    assert_eq!(lines[6]["data"]["state"], "destination_changed");
    assert_eq!(lines[6]["data"]["destination"], "Alice");
    assert!(!lines.iter().any(|line| line["data"]["state"] == "sent"));
}

#[test]
fn line_chat_command_errors_are_nonfatal_and_history_disabled_is_explicit() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    let output = invoke_with_input(
        &[
            "--config", &config, "--output", "jsonl", "chat", "Alice", "--line",
        ],
        b"/hlep\n/channel nope\n/to alice\n/send\n/history\n/quit\n",
    );
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    let lines: Vec<Value> = text(&output.stdout)
        .lines()
        .map(|line| serde_json::from_str(line).expect("chat JSONL record"))
        .collect();
    assert_eq!(lines.len(), 7, "{}", text(&output.stdout));
    assert!(lines.iter().all(|line| line["type"] == "chat"));
    assert_eq!(lines[2]["data"]["state"], "command_error");
    assert_eq!(lines[2]["data"]["command"], "/hlep");
    assert_eq!(lines[2]["data"]["suggestion"], "/help");
    assert_eq!(lines[3]["data"]["state"], "command_error");
    assert_eq!(lines[3]["data"]["command"], "/channel");
    assert!(lines[3]["data"].get("suggestion").is_none());
    assert_eq!(lines[4]["data"]["state"], "command_error");
    assert_eq!(lines[4]["data"]["command"], "/to");
    assert_eq!(lines[4]["data"]["suggestion"], "Alice");
    assert_eq!(lines[5]["data"]["state"], "command_error");
    assert_eq!(lines[5]["data"]["command"], "/send");
    assert_eq!(lines[6]["data"]["state"], "history");
    assert_eq!(lines[6]["data"]["destination"], "Alice");
    assert_eq!(lines[6]["data"]["enabled"], false);
    assert_eq!(lines[6]["data"]["storage"], "plaintext_opt_in");
    assert_eq!(lines[6]["data"]["entries"], Value::Array(Vec::new()));
    assert!(!lines.iter().any(|line| line["data"]["state"] == "sent"));
}

#[test]
fn line_chat_double_slash_sends_one_literal_slash() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    enable_history(&config);
    let data_dir = data_dir_path(&directory);
    let output = invoke_with_input(
        &[
            "--config",
            &config,
            "--data-dir",
            &data_dir,
            "--output",
            "jsonl",
            "chat",
            "Alice",
            "--line",
        ],
        b"//help\n/quit\n",
    );
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    let lines: Vec<Value> = text(&output.stdout)
        .lines()
        .map(|line| serde_json::from_str(line).expect("chat JSONL record"))
        .collect();
    assert!(lines.iter().all(|line| line["type"] == "chat"));
    assert!(lines.iter().any(|line| line["data"]["state"] == "sent"));
    assert!(!lines.iter().any(|line| line["data"]["state"] == "help"));

    let entries: Vec<Value> = fs::read_to_string(history_path(&directory, &config, "demo"))
        .expect("chat history")
        .lines()
        .map(|line| serde_json::from_str(line).expect("history entry"))
        .collect();
    assert!(entries.iter().any(|entry| {
        entry["direction"] == "outgoing" && entry["peer"] == "Alice" && entry["text"] == "/help"
    }));
}

#[test]
fn line_chat_rejects_oversized_piped_input_without_sending() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    let mut input = vec![b'x'; 4_097];
    input.push(b'\n');
    let output = invoke_with_input(
        &[
            "--config", &config, "--output", "jsonl", "chat", "Alice", "--line",
        ],
        &input,
    );
    assert_eq!(output.status.code(), Some(2), "{}", text(&output.stderr));
    assert!(text(&output.stderr).contains("4096-byte input bound"));
    let lines: Vec<Value> = text(&output.stdout)
        .lines()
        .map(|line| serde_json::from_str(line).expect("chat JSONL record"))
        .collect();
    assert!(!lines.iter().any(|line| line["data"]["state"] == "sent"));
}

#[cfg(unix)]
#[test]
fn line_chat_handles_sigint_while_waiting_for_input() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    let mut child = InterruptibleChild::spawn(&[
        "--config", &config, "--output", "jsonl", "chat", "Alice", "--line",
    ]);
    let first = child.wait_for_first_stdout(Duration::from_secs(2));
    let parsed: Value = serde_json::from_slice(&first).expect("first chat event");
    assert_eq!(parsed["data"]["state"], "connected");
    child.interrupt();
    let output = child.wait(Duration::from_secs(2));
    assert_eq!(output.status.code(), Some(130), "{}", text(&output.stderr));
    assert!(text(&output.stderr).contains("interrupted by user"));
}

#[test]
fn line_chat_ack_timeout_emits_a_nonfatal_terminal_state() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    let current = fs::read_to_string(&config).expect("demo configuration");
    fs::write(
        &config,
        current.replace("scenario = \"demo\"", "scenario = \"ack-timeout\""),
    )
    .expect("timeout scenario configuration");
    let output = invoke_with_input(
        &[
            "--config",
            &config,
            "--timeout",
            "20ms",
            "--output",
            "jsonl",
            "chat",
            "Alice",
            "--line",
        ],
        b"hello\n/quit\n",
    );
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    let lines: Vec<Value> = text(&output.stdout)
        .lines()
        .map(|line| serde_json::from_str(line).expect("chat JSONL record"))
        .collect();
    assert_eq!(lines.len(), 4);
    assert_eq!(lines[0]["data"]["state"], "connected");
    assert_eq!(lines[1]["data"]["state"], "incoming");
    assert_eq!(lines[2]["data"]["state"], "sent");
    assert_eq!(lines[3]["data"]["state"], "timed_out");
}

#[test]
fn line_chat_channel_send_has_no_direct_ack_state() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    let output = invoke_with_input(
        &[
            "--config", &config, "--output", "jsonl", "chat", "2", "--line",
        ],
        b"hello\n/quit\n",
    );
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    let lines: Vec<Value> = text(&output.stdout)
        .lines()
        .map(|line| serde_json::from_str(line).expect("chat JSONL record"))
        .collect();
    assert_eq!(lines.len(), 3);
    assert_eq!(lines[0]["data"]["state"], "connected");
    assert_eq!(lines[1]["data"]["state"], "incoming");
    assert_eq!(lines[2]["data"]["state"], "sent");
}

#[test]
fn unsupported_mutation_is_honest_even_without_configuration() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    let output = invoke(&["--config", &config, "contacts", "pending", "list"]);
    assert_eq!(output.status.code(), Some(9));
    assert!(text(&output.stderr).contains("not supported"));
    assert!(!Path::new(&config).exists());
}

#[test]
fn important_help_and_readme_examples_stay_in_sync() {
    for arguments in [
        &["--help"][..],
        &["init", "--help"],
        &["devices", "--help"],
        &["connect", "--help"],
        &["doctor", "--help"],
        &["contacts", "--help"],
        &["send", "--help"],
        &["inbox", "--help"],
        &["watch", "--help"],
        &["chat", "--help"],
        &["batch", "--help"],
        &["hooks", "--help"],
        &["mqtt", "--help"],
    ] {
        let output = invoke(arguments);
        assert_eq!(
            output.status.code(),
            Some(0),
            "help failed for {arguments:?}: {}",
            text(&output.stderr)
        );
        assert!(
            text(&output.stdout).contains("Examples:"),
            "important help page has no examples: {arguments:?}"
        );
    }

    let readme = fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../README.md"))
        .expect("read root README");
    for example in [
        "meshquill send Alice 'Are you receiving this?' --wait",
        "meshquill --non-interactive --output json contacts --kind repeater",
        "meshquill batch contacts --filter 'type=repeater,favorite=true' remote-status --dry-run",
        "await mesh.wait_for_ack(receipt, timeout=5.0)",
    ] {
        assert!(
            readme.contains(example),
            "README example drifted: {example}"
        );
    }
}

#[test]
fn completion_and_manpage_artifacts_are_real() {
    let completion = invoke(&["completions", "bash"]);
    assert_eq!(
        completion.status.code(),
        Some(0),
        "{}",
        text(&completion.stderr)
    );
    assert!(text(&completion.stdout).contains("meshquill"));

    let directory = TempDir::new().expect("temporary directory");
    let target = directory.path().display().to_string();
    let manpages = invoke(&["manpages", &target]);
    assert_eq!(
        manpages.status.code(),
        Some(0),
        "{}",
        text(&manpages.stderr)
    );
    assert!(directory.path().join("meshquill.1").exists());
    assert!(directory.path().join("meshquill-send.1").exists());
}

#[test]
fn hooks_validate_and_test_on_message_fixture() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    let script = hook_fixture("working.py");
    add_hook_config(&config, &script, None);

    let validate = invoke(&["--output", "json", "hooks", "validate", &script]);
    assert_eq!(
        validate.status.code(),
        Some(0),
        "{}",
        text(&validate.stderr)
    );
    let parsed: Value = serde_json::from_slice(&validate.stdout).expect("JSON validation result");
    assert_eq!(parsed["schema"], "meshquill.cli/v1");
    assert_eq!(parsed["type"], "hook_validation");
    assert!(
        parsed["data"]["handlers"]
            .as_array()
            .is_some_and(|handlers| { handlers.iter().any(|entry| entry == "on_message") }),
        "validation output should include on_message"
    );

    let test = invoke(&[
        "--config",
        &config,
        "--output",
        "json",
        "hooks",
        "test",
        "on_message",
    ]);
    assert_eq!(test.status.code(), Some(0), "{}", text(&test.stderr));
    let parsed: Value = serde_json::from_slice(&test.stdout).expect("JSON test result");
    assert_eq!(parsed["type"], "hook_test");
    assert_eq!(parsed["data"]["event"], "on_message");
    assert_eq!(parsed["data"]["status"]["status"], "completed");
}

#[test]
fn ordinary_read_hook_lifecycle_is_balanced_and_minimal() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    let script = add_recording_hook(&directory, &config);

    let output = invoke(&["--config", &config, "--output", "json", "device", "info"]);
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));

    let events = recorded_hook_events(&script);
    let names: Vec<_> = events
        .iter()
        .filter_map(|event| event["event"].as_str())
        .collect();
    assert_eq!(names, ["on_connect", "on_disconnect"]);
    assert_eq!(
        events[0]["payload"]
            .as_object()
            .expect("connect payload")
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["peer", "transport"]
    );
    assert_eq!(
        events[1]["payload"]
            .as_object()
            .expect("disconnect payload")
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["reason", "transport"]
    );
    let recorded = fs::read_to_string(script.with_extension("jsonl")).expect("hook JSONL");
    assert!(!recorded.contains("Demo direct packet"));
}

#[test]
fn post_connect_device_failure_hooks_error_without_changing_exit_status_or_leaking_secret() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    let script = add_recording_hook(&directory, &config);
    let secret = "wrong-password-hook-secret";

    let output = invoke_with_input(
        &[
            "--config",
            &config,
            "--non-interactive",
            "remote",
            "login",
            "Alice",
            "--password-stdin",
        ],
        format!("{secret}\n").as_bytes(),
    );
    assert_eq!(output.status.code(), Some(8), "{}", text(&output.stderr));

    let events = recorded_hook_events(&script);
    let names: Vec<_> = events
        .iter()
        .filter_map(|event| event["event"].as_str())
        .collect();
    assert_eq!(names, ["on_connect", "on_error", "on_disconnect"]);
    assert_eq!(events[1]["payload"]["operation"], "remote login");
    assert_eq!(
        events[1]["payload"]["message"],
        "remote authentication failed"
    );
    assert_eq!(
        events[1]["payload"]
            .as_object()
            .expect("error payload")
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["message", "operation"]
    );
    let recorded = fs::read_to_string(script.with_extension("jsonl")).expect("hook JSONL");
    assert!(!recorded.contains(secret));
    assert!(!recorded.contains("Demo direct packet"));
    assert!(!text(&output.stdout).contains(secret));
    assert!(!text(&output.stderr).contains(secret));
}

#[test]
fn every_successful_local_contact_mutation_emits_one_update_hook() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);

    let exported = invoke(&[
        "--config", &config, "--output", "json", "contacts", "export", "Alice",
    ]);
    assert_eq!(
        exported.status.code(),
        Some(0),
        "{}",
        text(&exported.stderr)
    );
    let exported_json: Value =
        serde_json::from_slice(&exported.stdout).expect("contact export JSON");
    let uri = exported_json["data"]["uri"]
        .as_str()
        .expect("exported contact URI")
        .to_owned();
    let script = add_recording_hook(&directory, &config);

    for (label, output) in [
        (
            "update",
            invoke(&[
                "--config", &config, "contacts", "update", "Alice", "--name", "Alicia",
            ]),
        ),
        (
            "forget",
            invoke(&["--config", &config, "--yes", "contacts", "forget", "Alice"]),
        ),
        (
            "import",
            invoke(&["--config", &config, "contacts", "import", &uri]),
        ),
        (
            "path set",
            invoke(&[
                "--config", &config, "--yes", "contacts", "path", "set", "Alice", "12,ab,ff",
            ]),
        ),
        (
            "path reset",
            invoke(&[
                "--config", &config, "--yes", "contacts", "path", "reset", "Alice",
            ]),
        ),
    ] {
        assert_eq!(
            output.status.code(),
            Some(0),
            "{label}: {}",
            text(&output.stderr)
        );
    }

    let events = recorded_hook_events(&script);
    let mutations: Vec<_> = events
        .iter()
        .filter(|event| event["event"] == "on_contact_update")
        .collect();
    assert_eq!(
        events
            .iter()
            .filter(|event| event["event"] == "on_connect")
            .count(),
        5
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event["event"] == "on_disconnect")
            .count(),
        5
    );
    assert!(!events.iter().any(|event| event["event"] == "on_error"));
    assert_eq!(mutations.len(), 5, "{events:?}");
    let changes: Vec<_> = mutations
        .iter()
        .filter_map(|event| event["payload"]["change"].as_str())
        .collect();
    assert_eq!(
        changes,
        ["updated", "removed", "added", "updated", "updated"]
    );
    assert_eq!(mutations[4]["payload"]["display_name"], "Alice");
    assert!(mutations.iter().all(|event| {
        event["payload"]
            .as_object()
            .is_some_and(|payload| payload.len() == 3 && !payload.contains_key("text"))
    }));
    let recorded = fs::read_to_string(script.with_extension("jsonl")).expect("hook JSONL");
    assert!(!recorded.contains(&uri));
    assert!(!recorded.contains("Demo direct packet"));
}

#[test]
fn configured_on_message_hook_runs_during_real_inbox_flow() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    let script = directory.path().join("observe_hook.py");
    fs::write(
        &script,
        concat!(
            "from pathlib import Path\n",
            "\n",
            "def on_message(event):\n",
            "    marker = Path(__file__).with_suffix('.observed')\n",
            "    marker.write_text(event['payload']['text'], encoding='utf-8')\n",
        ),
    )
    .expect("write on_message hook");
    add_hook_config(&config, &script.display().to_string(), None);

    let output = invoke(&["--config", &config, "inbox", "--limit", "1"]);
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    let observed =
        fs::read_to_string(script.with_extension("observed")).expect("on_message hook marker");
    assert_eq!(observed, "Demo direct packet for deterministic CLI tests");
}

#[test]
fn hooks_invalid_script_reports_hook_exit_code() {
    let validate = invoke(&[
        "--output",
        "json",
        "hooks",
        "validate",
        "/definitely-not-a-valid-hook.py",
    ]);
    assert_eq!(
        validate.status.code(),
        Some(11),
        "{}",
        text(&validate.stderr)
    );
}

#[test]
fn hooks_status_is_json_and_schema_only_without_paths() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);

    let status = invoke(&["--config", &config, "--output", "json", "hooks", "status"]);
    assert_eq!(status.status.code(), Some(0), "{}", text(&status.stderr));
    let parsed: Value = serde_json::from_slice(&status.stdout).expect("JSON status");
    assert_eq!(parsed["schema"], "meshquill.cli/v1");
    assert_eq!(parsed["type"], "hook_status");
    assert_eq!(parsed["data"]["protocol"], "meshquill.hook/v1");
    assert_eq!(parsed["data"]["enabled"], false);
    assert_eq!(parsed["data"]["configured"], false);
    assert!(parsed["data"].get("config_path").is_none());
    let stdout = text(&status.stdout);
    assert!(!stdout.contains("python_executable"));
}

#[test]
fn hooks_status_does_not_invoke_python() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    add_hook_config(
        &config,
        &hook_fixture("working.py"),
        Some("definitely-missing-python"),
    );

    let status = invoke(&["--config", &config, "--output", "json", "hooks", "status"]);
    assert_eq!(status.status.code(), Some(0), "{}", text(&status.stderr));
    let parsed: Value = serde_json::from_slice(&status.stdout).expect("JSON status");
    assert_eq!(parsed["data"]["enabled"], true);
    assert_eq!(parsed["data"]["configured"], true);
    let stdout = text(&status.stdout);
    assert!(!stdout.contains("definitely-missing-python"));
}

#[test]
fn mqtt_status_is_side_effect_free_and_redacted_without_configuration() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    let output = invoke(&["--config", &config, "--output", "json", "mqtt", "status"]);
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    assert!(!Path::new(&config).exists());
    let parsed: Value = serde_json::from_slice(&output.stdout).expect("MQTT status JSON");
    assert_eq!(parsed["type"], "mqtt_status");
    assert_eq!(parsed["data"]["schema"], "meshquill.mqtt/v1");
    assert_eq!(parsed["data"]["enabled"], false);
    assert_eq!(parsed["data"]["broker_state"], "not_probed");
    assert!(parsed["data"].get("password").is_none());
    assert!(parsed["data"].get("ca_path").is_none());
}

#[test]
fn mqtt_configure_persists_valid_non_secret_gateway_settings() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    let output = invoke(&[
        "--config",
        &config,
        "--output",
        "json",
        "mqtt",
        "configure",
        "--host",
        "127.0.0.1",
        "--port",
        "1883",
        "--no-tls",
        "--protocol",
        "5",
        "--qos",
        "2",
        "--topic-prefix",
        "field/team",
        "--allow-send",
    ]);
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    let parsed: Value = serde_json::from_slice(&output.stdout).expect("MQTT configure JSON");
    assert_eq!(parsed["type"], "mqtt_configuration");
    assert_eq!(parsed["data"]["tls"], false);
    assert_eq!(parsed["data"]["protocol"], "5");
    assert_eq!(parsed["data"]["qos"], 2);
    assert_eq!(parsed["data"]["allow_send"], true);
    assert_eq!(parsed["data"]["authentication"], false);

    let saved = fs::read_to_string(&config).expect("saved MQTT configuration");
    assert!(saved.contains("enabled = true"));
    assert!(saved.contains("host = \"127.0.0.1\""));
    assert!(saved.contains("topic_prefix = \"field/team\""));
    assert!(!saved.contains("password ="));
}

#[test]
fn mqtt_configure_persists_absolute_tls_paths_across_working_directories() {
    let directory = TempDir::new().expect("temporary directory");
    let configure_dir = directory.path().join("configure");
    let run_dir = directory.path().join("run");
    fs::create_dir_all(&configure_dir).expect("configure directory");
    fs::create_dir_all(&run_dir).expect("run directory");
    let ca_path = configure_dir.join("ca.pem");
    fs::write(
        &ca_path,
        b"-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----\n",
    )
    .expect("CA fixture");
    fs::write(run_dir.join("ca.pem"), b"conflicting relative file").expect("conflicting CA");
    let config = config_path(&directory);

    let configured = invoke_in_dir(
        &[
            "--config",
            &config,
            "mqtt",
            "configure",
            "--host",
            "broker.invalid",
            "--ca-file",
            "ca.pem",
        ],
        &configure_dir,
    );
    assert_eq!(
        configured.status.code(),
        Some(0),
        "{}",
        text(&configured.stderr)
    );
    let loaded = meshquill_store::ConfigStore::new(&config)
        .load_with_overrides(&std::collections::HashMap::new())
        .expect("load MQTT config");
    let meshquill_store::LoadOutcome::Loaded(loaded) = loaded else {
        panic!("expected current MQTT config");
    };
    assert_eq!(
        loaded.mqtt.gateway.tls.ca_path.as_deref(),
        Some(fs::canonicalize(&ca_path).expect("canonical CA").as_path())
    );

    let status = invoke_in_dir(
        &["--config", &config, "--output", "json", "mqtt", "status"],
        &run_dir,
    );
    assert_eq!(status.status.code(), Some(0), "{}", text(&status.stderr));
}

#[test]
fn mqtt_tls_validation_fails_before_writing_configuration() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    let missing_ca = directory.path().join("missing-ca.pem");
    let output = invoke(&[
        "--config",
        &config,
        "mqtt",
        "configure",
        "--host",
        "broker.invalid",
        "--ca-file",
        &missing_ca.display().to_string(),
    ]);
    assert_eq!(output.status.code(), Some(12));
    assert!(text(&output.stderr).contains("TLS"));
    assert!(!Path::new(&config).exists());
}

#[test]
fn mqtt_password_input_is_bounded_before_credential_store_access() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    let oversized = vec![b'x'; 4097];
    let output = invoke_with_input(
        &[
            "--config",
            &config,
            "mqtt",
            "configure",
            "--host",
            "broker.invalid",
            "--username",
            "alice",
            "--password-stdin",
        ],
        &oversized,
    );
    assert_eq!(output.status.code(), Some(2));
    assert!(text(&output.stderr).contains("4096-byte limit"));
    assert!(!Path::new(&config).exists());
}

#[test]
fn mqtt_password_environment_reference_persists_without_secret_material() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    let secret = "mqtt-environment-secret-must-not-be-persisted";
    let output = invoke_with_env(
        &[
            "--config",
            &config,
            "--output",
            "json",
            "mqtt",
            "configure",
            "--host",
            "broker.invalid",
            "--username",
            "alice",
            "--password-env",
            "MESHQUILL_TEST_MQTT_PASSWORD",
        ],
        "MESHQUILL_TEST_MQTT_PASSWORD",
        secret,
    );
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    assert!(!text(&output.stdout).contains(secret));
    assert!(!text(&output.stderr).contains(secret));

    let saved = fs::read_to_string(&config).expect("saved MQTT configuration");
    assert!(saved.contains("kind = \"environment\""));
    assert!(saved.contains("name = \"MESHQUILL_TEST_MQTT_PASSWORD\""));
    assert!(!saved.contains(secret));

    let status = invoke_with_env(
        &["--config", &config, "--output", "json", "mqtt", "status"],
        "MESHQUILL_TEST_MQTT_PASSWORD",
        secret,
    );
    assert_eq!(status.status.code(), Some(0), "{}", text(&status.stderr));
    assert!(!text(&status.stdout).contains(secret));
    assert!(!text(&status.stderr).contains(secret));
}

#[test]
fn mqtt_test_has_a_bounded_mqtt_specific_failure_status() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    let configure = invoke(&[
        "--config",
        &config,
        "mqtt",
        "configure",
        "--host",
        "127.0.0.1",
        "--port",
        "9",
        "--no-tls",
    ]);
    assert_eq!(
        configure.status.code(),
        Some(0),
        "{}",
        text(&configure.stderr)
    );
    let output = invoke(&["--config", &config, "--timeout", "30ms", "mqtt", "test"]);
    assert_eq!(output.status.code(), Some(12));
    assert!(output.stdout.is_empty());
    assert!(text(&output.stderr).contains("timed out"));
}

#[test]
fn mqtt_bridge_exits_on_core_disconnect_without_reconnect_or_hook_replay() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    init_demo(&config);
    set_mock_scenario(&config, "reconnect-demo");
    let script = add_recording_hook(&directory, &config);
    let configure = invoke(&[
        "--config",
        &config,
        "mqtt",
        "configure",
        "--host",
        "127.0.0.1",
        "--port",
        "9",
        "--no-tls",
    ]);
    assert_eq!(
        configure.status.code(),
        Some(0),
        "{}",
        text(&configure.stderr)
    );

    let output = invoke_with_env_timeout(
        &["--config", &config, "--output", "jsonl", "mqtt", "bridge"],
        "MESHQUILL_TIMEOUT_RETRY_MS",
        "1",
        Duration::from_secs(3),
    );
    assert_eq!(output.status.code(), Some(5), "{}", text(&output.stderr));
    assert!(
        text(&output.stderr).contains("MeshCore companion disconnected"),
        "{}",
        text(&output.stderr)
    );
    assert!(text(&output.stderr).contains("Restart the bridge"));

    let events = recorded_hook_events(&script);
    assert_eq!(
        events
            .iter()
            .filter(|event| event["event"] == "on_connect")
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event["event"] == "on_disconnect")
            .count(),
        1
    );
}

#[test]
fn mqtt_bridge_requires_stream_output_before_connecting() {
    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    let output = invoke(&["--config", &config, "--output", "json", "mqtt", "bridge"]);
    assert_eq!(output.status.code(), Some(2));
    assert!(text(&output.stderr).contains("jsonl"));
    assert!(!Path::new(&config).exists());
}

#[test]
#[allow(clippy::too_many_lines)]
fn profile_operations_share_selection_and_migrate_history_without_secret_loss() {
    use std::collections::HashMap;

    use meshquill_store::{
        ConfigStore, LoadOutcome, SecretRef, TransportConfig, TransportOverrides,
    };

    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);
    let data_dir = data_dir_path(&directory);
    init_demo(&config);
    let second = invoke(&[
        "--config",
        &config,
        "--non-interactive",
        "init",
        "--name",
        "field",
        "--demo",
    ]);
    assert_eq!(second.status.code(), Some(0), "{}", text(&second.stderr));

    let listed = invoke(&["--config", &config, "--output", "json", "profile", "list"]);
    assert_eq!(listed.status.code(), Some(0), "{}", text(&listed.stderr));
    let listed: Value = serde_json::from_slice(&listed.stdout).expect("profiles JSON");
    assert_eq!(listed["type"], "profiles");
    assert_eq!(
        listed["data"]["profiles"]
            .as_array()
            .expect("profile rows")
            .len(),
        2
    );

    let store = ConfigStore::new(&config);
    let LoadOutcome::Loaded(mut stored) = store
        .load_with_overrides(&HashMap::new())
        .expect("load profile configuration")
    else {
        panic!("expected current profile configuration");
    };
    let field = stored
        .device_profiles
        .get_mut("field")
        .expect("field profile");
    field.transport_overrides = Some(TransportOverrides {
        request_timeout_ms: Some(777),
    });
    field.secret = Some(SecretRef::environment("MESHQUILL_FIELD_SECRET").expect("secret ref"));
    store.save(&stored).expect("save profile settings");

    let reconfigured = invoke(&[
        "--config", &config, "--output", "json", "profiles", "edit", "field", "--serial", "COM7",
    ]);
    assert_eq!(
        reconfigured.status.code(),
        Some(0),
        "{}",
        text(&reconfigured.stderr)
    );
    let reconfigured_json: Value =
        serde_json::from_slice(&reconfigured.stdout).expect("reconfigured JSON");
    assert_eq!(reconfigured_json["type"], "profile_reconfigured");
    let LoadOutcome::Loaded(mut stored) = store
        .load_with_overrides(&HashMap::new())
        .expect("reload profile configuration")
    else {
        panic!("expected current profile configuration");
    };
    let field = stored.device_profiles.get("field").expect("field profile");
    assert!(matches!(
        &field.transport,
        TransportConfig::Serial { port, baud } if port == "COM7" && *baud == 115_200
    ));
    assert_eq!(
        field
            .transport_overrides
            .as_ref()
            .and_then(|overrides| overrides.request_timeout_ms),
        Some(777)
    );
    assert!(matches!(field.secret, Some(SecretRef::Environment { .. })));

    stored.default_profile = None;
    store.save(&stored).expect("clear default profile");
    let ambiguous = invoke(&["--config", &config, "status"]);
    assert_eq!(ambiguous.status.code(), Some(3));
    assert!(
        text(&ambiguous.stderr).contains("meshquill profiles set-default NAME"),
        "{}",
        text(&ambiguous.stderr)
    );

    let default_set = invoke(&[
        "--config",
        &config,
        "--output",
        "json",
        "profiles",
        "set-default",
        "field",
    ]);
    assert_eq!(
        default_set.status.code(),
        Some(0),
        "{}",
        text(&default_set.stderr)
    );
    let default_json: Value = serde_json::from_slice(&default_set.stdout).expect("default JSON");
    assert_eq!(default_json["type"], "profile_default_set");

    let selected = invoke(&["--config", &config, "--output", "json", "status"]);
    let selected: Value = serde_json::from_slice(&selected.stdout).expect("selected status JSON");
    assert_eq!(selected["data"]["profile"], "field");
    let explicit = invoke(&[
        "--config",
        &config,
        "--profile",
        "demo",
        "--output",
        "json",
        "status",
    ]);
    let explicit: Value = serde_json::from_slice(&explicit.stdout).expect("explicit status JSON");
    assert_eq!(explicit["data"]["profile"], "demo");

    let demo_transport = invoke(&[
        "--config",
        &config,
        "profiles",
        "reconfigure",
        "field",
        "--demo",
    ]);
    assert_eq!(
        demo_transport.status.code(),
        Some(0),
        "{}",
        text(&demo_transport.stderr)
    );
    enable_history(&config);
    let sent = invoke(&[
        "--config",
        &config,
        "--data-dir",
        &data_dir,
        "--profile",
        "field",
        "send",
        "Alice",
        "rename history",
        "--wait",
    ]);
    assert_eq!(sent.status.code(), Some(0), "{}", text(&sent.stderr));
    let old_history = history_path(&directory, &config, "field");
    let new_history = history_path(&directory, &config, "renamed");
    assert!(old_history.exists());

    let renamed = invoke(&[
        "--config",
        &config,
        "--data-dir",
        &data_dir,
        "--yes",
        "--output",
        "json",
        "profiles",
        "rename",
        "field",
        "renamed",
    ]);
    assert_eq!(renamed.status.code(), Some(0), "{}", text(&renamed.stderr));
    let renamed_json: Value = serde_json::from_slice(&renamed.stdout).expect("renamed JSON");
    assert_eq!(renamed_json["type"], "profile_renamed");
    assert_eq!(renamed_json["data"]["history_migrated"], true);
    assert!(
        renamed_json["data"]["warning"]
            .as_str()
            .is_some_and(|warning| warning.contains("credential hashes"))
    );
    assert!(!old_history.exists());
    assert!(new_history.exists());

    let LoadOutcome::Loaded(renamed_config) = store
        .load_with_overrides(&HashMap::new())
        .expect("load renamed configuration")
    else {
        panic!("expected renamed current configuration");
    };
    assert_eq!(renamed_config.default_profile.as_deref(), Some("renamed"));
    let renamed_profile = renamed_config
        .device_profiles
        .get("renamed")
        .expect("renamed profile");
    assert_eq!(
        renamed_profile
            .transport_overrides
            .as_ref()
            .and_then(|overrides| overrides.request_timeout_ms),
        Some(777)
    );
    assert!(matches!(
        renamed_profile.secret,
        Some(SecretRef::Environment { .. })
    ));

    let deleted = invoke(&[
        "--config", &config, "--yes", "--output", "json", "profiles", "remove", "renamed",
    ]);
    assert_eq!(deleted.status.code(), Some(0), "{}", text(&deleted.stderr));
    let deleted_json: Value = serde_json::from_slice(&deleted.stdout).expect("deleted JSON");
    assert_eq!(deleted_json["type"], "profile_deleted");
    assert_eq!(deleted_json["data"]["default_cleared"], true);
    assert_eq!(deleted_json["data"]["history_retained"], true);
    assert!(new_history.exists());

    let refused_reuse = invoke(&[
        "--config",
        &config,
        "--data-dir",
        &data_dir,
        "--yes",
        "profiles",
        "rename",
        "demo",
        "renamed",
    ]);
    assert_eq!(
        refused_reuse.status.code(),
        Some(3),
        "{}",
        text(&refused_reuse.stderr)
    );
    assert!(text(&refused_reuse.stderr).contains("retained destination history"));
    let LoadOutcome::Loaded(after_refusal) = store
        .load_with_overrides(&HashMap::new())
        .expect("load configuration after refused reuse")
    else {
        panic!("expected current configuration after refused reuse");
    };
    assert!(after_refusal.device_profiles.contains_key("demo"));
    assert!(!after_refusal.device_profiles.contains_key("renamed"));

    let sole = invoke(&["--config", &config, "--output", "json", "status"]);
    assert_eq!(sole.status.code(), Some(0), "{}", text(&sole.stderr));
    let sole: Value = serde_json::from_slice(&sole.stdout).expect("sole status JSON");
    assert_eq!(sole["data"]["profile"], "demo");
}

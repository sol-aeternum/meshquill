//! Bounded batch command execution and filtered contact operations.

use std::{
    fs::File,
    io::{Read, Write},
    path::Path,
};

use clap::Parser;
use meshquill_core::{Contact, ContactType};
use serde::Serialize;
use serde_json::Value;

use crate::{
    args::{
        BatchCommand, BatchContactsArgs, Cli, ColorMode, Command, ConfigCommand, MqttCommand,
        OutputMode,
    },
    config::select_profile,
    error::CliError,
    output::{ExitStatus, OutputWriter},
    runtime::{self, confirm, make_client},
};

const MAX_SCRIPT_BYTES: usize = 256 * 1024;
const MAX_SCRIPT_BYTES_U64: u64 = 256 * 1024;
const MAX_COMMANDS: usize = 1_000;
const MAX_LINE_BYTES: usize = 4_096;

#[derive(Debug)]
struct ParsedCommand {
    line: usize,
    cli: Cli,
}

#[derive(Debug, Serialize)]
struct BatchRunEntry {
    line: usize,
    result: Value,
}

#[derive(Debug, Serialize)]
struct BatchRunReport {
    file: String,
    command_count: usize,
    results: Vec<BatchRunEntry>,
}

#[derive(Debug, Serialize)]
struct BatchContactEntry {
    name: String,
    public_key: String,
    kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
}

#[derive(Debug, Serialize)]
struct BatchContactsReport {
    profile: String,
    operation: &'static str,
    dry_run: bool,
    target_count: usize,
    targets: Vec<BatchContactEntry>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BatchOperation {
    RemoteStatus,
    RemoteOwner,
    RemoteRegions,
    RemoteClock,
    SensorTelemetry,
    PathDiscover,
    PathReset,
}

impl BatchOperation {
    fn parse(value: &str) -> Result<Self, CliError> {
        match value.trim() {
            "remote-status" => Ok(Self::RemoteStatus),
            "remote-owner" => Ok(Self::RemoteOwner),
            "remote-regions" => Ok(Self::RemoteRegions),
            "remote-clock" => Ok(Self::RemoteClock),
            "sensor-telemetry" => Ok(Self::SensorTelemetry),
            "path-discover" => Ok(Self::PathDiscover),
            "path-reset" => Ok(Self::PathReset),
            _ => Err(
                CliError::new(ExitStatus::Usage, "unsupported batch contact operation").with_hint(
                    "Use remote-status, remote-owner, remote-regions, remote-clock, \
                 sensor-telemetry, path-discover, or path-reset.",
                ),
            ),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::RemoteStatus => "remote-status",
            Self::RemoteOwner => "remote-owner",
            Self::RemoteRegions => "remote-regions",
            Self::RemoteClock => "remote-clock",
            Self::SensorTelemetry => "sensor-telemetry",
            Self::PathDiscover => "path-discover",
            Self::PathReset => "path-reset",
        }
    }

    fn argv(self, public_key: &str) -> Vec<String> {
        let command: &[&str] = match self {
            Self::RemoteStatus => &["remote", "status"],
            Self::RemoteOwner => &["remote", "owner"],
            Self::RemoteRegions => &["remote", "regions"],
            Self::RemoteClock => &["remote", "clock"],
            Self::SensorTelemetry => &["sensor", "telemetry"],
            Self::PathDiscover => &["contacts", "path", "discover"],
            Self::PathReset => &["contacts", "path", "reset"],
        };
        let mut argv = Vec::with_capacity(command.len().saturating_add(2));
        argv.push("meshquill".to_owned());
        argv.extend(command.iter().map(|part| (*part).to_owned()));
        argv.push(public_key.to_owned());
        argv
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FilterKind {
    Client,
    Repeater,
    Room,
    Sensor,
}

#[derive(Debug, Eq, PartialEq)]
enum FilterClause {
    Kind(FilterKind),
    NameContains(String),
    Favorite(bool),
}

#[derive(Debug, Eq, PartialEq)]
enum ContactFilter {
    All,
    Clauses(Vec<FilterClause>),
}

impl ContactFilter {
    fn parse(value: &str) -> Result<Self, CliError> {
        let value = value.trim();
        if value == "all" {
            return Ok(Self::All);
        }
        if value.is_empty() {
            return Err(filter_usage_error());
        }

        let clauses = value
            .split(',')
            .map(str::trim)
            .map(parse_filter_clause)
            .collect::<Result<Vec<_>, _>>()?;
        if clauses.is_empty() {
            return Err(filter_usage_error());
        }
        Ok(Self::Clauses(clauses))
    }

    fn matches(&self, contact: &Contact) -> bool {
        self.matches_fields(
            contact.contact_type,
            &contact.adv_name,
            contact.flags & 1 != 0,
        )
    }

    fn matches_fields(&self, kind: ContactType, name: &str, favorite: bool) -> bool {
        match self {
            Self::All => true,
            Self::Clauses(clauses) => clauses.iter().all(|clause| match clause {
                FilterClause::Kind(required) => filter_kind_matches(kind, *required),
                FilterClause::NameContains(needle) => name.to_lowercase().contains(needle.as_str()),
                FilterClause::Favorite(required) => favorite == *required,
            }),
        }
    }
}

/// Execute a bounded batch operation using the caller's output contract.
pub(crate) async fn batch<W: Write>(
    cli: &Cli,
    command: &BatchCommand,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    match command {
        BatchCommand::Run { file } => run_file(cli, file, writer).await,
        BatchCommand::Contacts(args) => contacts(cli, args, writer).await,
    }
}

async fn run_file<W: Write>(
    cli: &Cli,
    path: &Path,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let script = read_script(path)?;
    let commands = parse_script(cli, &script)?;
    let mut results = Vec::with_capacity(commands.len());

    for command in commands {
        let result = dispatch_json(&command.cli)
            .await
            .map_err(|error| contextual_error(&error, &format!("batch line {}", command.line)))?;
        results.push(BatchRunEntry {
            line: command.line,
            result,
        });
    }

    let report = BatchRunReport {
        file: path.display().to_string(),
        command_count: results.len(),
        results,
    };
    let human = format!(
        "Executed {} batch command(s) from '{}'.",
        report.command_count,
        terminal_safe(&report.file)
    );
    writer
        .result("batch_run", &report, &human)
        .map_err(CliError::from)
}

async fn contacts<W: Write>(
    cli: &Cli,
    args: &BatchContactsArgs,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let filter = ContactFilter::parse(&args.filter)?;
    let operation = BatchOperation::parse(&args.operation)?;
    let (profile, contacts) = list_contacts_once(cli).await?;
    let contacts: Vec<_> = contacts
        .iter()
        .filter(|contact| filter.matches(contact))
        .collect();

    if !args.dry_run && operation == BatchOperation::PathReset && !contacts.is_empty() {
        confirm(cli, "reset the paths for all filtered batch contacts")?;
    }

    let mut targets = Vec::with_capacity(contacts.len());
    for contact in contacts {
        let public_key = contact.public_key.to_hex();
        let result = if args.dry_run {
            None
        } else {
            let nested = generated_cli(cli, operation, &public_key)?;
            Some(dispatch_json(&nested).await.map_err(|error| {
                let context = format!("batch target '{}'", terminal_safe(&contact.adv_name));
                contextual_error(&error, &context)
            })?)
        };
        targets.push(BatchContactEntry {
            name: contact.adv_name.clone(),
            public_key,
            kind: contact_type_name(contact.contact_type),
            result,
        });
    }

    let report = BatchContactsReport {
        profile,
        operation: operation.name(),
        dry_run: args.dry_run,
        target_count: targets.len(),
        targets,
    };
    let human = contacts_human(&report);
    writer
        .result("batch_contacts", &report, &human)
        .map_err(CliError::from)
}

fn read_script(path: &Path) -> Result<String, CliError> {
    let file = File::open(path).map_err(|error| batch_file_error(path, "open", &error))?;
    let metadata = file
        .metadata()
        .map_err(|error| batch_file_error(path, "inspect", &error))?;
    if !metadata.is_file() {
        return Err(CliError::new(
            ExitStatus::Usage,
            "batch input must be a regular file",
        ));
    }
    if metadata.len() > MAX_SCRIPT_BYTES_U64 {
        return Err(CliError::new(
            ExitStatus::Usage,
            format!("batch file exceeds the {MAX_SCRIPT_BYTES}-byte limit"),
        ));
    }

    let capacity = usize::try_from(metadata.len()).map_err(|_| {
        CliError::new(
            ExitStatus::Usage,
            format!("batch file exceeds the {MAX_SCRIPT_BYTES}-byte limit"),
        )
    })?;
    let mut bytes = Vec::with_capacity(capacity);
    file.take(MAX_SCRIPT_BYTES_U64.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|error| batch_file_error(path, "read", &error))?;
    if bytes.len() > MAX_SCRIPT_BYTES {
        return Err(CliError::new(
            ExitStatus::Usage,
            format!("batch file exceeds the {MAX_SCRIPT_BYTES}-byte limit"),
        ));
    }
    String::from_utf8(bytes)
        .map_err(|_| CliError::new(ExitStatus::Usage, "batch file must contain valid UTF-8"))
}

fn parse_script(parent: &Cli, script: &str) -> Result<Vec<ParsedCommand>, CliError> {
    let mut commands = Vec::new();
    for (line_index, line) in script.lines().enumerate() {
        let line_number = line_index.saturating_add(1);
        if line.len() > MAX_LINE_BYTES {
            return Err(line_error(
                line_number,
                format!("line exceeds the {MAX_LINE_BYTES}-byte limit"),
            ));
        }
        if line.as_bytes().contains(&0) {
            return Err(line_error(line_number, "NUL is not allowed"));
        }
        let Some(cli) = parse_command_line(parent, line, line_number)? else {
            continue;
        };
        if commands.len() == MAX_COMMANDS {
            return Err(line_error(
                line_number,
                format!("batch files may contain at most {MAX_COMMANDS} commands"),
            ));
        }
        commands.push(ParsedCommand {
            line: line_number,
            cli,
        });
    }
    Ok(commands)
}

fn parse_command_line(
    parent: &Cli,
    line: &str,
    line_number: usize,
) -> Result<Option<Cli>, CliError> {
    let words = lex_line(line).map_err(|message| line_error(line_number, message))?;
    if words.is_empty() {
        return Ok(None);
    }
    if has_line_global_option(&words) {
        return Err(line_error(
            line_number,
            "global options are not allowed inside a batch file",
        ));
    }

    let mut argv = Vec::with_capacity(words.len().saturating_add(1));
    argv.push("meshquill".to_owned());
    argv.extend(words);
    let mut parsed = Cli::try_parse_from(argv).map_err(|error| {
        line_error(
            line_number,
            format!(
                "command could not be parsed: {}",
                terminal_safe(&error.to_string())
            ),
        )
    })?;
    validate_nested_command(&parsed.command).map_err(|message| line_error(line_number, message))?;
    inherit_globals(parent, &mut parsed);
    Ok(Some(parsed))
}

fn lex_line(line: &str) -> Result<Vec<String>, &'static str> {
    #[derive(Clone, Copy)]
    enum Quote {
        Single,
        Double,
    }

    let mut words = Vec::new();
    let mut word = String::new();
    let mut quote = None;
    let mut escaped = false;
    let mut started = false;

    for character in line.chars() {
        if escaped {
            word.push(character);
            started = true;
            escaped = false;
            continue;
        }

        match quote {
            Some(Quote::Single) => {
                if character == '\'' {
                    quote = None;
                } else {
                    word.push(character);
                }
                started = true;
            }
            Some(Quote::Double) => {
                if character == '"' {
                    quote = None;
                } else if character == '\\' {
                    escaped = true;
                } else {
                    word.push(character);
                }
                started = true;
            }
            None if character == '#' => break,
            None if character.is_whitespace() => {
                if started {
                    words.push(std::mem::take(&mut word));
                    started = false;
                }
            }
            None if character == '\'' => {
                quote = Some(Quote::Single);
                started = true;
            }
            None if character == '"' => {
                quote = Some(Quote::Double);
                started = true;
            }
            None if character == '\\' => {
                escaped = true;
                started = true;
            }
            None => {
                word.push(character);
                started = true;
            }
        }
    }

    if escaped {
        return Err("line ends with an incomplete backslash escape");
    }
    if quote.is_some() {
        return Err("line contains an unterminated quote");
    }
    if started {
        words.push(word);
    }
    Ok(words)
}

fn has_line_global_option(words: &[String]) -> bool {
    const LONG_GLOBALS: &[&str] = &[
        "--profile",
        "--config",
        "--output",
        "--non-interactive",
        "--yes",
        "--timeout",
        "--color",
        "--quiet",
        "--verbose",
    ];

    for word in words {
        if word == "--" {
            return false;
        }
        let long_name = word.split_once('=').map_or(word.as_str(), |(name, _)| name);
        if LONG_GLOBALS.contains(&long_name) {
            return true;
        }
        if !word.starts_with("--")
            && word.starts_with('-')
            && word.chars().skip(1).any(|value| matches!(value, 'q' | 'v'))
        {
            return true;
        }
    }
    false
}

fn validate_nested_command(command: &Command) -> Result<(), &'static str> {
    match command {
        Command::Batch(_) => Err("nested batch commands are not allowed"),
        Command::Init(_) => Err("init is not allowed inside a batch file"),
        Command::Config(ConfigCommand::Show) => Ok(()),
        Command::Config(_) => Err("configuration mutation is not allowed inside a batch file"),
        Command::Watch(_) | Command::Chat(_) | Command::Mqtt(MqttCommand::Bridge) => {
            Err("streaming commands are not allowed inside a batch file")
        }
        Command::Connect(args) if args.watch => {
            Err("streaming commands are not allowed inside a batch file")
        }
        Command::Mqtt(MqttCommand::Configure(_)) => {
            Err("configuration mutation is not allowed inside a batch file")
        }
        Command::Completions(_) | Command::Manpages(_) => {
            Err("artifact generation is not allowed inside a batch file")
        }
        _ => Ok(()),
    }
}

fn inherit_globals(parent: &Cli, nested: &mut Cli) {
    nested.profile.clone_from(&parent.profile);
    nested.config.clone_from(&parent.config);
    nested.timeout = parent.timeout;
    nested.yes = parent.yes;
    nested.verbose = parent.verbose;
    nested.non_interactive = true;
    nested.color = ColorMode::Never;
    nested.quiet = true;
    nested.output = OutputMode::Json;
}

fn generated_cli(
    parent: &Cli,
    operation: BatchOperation,
    public_key: &str,
) -> Result<Cli, CliError> {
    let mut cli = Cli::try_parse_from(operation.argv(public_key)).map_err(|_| {
        CliError::new(
            ExitStatus::Protocol,
            "the fixed batch contact command could not be parsed",
        )
    })?;
    inherit_globals(parent, &mut cli);
    if operation == BatchOperation::PathReset {
        cli.yes = true;
    }
    Ok(cli)
}

async fn dispatch_json(cli: &Cli) -> Result<Value, CliError> {
    let mut writer = OutputWriter::new(OutputMode::Json, Vec::new());
    Box::pin(runtime::dispatch(cli, &mut writer)).await?;
    let output = writer.into_inner();
    serde_json::from_slice(&output).map_err(|_| {
        CliError::new(
            ExitStatus::Protocol,
            "batch command did not produce exactly one JSON value",
        )
    })
}

async fn list_contacts_once(cli: &Cli) -> Result<(String, Vec<Contact>), CliError> {
    let selected = select_profile(cli)?;
    let client = make_client(&selected)?;
    let contacts = match client.connect().await {
        Ok(_) => client.list_contacts(None).await,
        Err(error) => Err(error),
    };
    let shutdown = client.shutdown().await;
    match contacts {
        Ok(contacts) => {
            shutdown.map_err(CliError::from)?;
            Ok((selected.name, contacts))
        }
        Err(error) => Err(CliError::from(error)),
    }
}

fn parse_filter_clause(value: &str) -> Result<FilterClause, CliError> {
    if let Some(kind) = value.strip_prefix("type=") {
        return match kind {
            "client" => Ok(FilterClause::Kind(FilterKind::Client)),
            "repeater" => Ok(FilterClause::Kind(FilterKind::Repeater)),
            "room" => Ok(FilterClause::Kind(FilterKind::Room)),
            "sensor" => Ok(FilterClause::Kind(FilterKind::Sensor)),
            _ => Err(filter_usage_error()),
        };
    }
    if let Some(name) = value.strip_prefix("name~") {
        let name = name.trim();
        if !name.is_empty() && !name.contains('\0') {
            return Ok(FilterClause::NameContains(name.to_lowercase()));
        }
        return Err(filter_usage_error());
    }
    if let Some(favorite) = value.strip_prefix("favorite=") {
        return match favorite {
            "true" => Ok(FilterClause::Favorite(true)),
            "false" => Ok(FilterClause::Favorite(false)),
            _ => Err(filter_usage_error()),
        };
    }
    Err(filter_usage_error())
}

fn filter_usage_error() -> CliError {
    CliError::new(ExitStatus::Usage, "invalid batch contact filter").with_hint(
        "Use 'all' or comma-separated type=client|repeater|room|sensor, name~TEXT, and \
         favorite=true|false clauses.",
    )
}

const fn filter_kind_matches(contact_type: ContactType, required: FilterKind) -> bool {
    matches!(
        (contact_type, required),
        (ContactType::Chat, FilterKind::Client)
            | (ContactType::Repeater, FilterKind::Repeater)
            | (ContactType::Room, FilterKind::Room)
            | (ContactType::Sensor, FilterKind::Sensor)
    )
}

const fn contact_type_name(contact_type: ContactType) -> &'static str {
    match contact_type {
        ContactType::Chat => "client",
        ContactType::Repeater => "repeater",
        ContactType::Room => "room",
        ContactType::Sensor => "sensor",
        ContactType::Unknown(_) => "unknown",
    }
}

fn contacts_human(report: &BatchContactsReport) -> String {
    let action = if report.dry_run {
        "Matched"
    } else {
        "Applied operation to"
    };
    let heading = format!(
        "{action} {} contact(s) for '{}'.",
        report.target_count, report.operation
    );
    if report.targets.is_empty() {
        return heading;
    }
    let rows = report
        .targets
        .iter()
        .map(|target| {
            let short_key: String = target.public_key.chars().take(12).collect();
            format!(
                "{}\t{}\t{}",
                terminal_safe(&target.name),
                target.kind,
                short_key
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!("{heading}\n{rows}")
}

fn batch_file_error(path: &Path, action: &str, error: &std::io::Error) -> CliError {
    let status = if error.kind() == std::io::ErrorKind::NotFound {
        ExitStatus::NotFound
    } else {
        ExitStatus::Usage
    };
    CliError::new(
        status,
        format!(
            "could not {action} batch file '{}'",
            terminal_safe(&path.display().to_string())
        ),
    )
}

fn line_error(line: usize, message: impl AsRef<str>) -> CliError {
    CliError::new(
        ExitStatus::Usage,
        format!("batch line {line}: {}", terminal_safe(message.as_ref())),
    )
}

fn contextual_error(error: &CliError, context: &str) -> CliError {
    let status = error.status();
    let message = format!(
        "{}: {}",
        terminal_safe(context),
        terminal_safe(error.message())
    );
    let hint = error.hint().map(terminal_safe);
    let contextual = CliError::new(status, message);
    match hint {
        Some(hint) => contextual.with_hint(hint),
        None => contextual,
    }
}

fn terminal_safe(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        if character.is_control() {
            escaped.extend(character.escape_default());
        } else {
            escaped.push(character);
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use meshquill_core::ContactType;

    use super::{BatchOperation, ContactFilter, lex_line, validate_nested_command};
    use crate::args::{Cli, Command};

    #[test]
    fn lexer_handles_quotes_escapes_empty_values_and_comments() {
        let words = lex_line(r#"send 'Alice A' "hello # mesh" plain\ value '' # ignored"#)
            .unwrap_or_else(|error| panic!("lexer rejected valid line: {error}"));
        assert_eq!(
            words,
            ["send", "Alice A", "hello # mesh", "plain value", ""]
        );
    }

    #[test]
    fn lexer_rejects_incomplete_syntax() {
        assert!(lex_line("send Alice \\").is_err());
        assert!(lex_line("send 'Alice").is_err());
        assert!(lex_line("send \"Alice").is_err());
    }

    #[test]
    fn filter_is_a_conjunction_and_matches_favorite_bit() {
        let filter = ContactFilter::parse("type=sensor,name~Weather,favorite=true")
            .unwrap_or_else(|error| panic!("filter rejected: {error}"));
        assert!(filter.matches_fields(ContactType::Sensor, "North WEATHER", true));
        assert!(!filter.matches_fields(ContactType::Sensor, "North weather", false));
        assert!(!filter.matches_fields(ContactType::Repeater, "North weather", true));
    }

    #[test]
    fn filter_accepts_all_and_rejects_unknown_clauses() {
        let all = ContactFilter::parse("all")
            .unwrap_or_else(|error| panic!("all filter rejected: {error}"));
        assert!(all.matches_fields(ContactType::Unknown(77), "anything", false));
        assert!(ContactFilter::parse("type=unknown").is_err());
        assert!(ContactFilter::parse("all,type=client").is_err());
        assert!(ContactFilter::parse("name~").is_err());
    }

    #[test]
    fn operation_policy_is_an_exact_allowlist() {
        for name in [
            "remote-status",
            "remote-owner",
            "remote-regions",
            "remote-clock",
            "sensor-telemetry",
            "path-discover",
            "path-reset",
        ] {
            assert!(BatchOperation::parse(name).is_ok(), "rejected {name}");
        }
        assert!(BatchOperation::parse("remote-run").is_err());
        assert!(BatchOperation::parse("remote-status reboot").is_err());
        assert!(BatchOperation::parse("path-reset --yes").is_err());
    }

    #[test]
    fn generated_operations_use_the_complete_key() {
        let key = "ab".repeat(32);
        let argv = BatchOperation::PathDiscover.argv(&key);
        assert_eq!(argv.last(), Some(&key));
        let expected = vec![
            "meshquill".to_owned(),
            "contacts".to_owned(),
            "path".to_owned(),
            "discover".to_owned(),
            key,
        ];
        assert_eq!(argv, expected);
    }

    #[test]
    fn nested_policy_rejects_batch_and_streaming_commands() {
        let nested = Cli::try_parse_from(["meshquill", "batch", "run", "commands.txt"])
            .unwrap_or_else(|error| panic!("nested fixture did not parse: {error}"));
        assert!(validate_nested_command(&nested.command).is_err());

        let watch = Cli::try_parse_from(["meshquill", "watch"])
            .unwrap_or_else(|error| panic!("watch fixture did not parse: {error}"));
        assert!(validate_nested_command(&watch.command).is_err());

        let status = Cli::try_parse_from(["meshquill", "status"])
            .unwrap_or_else(|error| panic!("status fixture did not parse: {error}"));
        assert!(matches!(&status.command, Command::Status));
        assert!(validate_nested_command(&status.command).is_ok());
    }
}

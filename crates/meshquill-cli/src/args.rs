//! Stable command-line grammar.

use std::{path::PathBuf, time::Duration};

use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};

/// Independent Rust-first client for `MeshCore` companion radios.
#[derive(Debug, Parser)]
#[command(
    name = "meshquill",
    version,
    about,
    propagate_version = true,
    subcommand_required = true,
    arg_required_else_help = true,
    after_help = "Examples:\n  meshquill init\n  meshquill devices\n  meshquill contacts\n  meshquill send Alice 'Are you receiving this?'\n  meshquill watch"
)]
pub struct Cli {
    /// Named device profile. Uses the configured default when omitted.
    #[arg(long, global = true, env = "MESHQUILL_PROFILE")]
    pub profile: Option<String>,

    /// Configuration file override, useful for CI and containers.
    #[arg(long, global = true, env = "MESHQUILL_CONFIG")]
    pub config: Option<PathBuf>,

    /// Output contract. Streams require jsonl rather than json.
    #[arg(long, global = true, value_enum, default_value_t = OutputMode::Human)]
    pub output: OutputMode,

    /// Never read input or display a prompt.
    #[arg(long, global = true)]
    pub non_interactive: bool,

    /// Confirm the explicitly named destructive operation.
    #[arg(long, global = true)]
    pub yes: bool,

    /// Device-operation timeout, for example 5s or 1500ms.
    #[arg(long, global = true, default_value = "5s", value_parser = parse_duration)]
    pub timeout: Duration,

    /// Colour policy. Redirected output is always free of terminal controls.
    #[arg(long, global = true, value_enum, default_value_t = ColorMode::Auto)]
    pub color: ColorMode,

    /// Suppress non-result diagnostics.
    #[arg(short, long, global = true, conflicts_with = "verbose")]
    pub quiet: bool,

    /// Increase diagnostics (`-vv` includes protocol metadata with secrets redacted).
    #[arg(short, long, global = true, action = clap::ArgAction::Count, conflicts_with = "quiet")]
    pub verbose: u8,

    /// Requested operation.
    #[command(subcommand)]
    pub command: Command,
}

/// Output representation used on stdout.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum OutputMode {
    /// Concise text intended for a person.
    #[default]
    Human,
    /// One complete versioned JSON value.
    Json,
    /// One complete versioned JSON object per line.
    Jsonl,
}

/// Terminal colour policy.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum ColorMode {
    /// Use accessible colour only on a capable terminal.
    #[default]
    Auto,
    /// Request colour on terminals; redirects remain plain for safety.
    Always,
    /// Never emit colour.
    Never,
}

/// MQTT wire-protocol selection exposed by broker configuration.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum MqttProtocolChoice {
    /// MQTT 3.1.1, supported by the widest range of brokers.
    #[default]
    #[value(name = "3.1.1", alias = "v3")]
    V311,
    /// MQTT 5.0.
    #[value(name = "5", alias = "v5")]
    V5,
}

/// MQTT quality-of-service selection.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum MqttQosChoice {
    /// At most once (`QoS` 0).
    #[value(name = "0")]
    AtMostOnce,
    /// At least once (`QoS` 1).
    #[default]
    #[value(name = "1")]
    AtLeastOnce,
    /// Exactly once (`QoS` 2).
    #[value(name = "2")]
    ExactlyOnce,
}

/// Supported device transport filters.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum TransportChoice {
    /// Bluetooth Low Energy companion service.
    Ble,
    /// USB or other serial companion interface.
    Serial,
    /// Framed companion protocol over TCP.
    Tcp,
    /// Deterministic virtual companion (demo/test profiles only).
    Mock,
}

/// Top-level command set.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Guided first-run setup and profile creation.
    #[command(
        after_help = "Examples:\n  meshquill init\n  meshquill init --non-interactive --name field --tcp 192.0.2.10:5000 --set-default"
    )]
    Init(InitArgs),
    /// Discover BLE and serial devices, or list configured TCP profiles.
    #[command(
        after_help = "Examples:\n  meshquill devices\n  meshquill devices --transport ble --scan-timeout 8s\n  meshquill devices --output json"
    )]
    Devices(DevicesArgs),
    /// Test a connection and show its state.
    #[command(
        after_help = "Examples:\n  meshquill connect\n  meshquill --output json connect\n  meshquill --output jsonl connect --watch"
    )]
    Connect(ConnectArgs),
    /// Show saved/default connection state without changing it.
    Status,
    /// Diagnose configuration, host transports and optional firmware compatibility.
    #[command(
        after_help = "Examples:\n  meshquill doctor\n  meshquill --profile field doctor --connect\n  meshquill --yes doctor --repair"
    )]
    Doctor(DoctorArgs),
    /// Inspect or administer the locally connected device.
    #[command(subcommand)]
    Device(DeviceCommand),
    /// List or manage contacts (`contacts` alone lists them).
    #[command(
        alias = "list",
        after_help = "Examples:\n  meshquill contacts\n  meshquill contacts --search Alice --kind client\n  meshquill contacts show Alice"
    )]
    Contacts(ContactsArgs),
    /// Send a direct or channel text message.
    #[command(
        after_help = "Examples:\n  meshquill send Alice 'Hello' --wait\n  meshquill send 0 'Check in' --channel\n  meshquill --non-interactive --output json send Alice 'Hello'"
    )]
    Send(SendArgs),
    /// Fetch queued messages from the companion.
    #[command(
        after_help = "Examples:\n  meshquill inbox\n  meshquill inbox --limit 10\n  meshquill --output json inbox"
    )]
    Inbox(InboxArgs),
    /// Inspect or clear explicitly enabled plaintext local message history.
    #[command(subcommand)]
    History(HistoryCommand),
    /// Stream messages and connection events.
    #[command(
        after_help = "Examples:\n  meshquill watch\n  meshquill --output jsonl watch --event message --event ack"
    )]
    Watch(WatchArgs),
    /// Open the portable line-oriented chat interface.
    #[command(
        after_help = "Examples:\n  meshquill chat Alice\n  meshquill chat 0 --line\n  meshquill --output jsonl chat Alice --line"
    )]
    Chat(ChatArgs),
    /// Inspect or administer channels.
    #[command(subcommand)]
    Channels(ChannelCommand),
    /// Remote repeater and room-server operations.
    #[command(subcommand)]
    Remote(RemoteCommand),
    /// Sensor queries.
    #[command(subcommand)]
    Sensor(SensorCommand),
    /// Discovery, trace, regions and scope operations.
    #[command(subcommand)]
    Network(NetworkCommand),
    /// Execute a command file or a filtered contact operation.
    #[command(
        subcommand,
        after_help = "Examples:\n  meshquill batch run commands.meshquill\n  meshquill batch contacts --filter 'type=sensor,favorite=true' sensor-telemetry --dry-run"
    )]
    Batch(BatchCommand),
    /// Inspect, migrate or repair local configuration.
    #[command(subcommand)]
    Config(ConfigCommand),
    /// Validate and exercise trusted local Python hooks.
    #[command(
        subcommand,
        after_help = "Examples:\n  meshquill hooks validate examples/hooks/basic.py\n  meshquill hooks test on_message\n  meshquill --output json hooks status"
    )]
    Hooks(HooksCommand),
    /// Configure or run the optional application-level MQTT gateway.
    #[command(
        subcommand,
        after_help = "Examples:\n  meshquill mqtt status\n  meshquill mqtt test\n  meshquill --output jsonl mqtt bridge"
    )]
    Mqtt(MqttCommand),
    /// Generate a completion script on stdout.
    Completions(CompletionsArgs),
    /// Generate reference man pages into a directory.
    Manpages(ManpagesArgs),
}

/// First-run setup inputs.
#[derive(Debug, Args)]
pub struct InitArgs {
    /// Profile name for non-interactive setup.
    #[arg(long)]
    pub name: Option<String>,
    /// BLE device identifier or address.
    #[arg(long, conflicts_with_all = ["serial", "tcp", "demo"])]
    pub ble: Option<String>,
    /// Serial port path or Windows COM name.
    #[arg(long, conflicts_with_all = ["ble", "tcp", "demo"])]
    pub serial: Option<String>,
    /// TCP endpoint in HOST:PORT form.
    #[arg(long, conflicts_with_all = ["ble", "serial", "demo"])]
    pub tcp: Option<String>,
    /// Create the deterministic demo profile.
    #[arg(long, conflicts_with_all = ["ble", "serial", "tcp"])]
    pub demo: bool,
    /// Select this profile as the default.
    #[arg(long)]
    pub set_default: bool,
}

/// Device discovery inputs.
#[derive(Debug, Args)]
pub struct DevicesArgs {
    /// Limit discovery to one transport.
    #[arg(long, value_enum)]
    pub transport: Option<TransportChoice>,
    /// BLE scan duration.
    #[arg(long, default_value = "5s", value_parser = parse_duration)]
    pub scan_timeout: Duration,
}

/// Connection probe inputs.
#[derive(Debug, Args)]
pub struct ConnectArgs {
    /// Keep the process connected and report state changes.
    #[arg(long)]
    pub watch: bool,
}

/// Doctor scope.
#[derive(Debug, Args)]
pub struct DoctorArgs {
    /// Include an actual connection and protocol handshake.
    #[arg(long)]
    pub connect: bool,
    /// Apply only safe local repairs, with confirmation unless `--yes` is set.
    #[arg(long)]
    pub repair: bool,
}

/// Local device operations.
#[derive(Debug, Subcommand)]
pub enum DeviceCommand {
    /// Show identity, radio settings and negotiated capabilities.
    Info,
    /// Show firmware, model and protocol version.
    Firmware,
    /// Show battery, storage and sensor telemetry.
    Telemetry,
    /// Read or synchronize the device clock.
    Clock(DeviceClockArgs),
    /// Send a local advertisement.
    Advertise(DeviceAdvertiseArgs),
    /// Reboot the local companion after confirmation.
    Reboot,
}

/// Clock operation.
#[derive(Debug, Args)]
pub struct DeviceClockArgs {
    /// Set device time from the host and read it back.
    #[arg(long)]
    pub sync: bool,
}

/// Advertisement operation.
#[derive(Debug, Args)]
pub struct DeviceAdvertiseArgs {
    /// Flood instead of using the normal advertisement path.
    #[arg(long)]
    pub flood: bool,
    /// Temporary region/flood scope.
    #[arg(long)]
    pub scope: Option<String>,
}

/// Contact list defaults and subcommands.
#[derive(Debug, Args)]
pub struct ContactsArgs {
    /// Optional contact operation; omitted means list.
    #[command(subcommand)]
    pub command: Option<ContactCommand>,
    /// Search names or key prefixes while listing.
    #[arg(long, global = true)]
    pub search: Option<String>,
    /// Filter contact type while listing.
    #[arg(long, global = true, value_enum)]
    pub kind: Option<ContactKind>,
    /// Refresh contacts from the device before the operation.
    #[arg(long, global = true)]
    pub refresh: bool,
}

/// Contact type filter.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum ContactKind {
    /// Client/user node.
    Client,
    /// Repeater node.
    Repeater,
    /// Room server.
    Room,
    /// Sensor node.
    Sensor,
}

/// Contact management operations.
#[derive(Debug, Subcommand)]
pub enum ContactCommand {
    /// Show one contact by exact name or key prefix.
    Show {
        /// Contact key or name to show.
        contact: String,
    },
    /// Rename or update flags/path metadata supported by firmware.
    Update(ContactUpdateArgs),
    /// Remove one contact after confirmation.
    Forget {
        /// Contact key or name to remove.
        contact: String,
    },
    /// Inspect or alter a route.
    #[command(subcommand)]
    Path(ContactPathCommand),
    /// Export a contact URI.
    Export {
        /// Contact key or name to export.
        contact: String,
    },
    /// Import a contact URI.
    Import {
        /// Contact URI to import.
        uri: String,
    },
    /// Show, accept or clear manually pending contacts.
    #[command(subcommand)]
    Pending(PendingContactCommand),
}

/// Contact update fields.
#[derive(Debug, Args)]
pub struct ContactUpdateArgs {
    /// Contact identifier to update.
    pub contact: String,
    /// Optional replacement contact name.
    #[arg(long)]
    pub name: Option<String>,
    /// Mark this contact as a favorite when set.
    #[arg(long)]
    pub favorite: Option<bool>,
}

/// Contact route operations.
#[derive(Debug, Subcommand)]
pub enum ContactPathCommand {
    /// Display current route and hash width.
    Show {
        /// Contact name or key prefix whose route is shown.
        contact: String,
    },
    /// Ask the mesh to discover a new route.
    Discover {
        /// Contact name or key prefix for route discovery.
        contact: String,
    },
    /// Reset the route to flood after confirmation.
    Reset {
        /// Contact name or key prefix whose route is reset.
        contact: String,
    },
    /// Set an explicit comma-separated hexadecimal route using the device hash width.
    Set {
        /// Contact name or key prefix whose route is set.
        contact: String,
        /// Comma-separated hexadecimal route bytes, for example `12,ab,ff`.
        path: String,
    },
}

/// Pending-contact operations.
#[derive(Debug, Subcommand)]
pub enum PendingContactCommand {
    /// List pending advertisements.
    List,
    /// Accept one pending contact.
    Accept {
        /// Contact key or name to accept.
        contact: String,
    },
    /// Clear all pending contacts after confirmation.
    Clear,
}

/// Message send inputs.
#[derive(Debug, Args)]
pub struct SendArgs {
    /// Contact name/key prefix, or a numeric channel index with `--channel`.
    pub destination: String,
    /// UTF-8 message text.
    pub message: String,
    /// Treat the destination as a channel.
    #[arg(long)]
    pub channel: bool,
    /// Wait for delivery acknowledgement (direct messages only).
    #[arg(long)]
    pub wait: bool,
    /// Temporary named scope, `default`, or `unscoped`.
    #[arg(long)]
    pub scope: Option<String>,
}

/// Inbox retrieval inputs.
#[derive(Debug, Args)]
pub struct InboxArgs {
    /// Stop after this many messages; omitted drains the queue.
    #[arg(long)]
    pub limit: Option<usize>,
}

/// Local message-history operations.
#[derive(Debug, Subcommand)]
pub enum HistoryCommand {
    /// List retained entries, newest bounded subset when `--limit` is supplied.
    List {
        /// Return at most this many of the newest retained entries.
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Delete the selected profile's local history file after confirmation.
    Clear,
}

/// Event stream inputs.
#[derive(Debug, Args)]
pub struct WatchArgs {
    /// Event kinds to include. May be repeated.
    #[arg(long = "event", value_enum)]
    pub events: Vec<WatchEvent>,
    /// Exit after this many events.
    #[arg(long)]
    pub count: Option<usize>,
}

/// Filterable event kinds.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum WatchEvent {
    /// A message event.
    Message,
    /// An acknowledgement event.
    Ack,
    /// A contact lifecycle event.
    Contact,
    /// A connection state event.
    Connection,
    /// A telemetry event.
    Telemetry,
    /// An error event.
    Error,
}

/// Chat mode inputs.
#[derive(Debug, Args)]
pub struct ChatArgs {
    /// Initial contact, channel, room, repeater or sensor.
    pub destination: Option<String>,
    /// Select the portable line-oriented interface explicitly.
    #[arg(long)]
    pub line: bool,
}

/// Channel management operations.
#[derive(Debug, Subcommand)]
pub enum ChannelCommand {
    /// List all configured channels.
    List,
    /// Show details for one channel.
    Show {
        /// Channel name or index.
        channel: String,
    },
    /// Add or update a channel.
    Set(ChannelSetArgs),
    /// Remove a configured channel.
    Remove {
        /// Channel name or index to remove.
        channel: String,
    },
}

/// Channel update fields.
#[derive(Debug, Args)]
pub struct ChannelSetArgs {
    /// Channel number to set.
    pub channel: u8,
    /// New channel name to apply.
    #[arg(long)]
    pub name: String,
    /// Read the 16-byte secret from a file rather than exposing it in argv.
    #[arg(long)]
    pub secret_file: Option<PathBuf>,
}

/// Remote node operations.
#[derive(Debug, Subcommand)]
pub enum RemoteCommand {
    /// Login; reads the password securely unless a credential reference exists.
    Login {
        /// Target contact for authentication.
        contact: String,
        /// Read the password from standard input instead of a terminal prompt.
        #[arg(long)]
        password_stdin: bool,
        /// Save the password in the operating-system credential store after login succeeds.
        #[arg(long)]
        save: bool,
    },
    /// End the companion's authenticated session with a remote peer.
    Logout {
        /// Target contact whose active session is closed.
        contact: String,
    },
    /// Send one `CommonCLI` command.
    Run(RemoteRunArgs),
    /// Show remote status for a peer.
    Status {
        /// Contact to inspect.
        contact: String,
    },
    /// Refresh a remote neighbour list.
    Neighbours(RemoteNeighboursArgs),
    /// List remote regions known to a peer.
    Regions {
        /// Contact that provides region lookup context.
        contact: String,
    },
    /// Show remote owner metadata.
    Owner {
        /// Contact for owner metadata lookup.
        contact: String,
    },
    /// Read or synchronize remote clock.
    Clock(RemoteClockArgs),
    /// Forget a stored credential reference.
    CredentialsForget {
        /// Contact whose stored credential reference is removed.
        contact: String,
    },
}

/// Remote command fields.
#[derive(Debug, Args)]
pub struct RemoteRunArgs {
    /// Target remote contact.
    pub contact: String,
    /// Command string forwarded to the remote system.
    pub command: String,
    /// Mark a known destructive remote CLI command as intentional.
    #[arg(long)]
    pub destructive: bool,
}

/// Remote neighbour query fields.
#[derive(Debug, Args)]
pub struct RemoteNeighboursArgs {
    /// Contact that defines the neighbour lookup scope.
    pub contact: String,
    /// Maximum records requested in this page.
    #[arg(long, default_value_t = 255)]
    pub count: u8,
    /// Record offset for pagination.
    #[arg(long, default_value_t = 0)]
    pub offset: u16,
    /// Neighbour ordering mode.
    #[arg(long, value_enum, default_value_t = NeighbourOrderChoice::Newest)]
    pub order: NeighbourOrderChoice,
    /// Public-key prefix bytes returned for each neighbour (1 through 32).
    #[arg(long, default_value_t = 6)]
    pub prefix_length: u8,
}

/// Ordering supported by current repeater firmware.
#[derive(Clone, Copy, Debug, Default, ValueEnum)]
pub enum NeighbourOrderChoice {
    /// Newest observation first.
    #[default]
    Newest,
    /// Oldest observation first.
    Oldest,
    /// Strongest signal first.
    Strongest,
    /// Weakest signal first.
    Weakest,
}

/// Remote clock fields.
#[derive(Debug, Args)]
pub struct RemoteClockArgs {
    /// Target remote contact.
    pub contact: String,
    /// Synchronize remote time from this host.
    #[arg(long)]
    pub sync: bool,
}

/// Sensor operations.
#[derive(Debug, Subcommand)]
pub enum SensorCommand {
    /// Retrieve sensor telemetry.
    Telemetry {
        /// Contact exposing telemetry.
        contact: String,
    },
    /// Retrieve a sensor summary.
    Summary(SensorSummaryArgs),
    /// Read the sensor ACL (firmware requires an administrative session).
    Acl {
        /// Contact whose sensor ACL is read.
        contact: String,
    },
}

/// Sensor min/max/average query fields.
#[derive(Debug, Args)]
pub struct SensorSummaryArgs {
    /// Contact exposing sensor summary.
    pub contact: String,
    /// Oldest sample boundary in seconds before the remote clock.
    #[arg(long, default_value_t = 3600)]
    pub start_secs_ago: u32,
    /// Newest sample boundary in seconds before the remote clock.
    #[arg(long, default_value_t = 0)]
    pub end_secs_ago: u32,
}

/// Network operations.
#[derive(Debug, Subcommand)]
pub enum NetworkCommand {
    /// Discover contacts or neighbours from the network.
    Discover(NetworkDiscoverArgs),
    /// Trace a route through the network.
    Trace(NetworkTraceArgs),
    /// Inspect or update scope settings.
    Scope(NetworkScopeArgs),
}

/// Discovery fields.
#[derive(Debug, Args)]
pub struct NetworkDiscoverArgs {
    /// Optional contact-kind filter.
    #[arg(long, value_enum)]
    pub kind: Option<ContactKind>,
    /// Optional network scope filter.
    #[arg(long)]
    pub scope: Option<String>,
}

/// Trace fields.
#[derive(Debug, Args)]
pub struct NetworkTraceArgs {
    /// Contact name or unique public-key prefix.
    pub target: String,
    /// Hash bytes per path segment: 1, 2, or 3 (4 is parsed only for a reserved-mode diagnostic).
    #[arg(long, value_parser = parse_hash_bytes)]
    pub hash_bytes: Option<u8>,
}

/// Scope selection fields.
#[derive(Debug, Args)]
pub struct NetworkScopeArgs {
    /// Named `#region`, `default`, or `unscoped`.
    pub scope: Option<String>,
    /// Persist as the device default rather than a temporary client scope.
    #[arg(long)]
    pub set_default: bool,
}

/// Batch operations.
#[derive(Debug, Subcommand)]
pub enum BatchCommand {
    /// Execute one command per line.
    Run {
        /// Script file containing one command per line.
        file: PathBuf,
    },
    /// Apply a supported operation to filtered contacts.
    Contacts(BatchContactsArgs),
}

/// Filtered contact operation.
#[derive(Debug, Args)]
pub struct BatchContactsArgs {
    /// Documented filter expression compatible with useful `apply_to` cases.
    #[arg(long)]
    pub filter: String,
    /// Operation and arguments as one quoted value.
    pub operation: String,
    /// Resolve and print targets without changing device state.
    #[arg(long)]
    pub dry_run: bool,
}

/// Configuration operations.
#[derive(Debug, Subcommand)]
pub enum ConfigCommand {
    /// Display effective non-secret configuration.
    Show,
    /// Upgrade an older schema without discarding the source backup.
    Migrate,
    /// Validate and recover a malformed file with explicit confirmation.
    Repair,
    /// Import a safely recognized existing meshcore-cli device selection.
    ImportLegacy {
        /// Optional source meshcore-cli configuration path.
        path: Option<PathBuf>,
    },
}

/// Hook operations.
#[derive(Debug, Subcommand)]
pub enum HooksCommand {
    /// Validate imports, signatures and configuration without connecting.
    Validate {
        /// Optional hook test script or fixture path.
        path: Option<PathBuf>,
    },
    /// Send a fixture event to one hook and show its bounded result.
    Test {
        /// Event payload passed to hook test mode.
        event: String,
    },
    /// Show API version, enablement and configured hook policy.
    Status,
}

/// MQTT gateway operations.
#[derive(Debug, Subcommand)]
pub enum MqttCommand {
    /// Create or update broker settings without putting passwords in argv.
    Configure(MqttConfigureArgs),
    /// Validate DNS/TCP/TLS/auth without connecting a radio.
    Test,
    /// Run the foreground event bridge.
    Bridge,
    /// Display configuration and last known local status.
    Status,
}

/// MQTT configuration inputs.
#[derive(Debug, Args)]
#[allow(clippy::struct_excessive_bools)]
pub struct MqttConfigureArgs {
    /// Broker hostname.
    #[arg(long)]
    pub host: String,
    /// Broker port.
    #[arg(long, default_value_t = 8883)]
    pub port: u16,
    /// MQTT protocol version.
    #[arg(long, value_enum, default_value_t = MqttProtocolChoice::V311)]
    pub protocol: MqttProtocolChoice,
    /// MQTT publish and subscription quality of service.
    #[arg(long, value_enum, default_value_t = MqttQosChoice::AtLeastOnce)]
    pub qos: MqttQosChoice,
    /// Use plain TCP instead of certificate-validated TLS.
    #[arg(long)]
    pub no_tls: bool,
    /// PEM CA bundle to use instead of system trust roots.
    #[arg(long, value_name = "PATH", conflicts_with = "no_tls")]
    pub ca_file: Option<PathBuf>,
    /// PEM client certificate chain for mutual TLS.
    #[arg(
        long,
        value_name = "PATH",
        requires = "client_key",
        conflicts_with = "no_tls"
    )]
    pub client_certificate: Option<PathBuf>,
    /// PEM private key paired with --client-certificate.
    #[arg(
        long,
        value_name = "PATH",
        requires = "client_certificate",
        conflicts_with = "no_tls"
    )]
    pub client_key: Option<PathBuf>,
    /// Optional broker username.
    #[arg(long, conflicts_with = "clear_auth")]
    pub username: Option<String>,
    /// Read one bounded password from stdin instead of exposing it in argv.
    #[arg(long, requires = "username", conflicts_with = "clear_auth")]
    pub password_stdin: bool,
    /// Remove configured broker authentication and its managed credential.
    #[arg(long, conflicts_with_all = ["username", "password_stdin"])]
    pub clear_auth: bool,
    /// Prefix to prepend to broker topics.
    #[arg(long, default_value = "meshquill")]
    pub topic_prefix: String,
    /// Explicitly permit outbound direct/channel sends from allowlisted topics.
    #[arg(long)]
    pub allow_send: bool,
}

/// Completion generation input.
#[derive(Debug, Args)]
pub struct CompletionsArgs {
    /// Target shell for generated completion script.
    #[arg(value_enum)]
    pub shell: clap_complete::Shell,
}

/// Man-page generation input.
#[derive(Debug, Args)]
pub struct ManpagesArgs {
    /// Directory where man pages are emitted.
    #[arg(default_value = ".")]
    pub directory: PathBuf,
}

fn parse_duration(value: &str) -> Result<Duration, String> {
    humantime::parse_duration(value).map_err(|error| error.to_string())
}

fn parse_hash_bytes(value: &str) -> Result<u8, String> {
    match value {
        "1" => Ok(1),
        "2" => Ok(2),
        "3" => Ok(3),
        "4" => Ok(4),
        _ => Err("must be one of 1, 2, 3, or 4".to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use clap::{CommandFactory, Parser};

    use super::{Cli, ColorMode, Command, OutputMode};

    #[test]
    fn command_definition_is_internally_consistent() {
        Cli::command().debug_assert();
    }

    #[test]
    fn parses_script_safe_direct_send() {
        let cli = Cli::try_parse_from([
            "meshquill",
            "--profile",
            "field",
            "--output",
            "json",
            "--non-interactive",
            "--timeout",
            "1500ms",
            "send",
            "Alice",
            "hello",
            "--wait",
        ]);
        let cli = cli.unwrap_or_else(|error| panic!("valid CLI rejected: {error}"));
        assert_eq!(cli.output, OutputMode::Json);
        assert_eq!(cli.color, ColorMode::Auto);
        assert_eq!(cli.timeout, Duration::from_millis(1_500));
        assert!(cli.non_interactive);
        assert!(matches!(cli.command, Command::Send(args) if args.wait));
    }

    #[test]
    fn rejects_two_init_transports() {
        let result = Cli::try_parse_from([
            "meshquill",
            "init",
            "--name",
            "field",
            "--ble",
            "device",
            "--tcp",
            "localhost:5000",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn accepts_three_byte_trace_hashes_and_rejects_unknown_widths() {
        let valid = Cli::try_parse_from([
            "meshquill",
            "network",
            "trace",
            "Alice",
            "--hash-bytes",
            "3",
        ]);
        assert!(valid.is_ok());

        let invalid = Cli::try_parse_from([
            "meshquill",
            "network",
            "trace",
            "Alice",
            "--hash-bytes",
            "5",
        ]);
        assert!(invalid.is_err());
    }

    #[test]
    fn root_without_command_returns_help_error() {
        let result = Cli::try_parse_from(["meshquill"]);
        assert!(result.is_err());
    }
}

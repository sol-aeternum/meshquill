//! Remote administration and sensor query commands.

use std::{
    io::{self, IsTerminal, Read, Write},
    path::Path,
    sync::atomic::{AtomicU32, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use meshquill_core::{
    Contact, ContactRoute, ManagedClient, Path as MeshPath,
    remote::{
        AclResponse, AnonymousRequestKind, BasicResponse, BinaryRequestKind, NeighbourOrder,
        NeighbourPage, NeighbourQuery, OwnerResponse, RegionsResponse, SummaryResponse,
        TelemetrySample, acl_request_payload, parse_acl_payload, parse_basic_response,
        parse_neighbour_page, parse_owner_response, parse_regions_response, parse_summary_payload,
        parse_telemetry_payload, summary_request_payload,
    },
};
use meshquill_store::{SecretRef, SecretResolver, SystemSecretResolver};
use secrecy::{ExposeSecret, SecretString};
use serde::Serialize;
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

use crate::{
    args::{
        Cli, NeighbourOrderChoice, RemoteClockArgs, RemoteCommand, RemoteNeighboursArgs,
        RemoteRunArgs, SensorCommand, SensorSummaryArgs,
    },
    config::{SelectedProfile, select_profile},
    error::CliError,
    output::{ExitStatus, OutputWriter},
    runtime::{confirm, make_client, resolve_contact},
    workflow::CompanionSession,
};

const CREDENTIAL_SERVICE: &str = "meshquill.remote";
const MAX_PASSWORD_BYTES: usize = 1_024;
static NONCE_COUNTER: AtomicU32 = AtomicU32::new(1);

#[derive(Debug, Serialize)]
struct LoginReport {
    profile: String,
    contact: String,
    authenticated: bool,
    admin: bool,
    permissions: u8,
    acl_permissions: Option<u8>,
    server_timestamp: Option<u32>,
    firmware_version_level: Option<u8>,
    credential_saved: bool,
}

#[derive(Debug, Serialize)]
struct LogoutReport {
    profile: String,
    contact: String,
    logged_out: bool,
}

#[derive(Debug, Serialize)]
struct CredentialForgetReport {
    profile: String,
    contact: String,
    forgotten: bool,
}

#[derive(Debug, Serialize)]
struct RemoteRunReport {
    profile: String,
    contact: String,
    command_kind: String,
    destructive: bool,
    queued: bool,
    acknowledgement: String,
    suggested_timeout_ms: u32,
}

#[derive(Debug, Serialize)]
struct RemoteStatusReport {
    profile: String,
    contact: String,
    status: meshquill_core::RemoteStatus,
}

#[derive(Debug, Serialize)]
struct NeighboursReport {
    profile: String,
    contact: String,
    prefix_length: u8,
    page: NeighbourPage,
}

#[derive(Debug, Serialize)]
struct RegionsReport {
    profile: String,
    contact: String,
    response: RegionsResponse,
}

#[derive(Debug, Serialize)]
struct OwnerReport {
    profile: String,
    contact: String,
    response: OwnerResponse,
}

#[derive(Debug, Serialize)]
struct ClockReport {
    profile: String,
    contact: String,
    remote_clock: u32,
    feature_kind: u8,
    disabled: bool,
    synchronized: bool,
    acknowledgement: Option<String>,
}

#[derive(Debug, Serialize)]
struct TelemetryReport {
    profile: String,
    contact: String,
    samples: Vec<TelemetrySample>,
}

#[derive(Debug, Serialize)]
struct SummaryReport {
    profile: String,
    contact: String,
    start_secs_ago: u32,
    end_secs_ago: u32,
    summary: SummaryResponse,
}

#[derive(Debug, Serialize)]
struct AclReport {
    profile: String,
    contact: String,
    acl: AclResponse,
}

pub(crate) async fn remote<W: Write>(
    cli: &Cli,
    command: &RemoteCommand,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    match command {
        RemoteCommand::Login {
            contact,
            password_stdin,
            save,
        } => login(cli, contact, *password_stdin, *save, writer).await,
        RemoteCommand::Logout { contact } => logout(cli, contact, writer).await,
        RemoteCommand::Run(args) => run(cli, args, writer).await,
        RemoteCommand::Status { contact } => status(cli, contact, writer).await,
        RemoteCommand::Neighbours(args) => neighbours(cli, args, writer).await,
        RemoteCommand::Regions { contact } => regions(cli, contact, writer).await,
        RemoteCommand::Owner { contact } => owner(cli, contact, writer).await,
        RemoteCommand::Clock(args) => clock(cli, args, writer).await,
        RemoteCommand::CredentialsForget { contact } => {
            credentials_forget(cli, contact, writer).await
        }
    }
}

pub(crate) async fn sensor<W: Write>(
    cli: &Cli,
    command: &SensorCommand,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    match command {
        SensorCommand::Telemetry { contact } => telemetry(cli, contact, writer).await,
        SensorCommand::Summary(args) => summary(cli, args, writer).await,
        SensorCommand::Acl { contact } => acl(cli, contact, writer).await,
    }
}

async fn login<W: Write>(
    cli: &Cli,
    query: &str,
    password_stdin: bool,
    save: bool,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let (selected, client) = open(cli, "remote login").await?;
    let operation = async {
        let contact = load_contact(&client, query).await?;
        let account = credential_account(&selected.path, &selected.name, &contact);
        let password = resolve_login_password(cli, password_stdin, &account).await?;
        let session = client
            .login(contact.public_key.as_bytes(), password.expose_secret())
            .await
            .map_err(CliError::from)?;
        if save {
            store_credential(account, password).await?;
        }
        Ok(LoginReport {
            profile: selected.name.clone(),
            contact: contact.adv_name,
            authenticated: true,
            admin: session.is_admin(),
            permissions: session.permissions,
            acl_permissions: session.acl_permissions,
            server_timestamp: session.server_timestamp,
            firmware_version_level: session.firmware_version_level,
            credential_saved: save,
        })
    }
    .await;
    let report = shutdown_with(&client, operation).await?;
    let human = format!(
        "Authenticated to '{}'{}{}",
        report.contact,
        if report.admin {
            " as administrator"
        } else {
            ""
        },
        if report.credential_saved {
            " and saved the credential"
        } else {
            ""
        }
    );
    writer
        .result("remote_login", &report, &human)
        .map_err(CliError::from)
}

async fn logout<W: Write>(
    cli: &Cli,
    query: &str,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let (selected, client) = open(cli, "remote logout").await?;
    let operation = async {
        let contact = load_contact(&client, query).await?;
        require_session(&client, &contact).await?;
        client
            .logout(contact.public_key.as_bytes())
            .await
            .map_err(CliError::from)?;
        Ok(LogoutReport {
            profile: selected.name.clone(),
            contact: contact.adv_name,
            logged_out: true,
        })
    }
    .await;
    let report = shutdown_with(&client, operation).await?;
    let human = format!("Ended the remote session with '{}'", report.contact);
    writer
        .result("remote_logout", &report, &human)
        .map_err(CliError::from)
}

async fn credentials_forget<W: Write>(
    cli: &Cli,
    query: &str,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    confirm(cli, "delete the stored remote credential")?;
    let (selected, client) = open(cli, "remote credentials forget").await?;
    let operation = async {
        let contact = load_contact(&client, query).await?;
        let account = credential_account(&selected.path, &selected.name, &contact);
        delete_credential(account).await?;
        Ok(CredentialForgetReport {
            profile: selected.name.clone(),
            contact: contact.adv_name,
            forgotten: true,
        })
    }
    .await;
    let report = shutdown_with(&client, operation).await?;
    let human = format!("Deleted the stored credential for '{}'", report.contact);
    writer
        .result("remote_credential_forgotten", &report, &human)
        .map_err(CliError::from)
}

async fn run<W: Write>(
    cli: &Cli,
    args: &RemoteRunArgs,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let command = args.command.trim();
    if command.is_empty() || command.contains('\0') {
        return Err(CliError::new(
            ExitStatus::Usage,
            "remote command must be non-empty and must not contain NUL",
        ));
    }
    let command_kind = command
        .split_ascii_whitespace()
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    let read_only = is_read_only_command(command);
    if !read_only && !args.destructive {
        return Err(CliError::new(
            ExitStatus::Denied,
            "remote command is not in the conservative read-only allowlist",
        )
        .with_hint("Review it, add --destructive, and confirm with --yes for automation."));
    }
    if !read_only {
        confirm(cli, "send the explicitly marked destructive remote command")?;
    }

    let (selected, client) = open(cli, "remote run").await?;
    let operation = async {
        let contact = load_contact(&client, &args.contact).await?;
        require_session(&client, &contact).await?;
        let tracking = client
            .send_direct_command(&contact.public_key.as_bytes()[..6], 0, command)
            .await
            .map_err(CliError::from)?;
        Ok(RemoteRunReport {
            profile: selected.name.clone(),
            contact: contact.adv_name,
            command_kind,
            destructive: !read_only,
            queued: true,
            acknowledgement: hex::encode(tracking.ack_code),
            suggested_timeout_ms: tracking.timeout_ms,
        })
    }
    .await;
    let report = shutdown_with(&client, operation).await?;
    let human = format!(
        "Queued remote '{}' command for '{}' (ack {})",
        report.command_kind, report.contact, report.acknowledgement
    );
    writer
        .result("remote_command", &report, &human)
        .map_err(CliError::from)
}

async fn status<W: Write>(
    cli: &Cli,
    query: &str,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let (selected, client) = open(cli, "remote status").await?;
    let operation = async {
        let contact = load_contact(&client, query).await?;
        require_session(&client, &contact).await?;
        let status = client
            .remote_status(contact.public_key.as_bytes())
            .await
            .map_err(CliError::from)?;
        Ok(RemoteStatusReport {
            profile: selected.name.clone(),
            contact: contact.adv_name,
            status,
        })
    }
    .await;
    let report = shutdown_with(&client, operation).await?;
    let human = format!(
        "{}: {} mV, uptime {}s, RSSI {}, SNR {:.2} dB",
        report.contact,
        report.status.battery_mv,
        report.status.uptime_seconds,
        report.status.last_rssi,
        report.status.last_snr
    );
    writer
        .result("remote_status", &report, &human)
        .map_err(CliError::from)
}

async fn neighbours<W: Write>(
    cli: &Cli,
    args: &RemoteNeighboursArgs,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let order = match args.order {
        NeighbourOrderChoice::Newest => NeighbourOrder::Newest,
        NeighbourOrderChoice::Oldest => NeighbourOrder::Oldest,
        NeighbourOrderChoice::Strongest => NeighbourOrder::Strongest,
        NeighbourOrderChoice::Weakest => NeighbourOrder::Weakest,
    };
    let nonce = request_nonce()?;
    let payload = NeighbourQuery::new(args.count, args.offset, order, args.prefix_length, nonce)
        .encode()
        .map_err(remote_payload_error)?;

    let (selected, client) = open(cli, "remote neighbours").await?;
    let operation = async {
        let contact = load_contact(&client, &args.contact).await?;
        require_session(&client, &contact).await?;
        let response = client
            .binary_request(
                contact.public_key.as_bytes(),
                BinaryRequestKind::Neighbours.code(),
                &payload,
            )
            .await
            .map_err(CliError::from)?;
        let page = parse_neighbour_page(&response.payload, args.prefix_length)
            .map_err(remote_payload_error)?;
        Ok(NeighboursReport {
            profile: selected.name.clone(),
            contact: contact.adv_name,
            prefix_length: args.prefix_length,
            page,
        })
    }
    .await;
    let report = shutdown_with(&client, operation).await?;
    let human = format!(
        "{} returned {} of {} neighbours",
        report.contact, report.page.result_count, report.page.total_count
    );
    writer
        .result("remote_neighbours", &report, &human)
        .map_err(CliError::from)
}

async fn regions<W: Write>(
    cli: &Cli,
    query: &str,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let (selected, client) = open(cli, "remote regions").await?;
    let operation = async {
        let contact = load_contact(&client, query).await?;
        let response = anonymous(&client, &contact, AnonymousRequestKind::Regions).await?;
        let response = parse_regions_response(&response.payload).map_err(remote_payload_error)?;
        Ok(RegionsReport {
            profile: selected.name.clone(),
            contact: contact.adv_name,
            response,
        })
    }
    .await;
    let report = shutdown_with(&client, operation).await?;
    let human = format!("{}: {}", report.contact, report.response.names.join(", "));
    writer
        .result("remote_regions", &report, &human)
        .map_err(CliError::from)
}

async fn owner<W: Write>(
    cli: &Cli,
    query: &str,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let (selected, client) = open(cli, "remote owner").await?;
    let operation = async {
        let contact = load_contact(&client, query).await?;
        let response = anonymous(&client, &contact, AnonymousRequestKind::Owner).await?;
        let response = parse_owner_response(&response.payload).map_err(remote_payload_error)?;
        Ok(OwnerReport {
            profile: selected.name.clone(),
            contact: contact.adv_name,
            response,
        })
    }
    .await;
    let report = shutdown_with(&client, operation).await?;
    let human = format!(
        "{}: {} ({})",
        report.contact, report.response.name, report.response.owner
    );
    writer
        .result("remote_owner", &report, &human)
        .map_err(CliError::from)
}

async fn clock<W: Write>(
    cli: &Cli,
    args: &RemoteClockArgs,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let (selected, client) = open(cli, "remote clock").await?;
    let operation = async {
        let contact = load_contact(&client, &args.contact).await?;
        let response = anonymous(&client, &contact, AnonymousRequestKind::Basic).await?;
        let basic = parse_basic_response(&response.payload).map_err(remote_payload_error)?;
        let acknowledgement = if args.sync {
            require_session(&client, &contact).await?;
            let tracking = client
                .send_direct_command(&contact.public_key.as_bytes()[..6], 0, "clock sync")
                .await
                .map_err(CliError::from)?;
            Some(hex::encode(tracking.ack_code))
        } else {
            None
        };
        Ok(clock_report(
            &selected,
            contact,
            &basic,
            args.sync,
            acknowledgement,
        ))
    }
    .await;
    let report = shutdown_with(&client, operation).await?;
    let human = if report.synchronized {
        format!(
            "Remote clock for '{}' was {} before the synchronization command was queued",
            report.contact, report.remote_clock
        )
    } else {
        format!(
            "Remote clock for '{}': {}",
            report.contact, report.remote_clock
        )
    };
    writer
        .result("remote_clock", &report, &human)
        .map_err(CliError::from)
}

fn clock_report(
    selected: &SelectedProfile,
    contact: Contact,
    basic: &BasicResponse,
    synchronized: bool,
    acknowledgement: Option<String>,
) -> ClockReport {
    ClockReport {
        profile: selected.name.clone(),
        contact: contact.adv_name,
        remote_clock: basic.clock,
        feature_kind: basic.feature.kind,
        disabled: basic.feature.disabled,
        synchronized,
        acknowledgement,
    }
}

async fn telemetry<W: Write>(
    cli: &Cli,
    query: &str,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let (selected, client) = open(cli, "sensor telemetry").await?;
    let operation = async {
        let contact = load_contact(&client, query).await?;
        require_session(&client, &contact).await?;
        let response = client
            .binary_request(
                contact.public_key.as_bytes(),
                BinaryRequestKind::Telemetry.code(),
                &[],
            )
            .await
            .map_err(CliError::from)?;
        let samples = parse_telemetry_payload(&response.payload).map_err(remote_payload_error)?;
        Ok(TelemetryReport {
            profile: selected.name.clone(),
            contact: contact.adv_name,
            samples,
        })
    }
    .await;
    let report = shutdown_with(&client, operation).await?;
    let human = format!(
        "{} returned {} telemetry sample(s)",
        report.contact,
        report.samples.len()
    );
    writer
        .result("sensor_telemetry", &report, &human)
        .map_err(CliError::from)
}

async fn summary<W: Write>(
    cli: &Cli,
    args: &SensorSummaryArgs,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    if args.end_secs_ago > args.start_secs_ago {
        return Err(CliError::new(
            ExitStatus::Usage,
            "summary end-secs-ago must not exceed start-secs-ago",
        ));
    }
    let payload = summary_request_payload(args.start_secs_ago, args.end_secs_ago);
    let (selected, client) = open(cli, "sensor summary").await?;
    let operation = async {
        let contact = load_contact(&client, &args.contact).await?;
        require_session(&client, &contact).await?;
        let response = client
            .binary_request(
                contact.public_key.as_bytes(),
                BinaryRequestKind::Summary.code(),
                &payload,
            )
            .await
            .map_err(CliError::from)?;
        let summary = parse_summary_payload(&response.payload).map_err(remote_payload_error)?;
        Ok(SummaryReport {
            profile: selected.name.clone(),
            contact: contact.adv_name,
            start_secs_ago: args.start_secs_ago,
            end_secs_ago: args.end_secs_ago,
            summary,
        })
    }
    .await;
    let report = shutdown_with(&client, operation).await?;
    let human = format!(
        "{} returned {} min/max/average record(s)",
        report.contact,
        report.summary.entries.len()
    );
    writer
        .result("sensor_summary", &report, &human)
        .map_err(CliError::from)
}

async fn acl<W: Write>(
    cli: &Cli,
    query: &str,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let (selected, client) = open(cli, "sensor acl").await?;
    let operation = async {
        let contact = load_contact(&client, query).await?;
        require_session(&client, &contact).await?;
        let response = client
            .binary_request(
                contact.public_key.as_bytes(),
                BinaryRequestKind::Acl.code(),
                &acl_request_payload(),
            )
            .await
            .map_err(CliError::from)?;
        let acl = parse_acl_payload(&response.payload).map_err(remote_payload_error)?;
        Ok(AclReport {
            profile: selected.name.clone(),
            contact: contact.adv_name,
            acl,
        })
    }
    .await;
    let report = shutdown_with(&client, operation).await?;
    let human = format!(
        "{} returned {} ACL entr{}",
        report.contact,
        report.acl.entries.len(),
        if report.acl.entries.len() == 1 {
            "y"
        } else {
            "ies"
        }
    );
    writer
        .result("sensor_acl", &report, &human)
        .map_err(CliError::from)
}

async fn anonymous(
    client: &ManagedClient,
    contact: &Contact,
    kind: AnonymousRequestKind,
) -> Result<meshquill_core::BinaryResponse, CliError> {
    let path = MeshPath::try_from_bytes(&[]).map_err(CliError::from)?;
    client
        .anonymous_request(
            contact.public_key.as_bytes(),
            kind.code(),
            ContactRoute::Path {
                hash_mode: 0,
                hop_count: 0,
            },
            &path,
        )
        .await
        .map_err(CliError::from)
}

async fn open(
    cli: &Cli,
    operation: &'static str,
) -> Result<(SelectedProfile, CompanionSession), CliError> {
    let selected = select_profile(cli)?;
    let client = make_client(&selected)?;
    let (session, _) = CompanionSession::connect(&selected, client, operation).await?;
    Ok((selected, session))
}

async fn load_contact(client: &ManagedClient, query: &str) -> Result<Contact, CliError> {
    let contacts = client.list_contacts(None).await.map_err(CliError::from)?;
    resolve_contact(&contacts, query).cloned()
}

async fn require_session(client: &ManagedClient, contact: &Contact) -> Result<(), CliError> {
    if client
        .has_connection(contact.public_key.as_bytes())
        .await
        .map_err(CliError::from)?
    {
        return Ok(());
    }
    Err(CliError::new(
        ExitStatus::Authentication,
        format!(
            "no authenticated remote session exists for '{}'",
            contact.adv_name
        ),
    )
    .with_hint("Run `meshquill remote login CONTACT` first."))
}

async fn shutdown_with<T>(
    client: &CompanionSession,
    operation: Result<T, CliError>,
) -> Result<T, CliError> {
    client.finish(operation).await
}

async fn resolve_login_password(
    cli: &Cli,
    password_stdin: bool,
    account: &str,
) -> Result<SecretString, CliError> {
    if password_stdin {
        return read_password_stdin();
    }

    let reference = SecretRef::CredentialStore {
        service: CREDENTIAL_SERVICE.to_owned(),
        account: account.to_owned(),
    };
    let stored = tokio::task::spawn_blocking(move || SystemSecretResolver.resolve(&reference))
        .await
        .map_err(|_| credential_worker_error())?;
    if let Ok(password) = stored {
        return Ok(password);
    }

    if cli.non_interactive || !io::stdin().is_terminal() {
        return Err(CliError::new(
            ExitStatus::Authentication,
            "no stored remote credential is available in non-interactive mode",
        )
        .with_hint(
            "Pipe the password to `remote login --password-stdin`, optionally with --save.",
        ));
    }
    tokio::task::spawn_blocking(|| {
        rpassword::prompt_password("Remote password: ")
            .map(SecretString::from)
            .map_err(|_| {
                CliError::new(
                    ExitStatus::Authentication,
                    "could not read the remote password securely",
                )
            })
    })
    .await
    .map_err(|_| credential_worker_error())?
}

fn read_password_stdin() -> Result<SecretString, CliError> {
    let maximum = u64::try_from(MAX_PASSWORD_BYTES + 1).unwrap_or(u64::MAX);
    let mut input = io::stdin().lock().take(maximum);
    let mut bytes = Zeroizing::new(Vec::with_capacity(128));
    input.read_to_end(&mut bytes).map_err(|_| {
        CliError::new(
            ExitStatus::Authentication,
            "could not read the remote password from stdin",
        )
    })?;
    if bytes.len() > MAX_PASSWORD_BYTES {
        return Err(CliError::new(
            ExitStatus::Usage,
            format!("remote password exceeds the {MAX_PASSWORD_BYTES}-byte limit"),
        ));
    }
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
        if bytes.last() == Some(&b'\r') {
            bytes.pop();
        }
    }
    if bytes.is_empty() {
        return Err(CliError::new(
            ExitStatus::Usage,
            "remote password from stdin must not be empty",
        ));
    }
    let mut password = match String::from_utf8(std::mem::take(&mut *bytes)) {
        Ok(password) => Zeroizing::new(password),
        Err(error) => {
            let _invalid_password = Zeroizing::new(error.into_bytes());
            return Err(CliError::new(
                ExitStatus::Usage,
                "remote password from stdin must be valid UTF-8",
            ));
        }
    };
    if password.contains('\0') {
        return Err(CliError::new(
            ExitStatus::Usage,
            "remote password from stdin must not contain NUL",
        ));
    }
    Ok(SecretString::from(std::mem::take(&mut *password)))
}

async fn store_credential(account: String, password: SecretString) -> Result<(), CliError> {
    tokio::task::spawn_blocking(move || {
        SystemSecretResolver::set_credential(CREDENTIAL_SERVICE, &account, &password)
    })
    .await
    .map_err(|_| credential_worker_error())?
    .map_err(CliError::from)
}

async fn delete_credential(account: String) -> Result<(), CliError> {
    tokio::task::spawn_blocking(move || {
        SystemSecretResolver::delete_credential(CREDENTIAL_SERVICE, &account)
    })
    .await
    .map_err(|_| credential_worker_error())?
    .map_err(CliError::from)
}

fn credential_account(path: &Path, profile: &str, contact: &Contact) -> String {
    let mut digest = Sha256::new();
    digest.update(path.as_os_str().as_encoded_bytes());
    digest.update([0]);
    digest.update(profile.as_bytes());
    digest.update([0]);
    digest.update(contact.public_key.as_bytes());
    let digest = digest.finalize();
    format!("remote-{}", hex::encode(&digest[..16]))
}

fn credential_worker_error() -> CliError {
    CliError::new(
        ExitStatus::Authentication,
        "the operating-system credential worker was interrupted",
    )
}

fn request_nonce() -> Result<u32, CliError> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| {
        CliError::new(
            ExitStatus::Protocol,
            "system time is unavailable for remote request correlation",
        )
    })?;
    let mut digest = Sha256::new();
    digest.update(elapsed.as_nanos().to_le_bytes());
    digest.update(std::process::id().to_le_bytes());
    digest.update(NONCE_COUNTER.fetch_add(1, Ordering::Relaxed).to_le_bytes());
    let bytes = digest.finalize();
    let nonce = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    Ok(nonce.max(1))
}

fn remote_payload_error(_error: meshquill_core::remote::RemotePayloadError) -> CliError {
    CliError::new(
        ExitStatus::Protocol,
        "the remote node returned a malformed or unsupported payload",
    )
    .with_hint("Check the remote firmware version and retry the bounded query.")
}

fn is_read_only_command(command: &str) -> bool {
    matches!(
        command.trim().to_ascii_lowercase().as_str(),
        "?" | "help" | "info" | "status" | "stats" | "uptime" | "ver" | "version" | "clock"
    )
}

#[cfg(test)]
mod tests {
    use meshquill_core::{ContactRoute, ContactType, Path as MeshPath, PublicKey};
    use tempfile::tempdir;

    use super::{credential_account, is_read_only_command};

    fn contact() -> meshquill_core::Contact {
        meshquill_core::Contact {
            public_key: PublicKey::try_from_bytes(&[7; 32]).expect("valid key"),
            contact_type: ContactType::Repeater,
            flags: 0,
            route: ContactRoute::Flood,
            out_path: MeshPath::try_from_bytes(&[]).expect("empty path"),
            adv_name: "Repeater".to_owned(),
            last_advert: 0,
            adv_lat: 0.0,
            adv_lon: 0.0,
            lastmod: 0,
        }
    }

    #[test]
    fn conservative_command_classifier_requires_exact_read_only_commands() {
        assert!(is_read_only_command("ver"));
        assert!(is_read_only_command(" CLOCK "));
        assert!(!is_read_only_command("clock sync"));
        assert!(!is_read_only_command("set name demo"));
        assert!(!is_read_only_command("reboot"));
        assert!(!is_read_only_command("ver; reboot"));
    }

    #[test]
    fn credential_account_is_deterministic_and_identity_scoped() {
        let directory = tempdir().expect("temporary directory");
        let first = credential_account(&directory.path().join("config.toml"), "demo", &contact());
        let second = credential_account(&directory.path().join("config.toml"), "demo", &contact());
        let other_profile =
            credential_account(&directory.path().join("config.toml"), "other", &contact());
        assert_eq!(first, second);
        assert_ne!(first, other_profile);
        assert!(!first.contains("Repeater"));
    }
}

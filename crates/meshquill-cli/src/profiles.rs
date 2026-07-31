//! Stored profile listing and mutation commands.

use std::io::Write;

use meshquill_store::{Config, ConfigStore, LoadOutcome, LockedConfigStore, validate_identifier};
use serde::Serialize;

use crate::{
    args::{Cli, ProfileCommand, ProfileReconfigureArgs},
    config::{
        config_store, history_store_for_cli, load_unmodified, transport_from_options,
        transport_label,
    },
    error::CliError,
    output::{ExitStatus, OutputWriter},
    runtime::confirm,
};

const RENAME_WARNING: &str =
    "remote credential hashes are keyed outside the config and cannot be migrated automatically";

#[derive(Debug, Serialize)]
struct ProfileSummary {
    name: String,
    default: bool,
    transport: &'static str,
}

#[derive(Debug, Serialize)]
struct ProfilesReport {
    profiles: Vec<ProfileSummary>,
    default_profile: Option<String>,
}

#[derive(Debug, Serialize)]
struct ProfileReconfiguredReport {
    profile: String,
    transport: &'static str,
    default: bool,
}

#[derive(Debug, Serialize)]
struct ProfileRenamedReport {
    old: String,
    new: String,
    default: bool,
    history_migrated: bool,
    warning: &'static str,
}

#[derive(Debug, Serialize)]
struct ProfileDeletedReport {
    profile: String,
    default_cleared: bool,
    history_retained: bool,
    credentials_retained: bool,
}

#[derive(Debug, Serialize)]
struct ProfileDefaultSetReport {
    profile: String,
}

pub(crate) fn profiles<W: Write>(
    cli: &Cli,
    command: &ProfileCommand,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    match command {
        ProfileCommand::List => list(cli, writer),
        ProfileCommand::Reconfigure(args) => reconfigure(cli, args, writer),
        ProfileCommand::Rename { old, new } => rename(cli, old, new, writer),
        ProfileCommand::Delete { name } => delete(cli, name, writer),
        ProfileCommand::SetDefault { name } => set_default(cli, name, writer),
    }
}

fn list<W: Write>(cli: &Cli, writer: &mut OutputWriter<W>) -> Result<(), CliError> {
    let store = config_store(cli)?;
    let config = match load_unmodified(&store)? {
        LoadOutcome::Missing => return Err(missing_config(&store)),
        LoadOutcome::Loaded(config) | LoadOutcome::NeedsMigration(config) => config,
    };
    let profiles = config
        .device_profiles
        .iter()
        .map(|(name, profile)| ProfileSummary {
            name: name.clone(),
            default: config.default_profile.as_deref() == Some(name),
            transport: transport_label(&profile.transport),
        })
        .collect::<Vec<_>>();
    let human = if profiles.is_empty() {
        "No device profiles are configured.".to_owned()
    } else {
        profiles
            .iter()
            .map(|profile| {
                format!(
                    "{}\t{}{}",
                    profile.name,
                    profile.transport,
                    if profile.default { "\t(default)" } else { "" }
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    let report = ProfilesReport {
        profiles,
        default_profile: config.default_profile,
    };
    writer
        .result("profiles", &report, &human)
        .map_err(CliError::from)
}

fn reconfigure<W: Write>(
    cli: &Cli,
    args: &ProfileReconfigureArgs,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    validate_name(&args.name)?;
    let transport = transport_from_options(
        args.ble.as_deref(),
        args.serial.as_deref(),
        args.tcp.as_deref(),
        args.demo,
    )?;
    let transport_name = transport_label(&transport);
    let store = config_store(cli)?;
    let locked = store.lock_exclusive().map_err(CliError::from)?;
    let mut config = load_mutable_locked(&locked)?;
    let profile = config
        .device_profiles
        .get_mut(&args.name)
        .ok_or_else(|| profile_not_found(&args.name))?;
    profile.transport = transport;
    let default = config.default_profile.as_deref() == Some(args.name.as_str());
    locked.save(&config).map_err(CliError::from)?;
    let report = ProfileReconfiguredReport {
        profile: args.name.clone(),
        transport: transport_name,
        default,
    };
    writer
        .result(
            "profile_reconfigured",
            &report,
            &format!(
                "Reconfigured profile '{}' to use {}.",
                args.name, transport_name
            ),
        )
        .map_err(CliError::from)
}

fn rename<W: Write>(
    cli: &Cli,
    old: &str,
    new: &str,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    validate_name(old)?;
    validate_name(new)?;
    if old == new {
        return Err(CliError::new(
            ExitStatus::Usage,
            "the old and new profile names must differ",
        ));
    }
    let store = config_store(cli)?;
    let initial = load_mutable(&store)?;
    if !initial.device_profiles.contains_key(old) {
        return Err(profile_not_found(old));
    }
    if initial.device_profiles.contains_key(new) {
        return Err(CliError::new(
            ExitStatus::Denied,
            format!("profile '{new}' already exists"),
        ));
    }
    confirm(
        cli,
        &format!("rename profile '{old}' to '{new}' ({RENAME_WARNING})"),
    )?;

    let locked = store.lock_exclusive().map_err(CliError::from)?;
    let mut config = load_mutable_locked(&locked)?;
    if !config.device_profiles.contains_key(old) {
        return Err(profile_not_found(old));
    }
    if config.device_profiles.contains_key(new) {
        return Err(CliError::new(
            ExitStatus::Denied,
            format!("profile '{new}' already exists"),
        ));
    }

    let old_history = history_store_for_cli(cli, locked.path(), old, config.history.max_messages)?;
    let new_history = history_store_for_cli(cli, locked.path(), new, config.history.max_messages)?;

    let profile = config.device_profiles.remove(old).ok_or_else(|| {
        CliError::new(
            ExitStatus::Configuration,
            "the selected profile disappeared during the local rename operation",
        )
    })?;
    config.device_profiles.insert(new.to_owned(), profile);
    let default = config.default_profile.as_deref() == Some(old);
    if default {
        config.default_profile = Some(new.to_owned());
    }
    let history_migrated = old_history
        .move_to_with(&new_history, || locked.save(&config))
        .map_err(CliError::from)?;

    let report = ProfileRenamedReport {
        old: old.to_owned(),
        new: new.to_owned(),
        default,
        history_migrated,
        warning: RENAME_WARNING,
    };
    writer
        .result(
            "profile_renamed",
            &report,
            &format!("Renamed profile '{old}' to '{new}'. Warning: {RENAME_WARNING}."),
        )
        .map_err(CliError::from)
}

fn delete<W: Write>(cli: &Cli, name: &str, writer: &mut OutputWriter<W>) -> Result<(), CliError> {
    validate_name(name)?;
    let store = config_store(cli)?;
    if !load_mutable(&store)?.device_profiles.contains_key(name) {
        return Err(profile_not_found(name));
    }
    confirm(
        cli,
        &format!("delete profile '{name}' while retaining its local history and credentials"),
    )?;

    let locked = store.lock_exclusive().map_err(CliError::from)?;
    let mut config = load_mutable_locked(&locked)?;
    if !config.device_profiles.contains_key(name) {
        return Err(profile_not_found(name));
    }
    config.device_profiles.remove(name);
    let default_cleared = config.default_profile.as_deref() == Some(name);
    if default_cleared {
        config.default_profile = None;
    }
    locked.save(&config).map_err(CliError::from)?;
    let report = ProfileDeletedReport {
        profile: name.to_owned(),
        default_cleared,
        history_retained: true,
        credentials_retained: true,
    };
    writer
        .result(
            "profile_deleted",
            &report,
            &format!("Deleted profile '{name}'. Local history and credentials were retained."),
        )
        .map_err(CliError::from)
}

fn set_default<W: Write>(
    cli: &Cli,
    name: &str,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    validate_name(name)?;
    let store = config_store(cli)?;
    let locked = store.lock_exclusive().map_err(CliError::from)?;
    let mut config = load_mutable_locked(&locked)?;
    if !config.device_profiles.contains_key(name) {
        return Err(profile_not_found(name));
    }
    config.default_profile = Some(name.to_owned());
    locked.save(&config).map_err(CliError::from)?;
    let report = ProfileDefaultSetReport {
        profile: name.to_owned(),
    };
    writer
        .result(
            "profile_default_set",
            &report,
            &format!("Set profile '{name}' as the default."),
        )
        .map_err(CliError::from)
}

fn load_mutable(store: &ConfigStore) -> Result<Config, CliError> {
    let config = match load_unmodified(store)? {
        LoadOutcome::Missing => return Err(missing_config(store)),
        LoadOutcome::Loaded(config) => config,
        LoadOutcome::NeedsMigration(_) => {
            return Err(CliError::new(
                ExitStatus::Configuration,
                "configuration must be migrated before changing profiles",
            )
            .with_hint("Run `meshquill config migrate` first; it preserves a backup."));
        }
    };
    Ok(config)
}

fn load_mutable_locked(store: &LockedConfigStore<'_>) -> Result<Config, CliError> {
    let config = match store
        .load_with_overrides(&std::collections::HashMap::new())
        .map_err(CliError::from)?
    {
        LoadOutcome::Missing => {
            return Err(CliError::new(
                ExitStatus::Configuration,
                format!("configuration is missing at {}", store.path().display()),
            )
            .with_hint("Run `meshquill init` to create a profile."));
        }
        LoadOutcome::Loaded(config) => config,
        LoadOutcome::NeedsMigration(_) => {
            return Err(CliError::new(
                ExitStatus::Configuration,
                "configuration must be migrated before changing profiles",
            )
            .with_hint("Run `meshquill config migrate` first; it preserves a backup."));
        }
    };
    Ok(config)
}

fn missing_config(store: &ConfigStore) -> CliError {
    CliError::new(
        ExitStatus::Configuration,
        format!("configuration is missing at {}", store.path().display()),
    )
    .with_hint("Run `meshquill init` to create a profile.")
}

fn validate_name(name: &str) -> Result<(), CliError> {
    if validate_identifier(name) {
        Ok(())
    } else {
        Err(CliError::new(
            ExitStatus::Usage,
            "profile name must be a safe ASCII identifier of at most 64 bytes",
        ))
    }
}

fn profile_not_found(name: &str) -> CliError {
    CliError::new(
        ExitStatus::NotFound,
        format!("device profile '{name}' was not found"),
    )
    .with_hint("Run `meshquill profiles list` to list configured profiles.")
}

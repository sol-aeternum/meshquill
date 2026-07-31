//! Native command-line application surfaces for Meshquill.

use std::io::{IsTerminal, Write};

use clap::Parser;
use tracing_subscriber::EnvFilter;

/// Parsed arguments and command-surface declarations.
pub mod args;
mod batch_cli;
mod config;
mod error;
mod hooks_cli;
mod input;
mod interrupt;
mod mqtt_cli;
/// Stable output serialization and exit-code contracts.
pub mod output;
mod profiles;
mod reconnect;
mod remote_cli;
mod runtime;
mod transport;
mod workflow;

use args::{Cli, ColorMode};
use output::{ExitStatus, OutputWriter};

/// Enable native process-level interrupt delivery before starting CLI work.
///
/// The native binary invokes this before it starts asynchronous worker tasks.
/// Embedders that already own a runtime may omit it and use the portable
/// runtime signal handler instead.
#[doc(hidden)]
pub fn enable_process_interrupts() {
    interrupt::enable_process_interrupts();
}

/// Run one already-parsed CLI invocation and return its stable status.
///
/// Diagnostics are written to stderr. All result output passes through the
/// selected [`OutputWriter`] contract.
pub async fn run(cli: Cli) -> ExitStatus {
    initialize_diagnostics(&cli);
    let stdout = std::io::stdout();
    let mut writer = OutputWriter::new(cli.output, stdout.lock());
    match runtime::dispatch(&cli, &mut writer).await {
        Ok(()) => ExitStatus::Success,
        Err(error) if error.status() == ExitStatus::Success => ExitStatus::Success,
        Err(error) => {
            let mut stderr = std::io::stderr().lock();
            let _write_result = writeln!(stderr, "error: {}", error.message());
            if let Some(hint) = error.hint() {
                let _write_result = writeln!(stderr, "hint: {hint}");
            }
            error.status()
        }
    }
}

fn initialize_diagnostics(cli: &Cli) {
    let fallback = if cli.quiet {
        "off"
    } else {
        match cli.verbose {
            0 => "warn",
            1 => "info",
            2 => "debug",
            _ => "trace",
        }
    };
    let filter = if cli.quiet {
        EnvFilter::new("off")
    } else {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(fallback))
    };
    let ansi = std::io::stderr().is_terminal() && cli.color != ColorMode::Never;
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(ansi)
        .with_target(cli.verbose >= 2)
        .with_writer(std::io::stderr)
        .try_init();
}

/// Parse process arguments without terminating the process, then run the CLI.
pub async fn run_from_env() -> ExitStatus {
    match Cli::try_parse() {
        Ok(cli) => run(cli).await,
        Err(error) => {
            let status = if matches!(
                error.kind(),
                clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion
            ) {
                ExitStatus::Success
            } else {
                ExitStatus::Usage
            };
            let _print_result = error.print();
            status
        }
    }
}

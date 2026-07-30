//! Binary entrypoint for Meshquill CLI.

use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    meshquill::run_from_env().await.process()
}

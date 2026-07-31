//! Binary entrypoint for Meshquill CLI.

use std::process::ExitCode;

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    meshquill::enable_process_interrupts();
    meshquill::run_from_env().await.process()
}

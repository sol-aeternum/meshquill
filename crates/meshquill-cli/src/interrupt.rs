use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::{
    future::{Future as _, poll_fn},
    task::Poll,
};

#[cfg(any(target_os = "linux", target_os = "android"))]
use nix::sys::{
    signal::{SigSet, Signal},
    signalfd::{SfdFlags, SignalFd},
};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{error::CliError, output::ExitStatus};

#[cfg(any(target_os = "linux", target_os = "android"))]
static PROCESS_SIGNAL_FD_ENABLED: AtomicBool = AtomicBool::new(false);

pub(crate) struct InterruptWatcher {
    token: CancellationToken,
    handler_failed: Arc<AtomicBool>,
    task: JoinHandle<()>,
}

impl InterruptWatcher {
    pub(crate) async fn install() -> Self {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if PROCESS_SIGNAL_FD_ENABLED.load(Ordering::Acquire) {
            return Self::install_signal_fd();
        }

        Self::install_tokio().await
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn install_signal_fd() -> Self {
        let token = CancellationToken::new();
        let handler_failed = Arc::new(AtomicBool::new(false));
        let mut signals = SigSet::empty();
        signals.add(Signal::SIGINT);
        let signal_fd = signals.thread_block().and_then(|()| {
            SignalFd::with_flags(&signals, SfdFlags::SFD_NONBLOCK | SfdFlags::SFD_CLOEXEC)
        });
        let task = if let Ok(signal_fd) = signal_fd {
            tokio::spawn(wait_for_signal_fd(
                token.clone(),
                Arc::clone(&handler_failed),
                signal_fd,
            ))
        } else {
            handler_failed.store(true, Ordering::Release);
            token.cancel();
            tokio::spawn(async {})
        };
        Self {
            token,
            handler_failed,
            task,
        }
    }

    async fn install_tokio() -> Self {
        let token = CancellationToken::new();
        let task_token = token.clone();
        let handler_failed = Arc::new(AtomicBool::new(false));
        let task_failed = Arc::clone(&handler_failed);
        let (ready_tx, ready_rx) = oneshot::channel();
        let task = tokio::spawn(wait_for_tokio_interrupt(task_token, task_failed, ready_tx));
        let watcher = Self {
            token,
            handler_failed,
            task,
        };
        if ready_rx.await.is_err() {
            watcher.handler_failed.store(true, Ordering::Release);
            watcher.token.cancel();
        }
        watcher
    }

    pub(crate) fn token(&self) -> &CancellationToken {
        &self.token
    }

    pub(crate) async fn cancelled(&self) {
        self.token.cancelled().await;
    }

    pub(crate) fn error(&self) -> CliError {
        if self.handler_failed.load(Ordering::Acquire) {
            CliError::new(
                ExitStatus::Interrupted,
                "could not install the interrupt handler",
            )
        } else {
            CliError::new(ExitStatus::Interrupted, "interrupted by user")
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
pub(crate) fn enable_process_interrupts() {
    PROCESS_SIGNAL_FD_ENABLED.store(true, Ordering::Release);
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
pub(crate) const fn enable_process_interrupts() {}

#[cfg(any(target_os = "linux", target_os = "android"))]
async fn wait_for_signal_fd(
    token: CancellationToken,
    handler_failed: Arc<AtomicBool>,
    signal_fd: SignalFd,
) {
    loop {
        match signal_fd.read_signal() {
            Ok(Some(_)) => break,
            Ok(None) => tokio::time::sleep(std::time::Duration::from_millis(25)).await,
            Err(_) => {
                handler_failed.store(true, Ordering::Release);
                break;
            }
        }
    }
    token.cancel();
}

async fn wait_for_tokio_interrupt(
    token: CancellationToken,
    handler_failed: Arc<AtomicBool>,
    ready: oneshot::Sender<()>,
) {
    let signal = tokio::signal::ctrl_c();
    tokio::pin!(signal);
    let immediate = poll_fn(|context| match signal.as_mut().poll(context) {
        Poll::Ready(result) => Poll::Ready(Some(result)),
        Poll::Pending => Poll::Ready(None),
    })
    .await;
    let _ = ready.send(());
    let result = match immediate {
        Some(result) => result,
        None => signal.await,
    };
    if result.is_err() {
        handler_failed.store(true, Ordering::Release);
    }
    token.cancel();
}

impl Drop for InterruptWatcher {
    fn drop(&mut self) {
        self.task.abort();
    }
}

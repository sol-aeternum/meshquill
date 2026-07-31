use std::time::Duration;

use meshquill_core::{CoreError, ManagedClient, SelfInfo, TransportError};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::{error::CliError, output::ExitStatus};

pub(crate) const DEVICE_RECONNECT_ATTEMPTS: usize = 3;

#[derive(Clone, Copy, Debug)]
pub(crate) struct ReconnectPolicy {
    retry_delay: Duration,
    maximum_delay: Duration,
}

impl ReconnectPolicy {
    pub(crate) const fn new(retry_delay: Duration, maximum_delay: Duration) -> Self {
        Self {
            retry_delay,
            maximum_delay,
        }
    }

    fn delay_before_attempt(self, attempt: usize) -> Duration {
        debug_assert!(attempt > 0);
        let shift = u32::try_from(attempt.saturating_sub(1)).unwrap_or(u32::MAX);
        let multiplier = 1_u32.checked_shl(shift).unwrap_or(u32::MAX);
        self.retry_delay
            .saturating_mul(multiplier)
            .min(self.maximum_delay)
    }
}

/// Re-establishes only the companion session; mutating commands are never replayed here.
pub(crate) async fn reconnect_device(
    client: &ManagedClient,
    policy: ReconnectPolicy,
    cancellation: &CancellationToken,
) -> Result<SelfInfo, CliError> {
    for attempt in 0..DEVICE_RECONNECT_ATTEMPTS {
        if attempt > 0 {
            tokio::select! {
                () = sleep(policy.delay_before_attempt(attempt)) => {}
                () = cancellation.cancelled() => return Err(interrupted()),
            }
        }

        let result = tokio::select! {
            result = client.reconnect() => result,
            () = cancellation.cancelled() => {
                client.cancel_pending_operations();
                return Err(interrupted());
            },
        };
        match result {
            Ok(info) => return Ok(info),
            Err(error)
                if reconnect_attempt_is_retryable(&error)
                    && attempt.saturating_add(1) < DEVICE_RECONNECT_ATTEMPTS =>
            {
                tracing::warn!(
                    attempt = attempt.saturating_add(1),
                    "device reconnect attempt failed"
                );
            }
            Err(error) => return Err(CliError::from(error)),
        }
    }

    Err(CliError::new(
        ExitStatus::Connection,
        "bounded companion reconnect attempts were exhausted",
    ))
}

pub(crate) fn reconnect_trigger(error: &CoreError) -> bool {
    matches!(
        error,
        CoreError::Disconnected
            | CoreError::Transport(
                TransportError::NotConnected
                    | TransportError::Closed
                    | TransportError::ReconnectFailed { .. }
                    | TransportError::Io(_)
            )
    )
}

fn reconnect_attempt_is_retryable(error: &CoreError) -> bool {
    matches!(
        error,
        CoreError::Disconnected
            | CoreError::Timeout
            | CoreError::Transport(
                TransportError::NotConnected
                    | TransportError::Closed
                    | TransportError::Timeout
                    | TransportError::ReconnectFailed { .. }
                    | TransportError::Io(_)
            )
    )
}

fn interrupted() -> CliError {
    CliError::new(ExitStatus::Interrupted, "interrupted by user")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_delays_are_exponential_and_capped() {
        let policy = ReconnectPolicy::new(Duration::from_millis(75), Duration::from_millis(100));
        assert_eq!(policy.delay_before_attempt(1), Duration::from_millis(75));
        assert_eq!(policy.delay_before_attempt(2), Duration::from_millis(100));
    }

    #[test]
    fn stopped_actor_and_unsupported_reconnect_are_terminal() {
        assert!(!reconnect_attempt_is_retryable(&CoreError::ActorStopped));
        assert!(!reconnect_trigger(&CoreError::ActorStopped));
        assert!(!reconnect_attempt_is_retryable(&CoreError::Transport(
            TransportError::ReconnectUnsupported
        )));
        assert!(reconnect_attempt_is_retryable(&CoreError::Timeout));
    }
}

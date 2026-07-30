use std::time::Duration;

use thiserror::Error;

use crate::config::{MAX_RECONNECT_DELAY_MS, ReconnectConfig, milliseconds};

/// Stateful bounded exponential reconnect delay generator.
#[derive(Clone, Debug)]
pub struct ExponentialBackoff {
    config: ReconnectConfig,
    next_delay_ms: u64,
}

impl ExponentialBackoff {
    /// Creates a generator after independently checking its arithmetic bounds.
    ///
    /// # Errors
    ///
    /// Returns [`BackoffError`] for a zero initial delay, an inverted or excessive
    /// maximum, or a multiplier below two.
    pub fn new(config: ReconnectConfig) -> Result<Self, BackoffError> {
        if config.initial_delay_ms == 0 {
            return Err(BackoffError::ZeroInitialDelay);
        }
        if config.max_delay_ms < config.initial_delay_ms {
            return Err(BackoffError::MaximumBeforeInitial);
        }
        if config.max_delay_ms > MAX_RECONNECT_DELAY_MS {
            return Err(BackoffError::MaximumExceedsLimit {
                configured_ms: config.max_delay_ms,
                maximum_ms: MAX_RECONNECT_DELAY_MS,
            });
        }
        if config.multiplier < 2 {
            return Err(BackoffError::InvalidMultiplier);
        }
        Ok(Self {
            config,
            next_delay_ms: config.initial_delay_ms,
        })
    }

    /// Returns the current delay and advances to the next bounded delay.
    pub fn next_delay(&mut self) -> Duration {
        let current = self.next_delay_ms;
        self.next_delay_ms = current
            .saturating_mul(u64::from(self.config.multiplier))
            .min(self.config.max_delay_ms);
        milliseconds(current)
    }

    /// Restores the initial delay after a successful connection.
    pub fn reset(&mut self) {
        self.next_delay_ms = self.config.initial_delay_ms;
    }
}

/// Invalid reconnect backoff settings.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum BackoffError {
    /// A retry loop must yield for a positive duration.
    #[error("MQTT reconnect initial delay must be non-zero")]
    ZeroInitialDelay,
    /// The maximum cannot be smaller than the initial delay.
    #[error("MQTT reconnect maximum delay is below the initial delay")]
    MaximumBeforeInitial,
    /// The maximum exceeds the crate-wide hard reconnect bound.
    #[error(
        "MQTT reconnect maximum delay {configured_ms} ms exceeds the {maximum_ms} ms hard limit"
    )]
    MaximumExceedsLimit {
        /// Requested maximum delay in milliseconds.
        configured_ms: u64,
        /// Crate-wide hard maximum in milliseconds.
        maximum_ms: u64,
    },
    /// Exponential growth needs an integer multiplier of at least two.
    #[error("MQTT reconnect multiplier must be at least 2")]
    InvalidMultiplier,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_saturates_and_resets() {
        let mut backoff = ExponentialBackoff::new(ReconnectConfig {
            initial_delay_ms: 100,
            max_delay_ms: 450,
            multiplier: 2,
        })
        .expect("valid backoff");

        assert_eq!(backoff.next_delay(), Duration::from_millis(100));
        assert_eq!(backoff.next_delay(), Duration::from_millis(200));
        assert_eq!(backoff.next_delay(), Duration::from_millis(400));
        assert_eq!(backoff.next_delay(), Duration::from_millis(450));
        assert_eq!(backoff.next_delay(), Duration::from_millis(450));

        backoff.reset();
        assert_eq!(backoff.next_delay(), Duration::from_millis(100));
    }

    #[test]
    fn large_multiplier_saturates_at_the_hard_bounded_maximum() {
        let mut backoff = ExponentialBackoff::new(ReconnectConfig {
            initial_delay_ms: MAX_RECONNECT_DELAY_MS - 1,
            max_delay_ms: MAX_RECONNECT_DELAY_MS,
            multiplier: u32::MAX,
        })
        .expect("valid backoff");
        assert_eq!(
            backoff.next_delay(),
            Duration::from_millis(MAX_RECONNECT_DELAY_MS - 1)
        );
        assert_eq!(
            backoff.next_delay(),
            Duration::from_millis(MAX_RECONNECT_DELAY_MS)
        );
    }

    #[test]
    fn maximum_above_the_hard_limit_is_rejected() {
        let result = ExponentialBackoff::new(ReconnectConfig {
            initial_delay_ms: 1,
            max_delay_ms: MAX_RECONNECT_DELAY_MS + 1,
            multiplier: 2,
        });
        assert!(matches!(
            result,
            Err(BackoffError::MaximumExceedsLimit {
                configured_ms,
                maximum_ms,
            }) if configured_ms == MAX_RECONNECT_DELAY_MS + 1
                && maximum_ms == MAX_RECONNECT_DELAY_MS
        ));
    }
}

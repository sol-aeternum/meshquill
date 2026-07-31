use std::time::Duration;

use meshquill_core::MAX_OPERATION_TIMEOUT;
use pyo3::prelude::*;

use crate::errors::InvalidArgumentError;

pub(crate) fn duration_from_seconds(value: f64, field: &str) -> PyResult<Duration> {
    if !value.is_finite() || value <= 0.0 {
        return Err(InvalidArgumentError::new_err(format!(
            "{field} must be a finite number greater than zero"
        )));
    }
    let duration = Duration::try_from_secs_f64(value).map_err(|_| {
        InvalidArgumentError::new_err(format!("{field} is outside the supported duration range"))
    })?;
    if duration > MAX_OPERATION_TIMEOUT {
        return Err(InvalidArgumentError::new_err(format!(
            "{field} must not exceed 86400 seconds"
        )));
    }
    Ok(duration)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn python_duration_conversion_enforces_finite_24_hour_bound() {
        assert_eq!(
            duration_from_seconds(86_400.0, "timeout").expect("maximum timeout"),
            MAX_OPERATION_TIMEOUT
        );
        assert!(duration_from_seconds(86_400.001, "timeout").is_err());
        assert!(duration_from_seconds(0.0, "timeout").is_err());
        assert!(duration_from_seconds(f64::INFINITY, "timeout").is_err());
        assert!(duration_from_seconds(f64::NAN, "timeout").is_err());
    }
}

use std::time::Duration;

use pyo3::prelude::*;

use crate::errors::InvalidArgumentError;

pub(crate) fn duration_from_seconds(value: f64, field: &str) -> PyResult<Duration> {
    if !value.is_finite() || value <= 0.0 {
        return Err(InvalidArgumentError::new_err(format!(
            "{field} must be a finite number greater than zero"
        )));
    }
    Duration::try_from_secs_f64(value).map_err(|_| {
        InvalidArgumentError::new_err(format!("{field} is outside the supported duration range"))
    })
}

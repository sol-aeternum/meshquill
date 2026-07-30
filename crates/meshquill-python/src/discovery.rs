use pyo3::prelude::*;

use crate::errors::discovery_error;
use crate::models::PyDiscoveredDevice;
use crate::util::duration_from_seconds;

/// Scan for `MeshCore` Nordic-UART BLE devices during a bounded observation window.
#[pyfunction(name = "discover_ble", signature = (*, timeout=5.0))]
pub(crate) fn discover_ble_py(py: Python<'_>, timeout: f64) -> PyResult<Bound<'_, PyAny>> {
    let timeout = duration_from_seconds(timeout, "timeout")?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let devices = meshquill_transport::discover_ble(timeout)
            .await
            .map_err(|error| discovery_error(&error))?;
        Ok(devices
            .into_iter()
            .map(PyDiscoveredDevice::from)
            .collect::<Vec<_>>())
    })
}

/// Enumerate serial ports without blocking the Python event-loop thread.
#[pyfunction(name = "discover_serial")]
pub(crate) fn discover_serial_py(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let devices = meshquill_transport::discover_serial_async()
            .await
            .map_err(|error| discovery_error(&error))?;
        Ok(devices
            .into_iter()
            .map(PyDiscoveredDevice::from)
            .collect::<Vec<_>>())
    })
}

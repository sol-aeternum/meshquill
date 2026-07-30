//! TCP transport with MeshCore outer stream framing.

use std::{fmt, io, time::Duration};

use async_trait::async_trait;
use meshquill_core::{
    TransportError,
    transport::{ReconnectableTransport, Transport, TransportKind},
};
use tokio::{io::AsyncWriteExt, net::TcpStream, time};

use crate::{
    discovery::{TargetError, TransportTarget, format_tcp_endpoint, validate_nonzero},
    framed::{
        FramedReadState, invalidate_on_terminal_read_error, validate_payload, write_framed_bounded,
    },
};

/// A logical-packet transport over a framed TCP stream.
///
/// The connection target is retained across disconnects so [`ReconnectableTransport`] reconnects
/// to exactly the same host and port. The configured timeout bounds provider connect, framed-write,
/// and shutdown operations. No writes are queued or replayed across a reconnect.
pub struct TcpTransport {
    host: String,
    port: u16,
    connect_timeout: Duration,
    stream: Option<TcpStream>,
    read_state: FramedReadState,
}

impl TcpTransport {
    /// Creates a disconnected TCP transport. `connect_timeout` is retained for API compatibility
    /// and bounds each provider connect, framed-write, and shutdown operation.
    ///
    /// # Errors
    /// Returns [`TargetError`] when the host is blank, the port is zero, or the timeout is zero.
    pub fn new(
        host: impl Into<String>,
        port: u16,
        connect_timeout: Duration,
    ) -> Result<Self, TargetError> {
        let host = host.into();
        let target = TransportTarget::Tcp {
            host: host.clone(),
            port,
        };
        target.validate()?;
        validate_timeout(connect_timeout)?;

        Ok(Self {
            host,
            port,
            connect_timeout,
            stream: None,
            read_state: FramedReadState::new(),
        })
    }

    /// Returns the configured hostname or address.
    #[must_use]
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns the configured TCP port.
    #[must_use]
    pub const fn port(&self) -> u16 {
        self.port
    }

    /// Returns the configured provider-operation timeout for connect, write, and shutdown.
    #[must_use]
    pub const fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    /// Returns a persistable copy of the connection target.
    #[must_use]
    pub fn target(&self) -> TransportTarget {
        TransportTarget::Tcp {
            host: self.host.clone(),
            port: self.port,
        }
    }

    /// Reports whether this value currently owns an open socket.
    ///
    /// A peer can close TCP asynchronously, so a definitive liveness result requires an I/O or
    /// application-level probe.
    #[must_use]
    pub const fn is_connected(&self) -> bool {
        self.stream.is_some()
    }

    fn endpoint(&self) -> String {
        format_tcp_endpoint(&self.host, self.port)
    }
}

impl fmt::Debug for TcpTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TcpTransport")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("connect_timeout", &self.connect_timeout)
            .field("connected", &self.stream.is_some())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl Transport for TcpTransport {
    fn kind(&self) -> TransportKind {
        TransportKind::Tcp
    }

    async fn connect(&mut self) -> Result<(), TransportError> {
        if self.stream.is_some() {
            return Err(TransportError::Io(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "TCP transport to {} is already connected; disconnect before connecting again",
                    self.endpoint()
                ),
            )));
        }

        self.read_state.reset();
        let endpoint = self.endpoint();
        let stream = time::timeout(
            self.connect_timeout,
            TcpStream::connect((self.host.as_str(), self.port)),
        )
        .await
        .map_err(|_elapsed| timeout_error("TCP connect", &endpoint, self.connect_timeout))?
        .map_err(|error| contextual_io(&error, "TCP connect", &endpoint))?;

        stream
            .set_nodelay(true)
            .map_err(|error| contextual_io(&error, "configure TCP_NODELAY for", &endpoint))?;
        self.stream = Some(stream);
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<(), TransportError> {
        self.read_state.reset();
        let Some(mut stream) = self.stream.take() else {
            return Ok(());
        };

        let endpoint = self.endpoint();
        time::timeout(self.connect_timeout, stream.shutdown())
            .await
            .map_err(|_elapsed| timeout_error("TCP shutdown", &endpoint, self.connect_timeout))?
            .map_err(|error| contextual_io(&error, "TCP shutdown for", &self.endpoint()))
            .map_err(TransportError::Io)
    }

    async fn write(&mut self, payload: &[u8]) -> Result<(), TransportError> {
        validate_payload(payload)?;
        let endpoint = self.endpoint();
        write_framed_bounded(
            &mut self.stream,
            &mut self.read_state,
            payload,
            self.connect_timeout,
        )
        .await
        .map_err(|error| contextual_transport(error, "TCP write to", &endpoint))
    }

    async fn read(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
        let endpoint = self.endpoint();
        let result = match self.stream.as_mut() {
            Some(stream) => self.read_state.read_from(stream).await,
            None => Err(TransportError::NotConnected),
        };

        match result {
            Ok(None) => {
                self.stream = None;
                self.read_state.reset();
                Ok(None)
            }
            Ok(packet) => Ok(packet),
            Err(error) => {
                invalidate_on_terminal_read_error(&mut self.stream, &mut self.read_state, &error);
                Err(contextual_transport(error, "TCP read from", &endpoint))
            }
        }
    }
}

impl ReconnectableTransport for TcpTransport {}

fn validate_timeout(timeout: Duration) -> Result<(), TargetError> {
    validate_nonzero("connect_timeout", timeout.as_nanos())
}

fn timeout_error(operation: &str, endpoint: &str, timeout: Duration) -> TransportError {
    TransportError::Io(io::Error::new(
        io::ErrorKind::TimedOut,
        format!("{operation} for {endpoint} timed out after {timeout:?}"),
    ))
}

fn contextual_transport(error: TransportError, operation: &str, endpoint: &str) -> TransportError {
    match error {
        TransportError::Io(error) => TransportError::Io(contextual_io(&error, operation, endpoint)),
        other => other,
    }
}

fn contextual_io(error: &io::Error, operation: &str, endpoint: &str) -> io::Error {
    io::Error::new(
        error.kind(),
        format!("{operation} {endpoint} failed: {error}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructor_validates_target_and_exposes_it() {
        assert!(TcpTransport::new("", 5_000, Duration::from_secs(1)).is_err());
        assert!(TcpTransport::new("localhost", 0, Duration::from_secs(1)).is_err());
        assert!(TcpTransport::new("localhost", 5_000, Duration::ZERO).is_err());

        let transport = TcpTransport::new("localhost", 5_000, Duration::from_secs(2))
            .expect("valid TCP target");
        assert_eq!(transport.host(), "localhost");
        assert_eq!(transport.port(), 5_000);
        assert_eq!(transport.connect_timeout(), Duration::from_secs(2));
        assert_eq!(
            transport.target(),
            TransportTarget::Tcp {
                host: "localhost".to_string(),
                port: 5_000,
            }
        );
    }
}

use std::collections::VecDeque;

use async_trait::async_trait;

use crate::error::TransportError;

/// Transport back-end identity used by diagnostics and logging.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportKind {
    /// Synthetic in-memory transport for tests and unit fixtures.
    Scripted,
    /// Serial-based companion transport (not implemented in this crate layer).
    Serial,
    /// Bluetooth LE companion transport (not implemented in this crate layer).
    Ble,
    /// TCP companion transport (not implemented in this crate layer).
    Tcp,
    /// Fallback for unknown/legacy transport implementations.
    Unknown,
}

/// Minimal async transport abstraction used by the core client.
#[async_trait]
pub trait Transport: Send + Unpin {
    /// Returns the transport kind for diagnostics.
    fn kind(&self) -> TransportKind;

    /// Opens the transport and marks it ready for IO.
    async fn connect(&mut self) -> Result<(), TransportError>;

    /// Closes the transport.
    async fn disconnect(&mut self) -> Result<(), TransportError>;

    /// Writes one logical companion packet without transport-specific framing.
    ///
    /// The caller retains ownership so secret-bearing command buffers can be zeroized immediately
    /// after the write future completes. Implementations must not retain this slice after return.
    async fn write(&mut self, payload: &[u8]) -> Result<(), TransportError>;

    /// Reads one logical companion packet without transport-specific framing.
    ///
    /// `None` means the peer closed the connection cleanly. Calling this method
    /// before connecting returns [`TransportError::NotConnected`]. Implementations must make
    /// this operation cancellation safe: dropping the returned future before it resolves must
    /// not consume a logical packet. This lets the managed client interrupt an idle read when a
    /// command arrives without losing protocol data.
    async fn read(&mut self) -> Result<Option<Vec<u8>>, TransportError>;
}

/// Transport abstraction for transports that can perform a reconnect attempt.
#[async_trait]
pub trait ReconnectableTransport: Transport {
    /// Reopens the transport after a failure.
    async fn reconnect(&mut self) -> Result<(), TransportError> {
        self.disconnect().await?;
        self.connect().await
    }
}

/// In-memory scripted transport for deterministic tests and replay scenarios.
#[derive(Debug)]
pub struct ScriptedTransport {
    kind: TransportKind,
    connected: bool,
    incoming: VecDeque<Vec<u8>>,
    outgoing: VecDeque<Vec<u8>>,
}

impl ScriptedTransport {
    /// Creates a disconnected scripted transport.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a connected scripted transport seeded with initial inbound frames.
    pub fn with_inbound_frames<I, B>(frames: I) -> Self
    where
        I: IntoIterator<Item = B>,
        B: Into<Vec<u8>>,
    {
        Self {
            kind: TransportKind::Scripted,
            connected: true,
            incoming: frames.into_iter().map(Into::into).collect(),
            outgoing: VecDeque::new(),
        }
    }

    /// Enqueues one inbound companion payload.
    pub fn enqueue_inbound(&mut self, payload: impl Into<Vec<u8>>) {
        self.incoming.push_back(payload.into());
    }

    /// Returns the next queued outbound payload, if any.
    pub fn pop_outbound(&mut self) -> Option<Vec<u8>> {
        self.outgoing.pop_front()
    }

    /// Returns all outbound payloads recorded so far.
    #[must_use]
    pub fn outbound_frames(&self) -> Vec<Vec<u8>> {
        self.outgoing.iter().cloned().collect()
    }

    /// Clears the outbound history and returns the consumed payloads.
    pub fn drain_outbound(&mut self) -> Vec<Vec<u8>> {
        self.outgoing.drain(..).collect()
    }

    /// Indicates whether the transport is connected.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.connected
    }
}

impl Default for ScriptedTransport {
    fn default() -> Self {
        Self {
            kind: TransportKind::Scripted,
            connected: false,
            incoming: VecDeque::new(),
            outgoing: VecDeque::new(),
        }
    }
}

#[async_trait]
impl Transport for ScriptedTransport {
    fn kind(&self) -> TransportKind {
        self.kind
    }

    async fn connect(&mut self) -> Result<(), TransportError> {
        self.connected = true;
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<(), TransportError> {
        self.connected = false;
        Ok(())
    }

    async fn write(&mut self, payload: &[u8]) -> Result<(), TransportError> {
        if !self.connected {
            return Err(TransportError::NotConnected);
        }

        self.outgoing.push_back(payload.to_vec());
        Ok(())
    }

    async fn read(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
        if !self.connected {
            return Err(TransportError::NotConnected);
        }

        Ok(self.incoming.pop_front())
    }
}

impl ReconnectableTransport for ScriptedTransport {}

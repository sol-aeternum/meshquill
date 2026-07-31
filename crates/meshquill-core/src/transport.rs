use std::{
    collections::VecDeque,
    task::{Context, Poll, Waker},
};

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

/// Result of one non-blocking transport read attempt.
#[derive(Debug)]
pub enum ReadyRead {
    /// No complete logical packet is currently buffered.
    Pending,
    /// The peer has closed the connection cleanly.
    Closed,
    /// One complete logical companion packet was available.
    Packet(Vec<u8>),
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

    /// Polls once for a logical packet without waiting for future input.
    ///
    /// The default implementation relies on the cancellation-safety guarantee of [`Self::read`].
    /// Test transports whose preloaded frames model future command responses may override this and
    /// report [`ReadyRead::Pending`].
    ///
    /// # Errors
    /// Returns the same transport errors as [`Self::read`] when a packet is immediately ready.
    fn try_read(&mut self) -> Result<ReadyRead, TransportError> {
        let mut future = self.read();
        let mut context = Context::from_waker(Waker::noop());
        match future.as_mut().poll(&mut context) {
            Poll::Pending => Ok(ReadyRead::Pending),
            Poll::Ready(Ok(Some(packet))) => Ok(ReadyRead::Packet(packet)),
            Poll::Ready(Ok(None)) => Ok(ReadyRead::Closed),
            Poll::Ready(Err(error)) => Err(error),
        }
    }
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

    fn try_read(&mut self) -> Result<ReadyRead, TransportError> {
        if !self.connected {
            return Err(TransportError::NotConnected);
        }
        // Scripted inbound frames model responses that become visible after the matching write.
        Ok(ReadyRead::Pending)
    }
}

impl ReconnectableTransport for ScriptedTransport {}

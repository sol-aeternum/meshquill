use std::{io, time::Duration};

use bytes::{Buf, BytesMut};
use meshquill_core::{
    CoreError, TransportError,
    framing::{OuterDecoder, encode_payload},
    protocol::MAX_INNER_PAYLOAD,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    time,
};
use tokio_util::codec::Decoder;
use zeroize::Zeroizing;

const READ_CHUNK_SIZE: usize = 1_024;
const DEVICE_FRAME_PREFIX: u8 = 0x3e;

pub(crate) struct FramedReadState {
    decoder: OuterDecoder,
    buffered: BytesMut,
}

impl FramedReadState {
    pub(crate) fn new() -> Self {
        Self {
            decoder: OuterDecoder,
            buffered: BytesMut::with_capacity(READ_CHUNK_SIZE),
        }
    }

    pub(crate) fn reset(&mut self) {
        self.buffered.clear();
        self.decoder = OuterDecoder;
    }

    /// Reads exactly one logical packet. Tokio's `read` is cancellation-safe, and bytes are added
    /// to the persistent decoder only after a completed read operation.
    pub(crate) async fn read_from<R>(
        &mut self,
        reader: &mut R,
    ) -> Result<Option<Vec<u8>>, TransportError>
    where
        R: AsyncRead + Unpin,
    {
        loop {
            self.discard_preceding_noise();
            if let Some(frame) = self
                .decoder
                .decode(&mut self.buffered)
                .map_err(core_codec_error)?
            {
                return Ok(Some(frame.into_payload()));
            }

            let mut chunk = [0_u8; READ_CHUNK_SIZE];
            let read = reader.read(&mut chunk).await.map_err(TransportError::Io)?;
            if read == 0 {
                if self.buffered.is_empty() {
                    return Ok(None);
                }

                let pending = self.buffered.len();
                self.reset();
                return Err(TransportError::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("peer closed with {pending} byte(s) of an incomplete outer frame"),
                )));
            }
            self.buffered.extend_from_slice(&chunk[..read]);
        }
    }

    fn discard_preceding_noise(&mut self) {
        if self.buffered.first() == Some(&DEVICE_FRAME_PREFIX) {
            return;
        }
        if let Some(prefix) = self
            .buffered
            .iter()
            .position(|byte| *byte == DEVICE_FRAME_PREFIX)
        {
            self.buffered.advance(prefix);
        } else {
            self.buffered.clear();
        }
    }
}

pub(crate) async fn write_framed<W>(writer: &mut W, payload: &[u8]) -> Result<(), TransportError>
where
    W: AsyncWrite + Unpin,
{
    validate_payload(payload)?;
    let encoded = Zeroizing::new(encode_payload(payload).map_err(core_codec_error)?);
    writer
        .write_all(&encoded)
        .await
        .map_err(TransportError::Io)?;
    writer.flush().await.map_err(TransportError::Io)
}

/// Writes one frame within `timeout`, invalidating both stream directions after any framed-write
/// failure. Once `write_all` has started, an error or timeout can leave a partial outer frame at the
/// peer, so retaining the connection would make a later write unsafe.
pub(crate) async fn write_framed_bounded<W>(
    writer: &mut Option<W>,
    read_state: &mut FramedReadState,
    payload: &[u8],
    timeout: Duration,
) -> Result<(), TransportError>
where
    W: AsyncWrite + Unpin,
{
    let result = {
        let writer = writer.as_mut().ok_or(TransportError::NotConnected)?;
        match time::timeout(timeout, write_framed(writer, payload)).await {
            Ok(result) => result,
            Err(_elapsed) => Err(TransportError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("framed write timed out after {timeout:?}"),
            ))),
        }
    };

    if result.is_err() {
        writer.take();
        read_state.reset();
    }
    result
}

/// Drops a stream and clears buffered decoder state when `error` means the peer connection cannot
/// be reused. The classification intentionally matches `meshquill-core`'s disconnect handling.
pub(crate) fn invalidate_on_terminal_read_error<S>(
    stream: &mut Option<S>,
    read_state: &mut FramedReadState,
    error: &TransportError,
) {
    if is_terminal_read_error(error) {
        stream.take();
        read_state.reset();
    }
}

fn is_terminal_read_error(error: &TransportError) -> bool {
    match error {
        TransportError::NotConnected | TransportError::Closed => true,
        TransportError::Io(error) => matches!(
            error.kind(),
            io::ErrorKind::ConnectionAborted
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::BrokenPipe
                | io::ErrorKind::NotConnected
                | io::ErrorKind::UnexpectedEof
        ),
        _ => false,
    }
}

pub(crate) fn validate_payload(payload: &[u8]) -> Result<(), TransportError> {
    if payload.len() > MAX_INNER_PAYLOAD {
        return Err(TransportError::PayloadTooLarge {
            maximum: MAX_INNER_PAYLOAD,
            actual: payload.len(),
        });
    }
    Ok(())
}

fn core_codec_error(error: CoreError) -> TransportError {
    match error {
        CoreError::Transport(error) => error,
        other => TransportError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("outer frame codec failed: {other}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };
    use tokio::io::{AsyncWriteExt, duplex};

    struct PendingWriter;

    impl AsyncWrite for PendingWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            _buffer: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Pending
        }

        fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }

        fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    struct FailingWriter;

    impl AsyncWrite for FailingWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            _buffer: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "injected write failure",
            )))
        }

        fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn reads_partial_concatenated_frames_and_resynchronizes() {
        let (mut sender, mut receiver) = duplex(64);
        let send = tokio::spawn(async move {
            sender
                .write_all(&[0x99, 0x3e, 2])
                .await
                .expect("partial write");
            tokio::task::yield_now().await;
            sender
                .write_all(&[0, 1, 2, 0x3e, 1, 0, 3])
                .await
                .expect("remaining write");
        });

        let mut state = FramedReadState::new();
        assert_eq!(
            state.read_from(&mut receiver).await.expect("first frame"),
            Some(vec![1, 2])
        );
        assert_eq!(
            state.read_from(&mut receiver).await.expect("second frame"),
            Some(vec![3])
        );
        send.await.expect("sender task");
    }

    #[tokio::test]
    async fn reports_truncated_frame_at_eof() {
        let (mut sender, mut receiver) = duplex(16);
        sender
            .write_all(&[0x3e, 2, 0, 1])
            .await
            .expect("partial frame");
        drop(sender);

        let error = FramedReadState::new()
            .read_from(&mut receiver)
            .await
            .expect_err("truncated frame must fail");
        assert!(
            matches!(error, TransportError::Io(ref io_error) if io_error.kind() == io::ErrorKind::UnexpectedEof)
        );
    }

    #[tokio::test]
    async fn cancelled_read_preserves_partial_frame_state() {
        let (mut sender, mut receiver) = duplex(16);
        sender
            .write_all(&[DEVICE_FRAME_PREFIX, 2, 0, 7])
            .await
            .expect("partial frame");

        let mut state = FramedReadState::new();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), state.read_from(&mut receiver))
                .await
                .is_err(),
            "partial read should remain pending"
        );

        sender.write_all(&[8]).await.expect("finish frame");
        assert_eq!(
            state.read_from(&mut receiver).await.expect("resumed read"),
            Some(vec![7, 8])
        );
    }

    #[tokio::test]
    async fn write_helper_adds_app_outer_frame() {
        let (mut writer, mut reader) = duplex(16);
        write_framed(&mut writer, &[4, 5, 6])
            .await
            .expect("frame write");
        let mut actual = [0_u8; 6];
        reader.read_exact(&mut actual).await.expect("frame bytes");
        assert_eq!(actual, [0x3c, 3, 0, 4, 5, 6]);
    }

    #[tokio::test]
    async fn timed_out_framed_write_invalidates_stream_and_read_state() {
        let mut writer = Some(PendingWriter);
        let mut state = FramedReadState::new();
        state.buffered.extend_from_slice(&[DEVICE_FRAME_PREFIX, 1]);

        let error = write_framed_bounded(&mut writer, &mut state, &[1], Duration::ZERO)
            .await
            .expect_err("pending write must time out");

        assert!(matches!(
            error,
            TransportError::Io(ref error) if error.kind() == io::ErrorKind::TimedOut
        ));
        assert!(writer.is_none());
        assert!(state.buffered.is_empty());
    }

    #[tokio::test]
    async fn failed_framed_write_invalidates_stream_and_read_state() {
        let mut writer = Some(FailingWriter);
        let mut state = FramedReadState::new();
        state.buffered.extend_from_slice(&[DEVICE_FRAME_PREFIX, 1]);

        let error = write_framed_bounded(&mut writer, &mut state, &[1], Duration::from_secs(1))
            .await
            .expect_err("injected write must fail");

        assert!(matches!(
            error,
            TransportError::Io(ref error) if error.kind() == io::ErrorKind::BrokenPipe
        ));
        assert!(writer.is_none());
        assert!(state.buffered.is_empty());
    }

    #[test]
    fn terminal_read_error_classification_matches_core() {
        assert!(is_terminal_read_error(&TransportError::NotConnected));
        assert!(is_terminal_read_error(&TransportError::Closed));
        for kind in [
            io::ErrorKind::ConnectionAborted,
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::BrokenPipe,
            io::ErrorKind::NotConnected,
            io::ErrorKind::UnexpectedEof,
        ] {
            assert!(is_terminal_read_error(&TransportError::Io(io::Error::new(
                kind,
                "terminal test error"
            ))));
        }

        for kind in [
            io::ErrorKind::InvalidData,
            io::ErrorKind::TimedOut,
            io::ErrorKind::WouldBlock,
        ] {
            assert!(!is_terminal_read_error(&TransportError::Io(
                io::Error::new(kind, "recoverable test error")
            )));
        }
        assert!(!is_terminal_read_error(&TransportError::PayloadTooLarge {
            maximum: 1,
            actual: 2,
        }));
    }

    #[test]
    fn terminal_read_errors_invalidate_but_invalid_data_preserves_state() {
        let mut stream = Some(());
        let mut state = FramedReadState::new();
        state.buffered.extend_from_slice(&[DEVICE_FRAME_PREFIX, 1]);
        let invalid_data = TransportError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "recoverable decoder error",
        ));

        invalidate_on_terminal_read_error(&mut stream, &mut state, &invalid_data);
        assert!(stream.is_some());
        assert_eq!(&state.buffered[..], &[DEVICE_FRAME_PREFIX, 1]);

        let reset = TransportError::Io(io::Error::new(
            io::ErrorKind::ConnectionReset,
            "terminal provider error",
        ));
        invalidate_on_terminal_read_error(&mut stream, &mut state, &reset);
        assert!(stream.is_none());
        assert!(state.buffered.is_empty());
    }
}

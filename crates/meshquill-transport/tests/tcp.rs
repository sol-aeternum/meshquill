//! Deterministic loopback coverage for framed TCP transport behavior.

use std::{io, time::Duration};

use meshquill_core::{
    TransportError,
    protocol::MAX_INNER_PAYLOAD,
    transport::{ReconnectableTransport, Transport},
};
use meshquill_transport::TcpTransport;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
    time,
};

#[tokio::test]
async fn tcp_frames_writes_and_decodes_partial_concatenated_resynchronized_reads() {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind loopback listener");
    let address = listener.local_addr().expect("listener address");
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept TCP client");
        let mut request = [0_u8; 6];
        socket
            .read_exact(&mut request)
            .await
            .expect("read app frame");
        assert_eq!(request, [0x3c, 3, 0, 1, 2, 3]);

        socket
            .write_all(&[0x10, 0x11, 0x3e, 2])
            .await
            .expect("write garbage and partial frame");
        tokio::task::yield_now().await;
        socket
            .write_all(&[0, 7, 8, 0x3e, 1, 0, 9])
            .await
            .expect("write completed and concatenated frames");
        socket.shutdown().await.expect("shutdown server socket");
    });

    let mut transport = TcpTransport::new(
        address.ip().to_string(),
        address.port(),
        Duration::from_secs(2),
    )
    .expect("valid loopback target");
    transport.connect().await.expect("connect transport");
    transport
        .write(&[1, 2, 3])
        .await
        .expect("write logical packet");
    assert_eq!(
        transport.read().await.expect("read first packet"),
        Some(vec![7, 8])
    );
    assert_eq!(
        transport.read().await.expect("read second packet"),
        Some(vec![9])
    );
    assert_eq!(transport.read().await.expect("read clean EOF"), None);
    server.await.expect("server task");
}

#[tokio::test]
async fn tcp_rejects_oversized_logical_packet_without_writing() {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind loopback listener");
    let address = listener.local_addr().expect("listener address");
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept TCP client");
        let mut byte = [0_u8; 1];
        assert_eq!(socket.read(&mut byte).await.expect("observe close"), 0);
    });

    let mut transport = TcpTransport::new(
        address.ip().to_string(),
        address.port(),
        Duration::from_secs(2),
    )
    .expect("valid loopback target");
    transport.connect().await.expect("connect transport");
    let oversized = vec![0_u8; MAX_INNER_PAYLOAD + 1];
    let error = transport
        .write(&oversized)
        .await
        .expect_err("oversized packet must fail");
    assert!(matches!(
        error,
        TransportError::PayloadTooLarge {
            maximum: MAX_INNER_PAYLOAD,
            actual,
        } if actual == MAX_INNER_PAYLOAD + 1
    ));
    transport.disconnect().await.expect("disconnect transport");
    server.await.expect("server task");
}

#[tokio::test]
async fn tcp_decoder_recovers_after_oversized_header_on_a_later_read() {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind loopback listener");
    let address = listener.local_addr().expect("listener address");
    let (continue_sender, continue_receiver) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept TCP client");
        let oversized = u16::try_from(MAX_INNER_PAYLOAD + 1).expect("limit fits u16");
        socket
            .write_all(&[0x3e, oversized.to_le_bytes()[0], oversized.to_le_bytes()[1]])
            .await
            .expect("write oversized header");
        continue_receiver.await.expect("continue signal");
        socket
            .write_all(&[0x77, 0x3e, 2, 0, 4, 5])
            .await
            .expect("write resynchronization frame");
    });

    let mut transport = TcpTransport::new(
        address.ip().to_string(),
        address.port(),
        Duration::from_secs(2),
    )
    .expect("valid loopback target");
    transport.connect().await.expect("connect transport");
    let error = transport
        .read()
        .await
        .expect_err("oversized declared frame must fail");
    assert!(matches!(
        error,
        TransportError::Io(ref io_error) if io_error.kind() == io::ErrorKind::InvalidData
    ));
    assert!(
        transport.is_connected(),
        "recoverable framed InvalidData must retain the socket for resynchronization"
    );
    continue_sender.send(()).expect("send continue signal");
    assert_eq!(
        transport.read().await.expect("read recovered packet"),
        Some(vec![4, 5])
    );
    server.await.expect("server task");
}

#[tokio::test]
async fn tcp_terminal_read_error_invalidates_before_reconnect() {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind loopback listener");
    let address = listener.local_addr().expect("listener address");
    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.expect("accept first connection");
        first
            .write_all(&[0x3e, 2, 0, 0xaa])
            .await
            .expect("write incomplete frame");
        first.shutdown().await.expect("close incomplete stream");
        drop(first);

        let (mut second, _) = listener.accept().await.expect("accept reconnect");
        second
            .write_all(&[0x3e, 1, 0, 0xbb])
            .await
            .expect("write packet after reconnect");
    });

    let mut transport = TcpTransport::new(
        address.ip().to_string(),
        address.port(),
        Duration::from_secs(2),
    )
    .expect("valid loopback target");
    transport.connect().await.expect("connect transport");

    let error = transport
        .read()
        .await
        .expect_err("incomplete EOF must be terminal");
    assert!(matches!(
        error,
        TransportError::Io(ref io_error) if io_error.kind() == io::ErrorKind::UnexpectedEof
    ));
    assert!(
        !transport.is_connected(),
        "terminal read error must invalidate the dead socket"
    );

    transport
        .reconnect()
        .await
        .expect("reconnect must not shutdown the dead socket first");
    assert_eq!(
        transport.read().await.expect("read after reconnect"),
        Some(vec![0xbb])
    );
    server.await.expect("server task");
}

#[tokio::test]
async fn tcp_reconnects_same_target_without_replaying_a_write() {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind loopback listener");
    let address = listener.local_addr().expect("listener address");
    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.expect("accept first connection");
        let mut first_request = [0_u8; 5];
        first
            .read_exact(&mut first_request)
            .await
            .expect("read first request");
        assert_eq!(first_request, [0x3c, 2, 0, 1, 2]);
        first.shutdown().await.expect("close first connection");
        drop(first);

        let (mut second, _) = listener.accept().await.expect("accept reconnect");
        let mut unexpected = [0_u8; 1];
        assert!(
            time::timeout(Duration::from_millis(75), second.read(&mut unexpected))
                .await
                .is_err(),
            "reconnect must not replay the previous write"
        );
        second
            .write_all(&[0x3e, 1, 0, 0xaa])
            .await
            .expect("write packet after reconnect");

        let mut second_request = [0_u8; 4];
        second
            .read_exact(&mut second_request)
            .await
            .expect("read explicit second request");
        assert_eq!(second_request, [0x3c, 1, 0, 3]);
    });

    let mut transport = TcpTransport::new(
        address.ip().to_string(),
        address.port(),
        Duration::from_secs(2),
    )
    .expect("valid loopback target");
    transport.connect().await.expect("connect transport");
    transport.write(&[1, 2]).await.expect("write first packet");
    assert_eq!(transport.read().await.expect("observe first close"), None);

    transport.reconnect().await.expect("reconnect same target");
    assert_eq!(
        transport.read().await.expect("read after reconnect"),
        Some(vec![0xaa])
    );
    transport
        .write(&[3])
        .await
        .expect("write explicit packet after reconnect");
    server.await.expect("server task");
}

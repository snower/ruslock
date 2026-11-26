#![cfg(feature = "aio")]

//! Low-level raw command frames for the native binding.
//!
//! These tests pin the contract the binding needs for Java `sendCommand` /
//! `writeCommand`: a caller-built 64-byte header plus optional extra bytes is
//! validated without being re-encoded, unknown command types are preserved,
//! and write-only commands never pretend a response exists. Response encoding
//! must produce the exact bytes the Java `CommandResult` decoders consume.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use ruslock::aio::raw::{encode_response_frame, encode_response_header, RawCommand};
use ruslock::aio::Client;
use ruslock::data::LockResultData;
use ruslock::key::Key16;
use ruslock::protocol::command::{Command, EncodedCommand, PingCommand};
use ruslock::protocol::constants::*;
use ruslock::protocol::id::Id16;
use ruslock::protocol::result::{
    CommandResult, InitCommandResult, LockCommandResult, PingCommandResult,
};
use ruslock::SlockError;

fn init_response(request: &[u8; 64]) -> [u8; 64] {
    let mut response = [0u8; 64];
    response[0] = MAGIC;
    response[1] = VERSION;
    response[2] = COMMAND_TYPE_INIT;
    response[3..19].copy_from_slice(&request[3..19]);
    response[19] = COMMAND_RESULT_SUCCED;
    response
}

fn success_response(request: &[u8; 64]) -> [u8; 64] {
    let mut response = *request;
    response[0] = MAGIC;
    response[1] = VERSION;
    response[19] = COMMAND_RESULT_SUCCED;
    response
}

fn custom_header(command_type: u8, request_id: Id16) -> [u8; 64] {
    let mut header = [0u8; 64];
    header[0] = MAGIC;
    header[1] = VERSION;
    header[2] = command_type;
    header[3..19].copy_from_slice(request_id.as_bytes());
    // Deliberately non-zero reserved bytes: a raw frame must survive verbatim.
    header[19] = 0x5a;
    header[20] = 0xa5;
    header[60] = 0xde;
    header[61] = 0xad;
    header[62] = 0xbe;
    header[63] = 0xef;
    header
}

#[test]
fn raw_command_preserves_custom_header_and_extra_bytes() {
    let request_id = Id16::from_bytes([
        0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e,
        0x1f,
    ]);
    let header = custom_header(0x42, request_id);
    let extra = vec![0x00, 0xff, 0x00, 0x7f, 0x80];

    let command =
        RawCommand::from_parts(header, Some(extra.clone()), Duration::from_secs(3), true).unwrap();

    assert_eq!(command.request_id(), request_id);
    assert_eq!(command.command_type(), 0x42);
    assert_eq!(command.header(), &header);
    assert_eq!(command.extra(), Some(extra.as_slice()));
    assert_eq!(command.timeout(), Duration::from_secs(3));
    assert!(command.expects_response());
    assert!(!command.is_write_only());
    assert_eq!(
        command.timeout_with_grace(Duration::from_secs(120)),
        Duration::from_secs(123)
    );

    let mut expected = header.to_vec();
    expected.extend_from_slice(&extra);
    assert_eq!(command.frame(), expected);
}

#[test]
fn raw_command_from_frame_keeps_header_and_extra_byte_for_byte() {
    let request_id = Id16::from_bytes([7u8; 16]);
    let header = custom_header(0x99, request_id);
    let extra = vec![0x01, 0x00, 0x00, 0x00, 0xab];
    let mut frame = header.to_vec();
    frame.extend_from_slice(&extra);

    let command = RawCommand::from_frame(&frame, Duration::from_secs(9), false).unwrap();
    assert_eq!(command.header(), &header);
    assert_eq!(command.extra(), Some(extra.as_slice()));
    assert_eq!(command.frame(), frame);
    assert!(command.is_write_only());

    // A frame with exactly one header has no extra data, which is distinct from
    // an explicitly supplied empty payload.
    let bare = RawCommand::from_frame(&header, Duration::ZERO, true).unwrap();
    assert_eq!(bare.extra(), None);
    assert_eq!(bare.frame(), header.to_vec());

    let empty = RawCommand::from_parts(header, Some(Vec::new()), Duration::ZERO, true).unwrap();
    assert_eq!(empty.extra(), Some(&[][..]));
    assert_eq!(empty.frame(), header.to_vec());
}

#[test]
fn raw_command_accepts_existing_encoded_command_without_reencoding() {
    let encoded = Command::Ping(PingCommand::new(Id16::zero()))
        .encode()
        .unwrap();
    let expected_frame = encoded.frame();

    let raw = RawCommand::from_encoded(encoded.clone()).unwrap();
    assert_eq!(raw.command_type(), COMMAND_TYPE_PING);
    assert_eq!(raw.request_id(), encoded.request_id);
    assert_eq!(raw.header(), &encoded.header);
    assert_eq!(raw.frame(), expected_frame);
    assert_eq!(raw.timeout(), encoded.timeout);
    assert_eq!(raw.expects_response(), encoded.expects_response);

    let round_trip = raw.into_encoded();
    assert_eq!(round_trip, encoded);
}

#[test]
fn raw_command_rejects_malformed_frames_and_inconsistent_metadata() {
    let request_id = Id16::from_bytes([3u8; 16]);
    let header = custom_header(COMMAND_TYPE_LOCK, request_id);

    // Header shorter than the fixed 64-byte frame.
    assert!(RawCommand::from_frame(&header[..63], Duration::ZERO, true).is_err());

    let mut wrong_magic = header;
    wrong_magic[0] = 0x00;
    assert!(RawCommand::from_parts(wrong_magic, None, Duration::ZERO, true).is_err());

    let mut wrong_version = header;
    wrong_version[1] = 0x02;
    assert!(RawCommand::from_parts(wrong_version, None, Duration::ZERO, true).is_err());

    let encoded = EncodedCommand::new(
        request_id,
        COMMAND_TYPE_PING,
        header,
        None,
        Duration::ZERO,
        true,
    );
    assert!(
        RawCommand::from_encoded(encoded).is_err(),
        "header command type and encoded command type must agree"
    );
}

#[test]
fn encode_response_frame_matches_java_lock_layout_and_keeps_raw_extra() {
    let request_id = Id16::from_bytes([0x40; 16]);
    let lock_id = Id16::from_bytes([0x50; 16]);
    let lock_key = Key16::from_bytes([0x60; 16]);
    // Java's LockResultData reads the command-type and flags at offsets 4 and
    // 5, so the 4-byte payload length prefix is part of the raw extra bytes.
    let raw_extra = vec![0x03, 0x00, 0x00, 0x00, 0x2a, 0x01, 0xaa, 0xbb, 0xcc];
    let result = CommandResult::Lock(LockCommandResult {
        command_type: COMMAND_TYPE_UNLOCK,
        request_id,
        result: COMMAND_RESULT_SUCCED,
        flag: LOCK_FLAG_CONTAINS_DATA | LOCK_FLAG_SHOW_WHEN_LOCKED,
        db_id: 7,
        lock_id,
        lock_key,
        l_count: 0x1234,
        count: 0x5678,
        lr_count: 3,
        r_count: 4,
        data: Some(LockResultData::new(raw_extra.clone())),
    });

    let header = encode_response_header(&result);
    assert_eq!(header[0], MAGIC);
    assert_eq!(header[1], VERSION);
    assert_eq!(header[2], COMMAND_TYPE_UNLOCK);
    assert_eq!(&header[3..19], request_id.as_bytes());
    assert_eq!(header[19], COMMAND_RESULT_SUCCED);
    assert_eq!(
        header[20],
        LOCK_FLAG_CONTAINS_DATA | LOCK_FLAG_SHOW_WHEN_LOCKED
    );
    assert_eq!(header[21], 7);
    assert_eq!(&header[22..38], lock_id.as_bytes());
    assert_eq!(&header[38..54], lock_key.as_bytes());
    assert_eq!(&header[54..56], &0x1234u16.to_le_bytes());
    assert_eq!(&header[56..58], &0x5678u16.to_le_bytes());
    assert_eq!(header[58], 3);
    assert_eq!(header[59], 4);
    assert_eq!(&header[60..64], &[0u8; 4]);

    let frame = encode_response_frame(&result);
    let mut expected = header.to_vec();
    expected.extend_from_slice(&raw_extra);
    assert_eq!(frame, expected);
}

#[test]
fn encode_response_frame_covers_init_and_ping_results() {
    let request_id = Id16::from_bytes([0x70; 16]);

    let init = CommandResult::Init(InitCommandResult {
        request_id,
        result: COMMAND_RESULT_SUCCED,
        init_type: INIT_TYPE_FLAG_IS_LEADER,
    });
    let init_header = encode_response_header(&init);
    assert_eq!(init_header[2], COMMAND_TYPE_INIT);
    assert_eq!(&init_header[3..19], request_id.as_bytes());
    assert_eq!(init_header[19], COMMAND_RESULT_SUCCED);
    assert_eq!(init_header[20], INIT_TYPE_FLAG_IS_LEADER);
    assert_eq!(&init_header[21..64], &[0u8; 43]);
    assert_eq!(encode_response_frame(&init), init_header.to_vec());

    let ping = CommandResult::Ping(PingCommandResult {
        request_id,
        result: COMMAND_RESULT_UNKNOWN_COMMAND,
    });
    let ping_header = encode_response_header(&ping);
    assert_eq!(ping_header[2], COMMAND_TYPE_PING);
    assert_eq!(&ping_header[3..19], request_id.as_bytes());
    assert_eq!(ping_header[19], COMMAND_RESULT_UNKNOWN_COMMAND);
    assert_eq!(&ping_header[20..64], &[0u8; 44]);
    assert_eq!(encode_response_frame(&ping), ping_header.to_vec());
}

#[tokio::test]
async fn write_raw_returns_after_socket_write_without_response_and_no_pending() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (frame_tx, frame_rx) = oneshot::channel();

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).await.unwrap();
        stream.write_all(&init_response(&init)).await.unwrap();

        let mut header = [0u8; 64];
        stream.read_exact(&mut header).await.unwrap();
        let mut extra = [0u8; 5];
        stream.read_exact(&mut extra).await.unwrap();
        let mut frame = header.to_vec();
        frame.extend_from_slice(&extra);
        let _ = frame_tx.send(frame);

        // Deliberately never answer. Holding the socket open proves write_raw
        // waited for the write, not for a timeout or a fabricated response.
        let mut buf = [0u8; 1];
        let _ = stream.read(&mut buf).await;
    });

    let client = Client::connect(address).await.unwrap();
    let header = custom_header(0x43, Id16::from_bytes([0x21; 16]));
    let extra = vec![0xde, 0xad, 0xbe, 0xef, 0x00];
    let command =
        RawCommand::from_parts(header, Some(extra.clone()), Duration::from_secs(30), false)
            .unwrap();

    tokio::time::timeout(Duration::from_secs(2), client.write_raw(command))
        .await
        .expect("write_raw waited for a response instead of returning after the write")
        .unwrap();
    assert_eq!(client.pending_len().await, 0);

    let written = tokio::time::timeout(Duration::from_secs(2), frame_rx)
        .await
        .expect("server did not observe the raw write")
        .unwrap();
    let mut expected = header.to_vec();
    expected.extend_from_slice(&extra);
    assert_eq!(written, expected);

    client.close().await;
}

#[tokio::test]
async fn send_raw_custom_response_preserves_exact_server_bytes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).await.unwrap();
        stream.write_all(&init_response(&init)).await.unwrap();

        let mut request = [0u8; 64];
        stream.read_exact(&mut request).await.unwrap();
        assert_eq!(request[2], 0x42);
        let mut response = success_response(&request);
        // Unknown response type: the transport must not decode or normalize it.
        response[20] = 0x77;
        response[63] = 0xee;
        stream.write_all(&response).await.unwrap();
    });

    let client = Client::connect(address).await.unwrap();
    let header = custom_header(0x42, Id16::from_bytes([0x31; 16]));
    let command = RawCommand::from_parts(header, None, Duration::from_secs(5), true).unwrap();
    let response = client.send_raw(command).await.unwrap();
    let mut expected = success_response(&header);
    expected[20] = 0x77;
    expected[63] = 0xee;
    assert_eq!(response, expected.to_vec());
    assert_eq!(client.pending_len().await, 0);

    client.close().await;
}

#[tokio::test]
async fn send_raw_coexists_with_typed_ping() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).await.unwrap();
        stream.write_all(&init_response(&init)).await.unwrap();

        for _ in 0..2 {
            let mut request = [0u8; 64];
            stream.read_exact(&mut request).await.unwrap();
            match request[2] {
                0x42 | COMMAND_TYPE_PING => {
                    stream.write_all(&success_response(&request)).await.unwrap();
                }
                other => panic!("unexpected command type {other:#04x}"),
            }
        }
    });

    let client = Client::connect(address).await.unwrap();
    let header = custom_header(0x42, Id16::from_bytes([0x41; 16]));
    let command = RawCommand::from_parts(header, None, Duration::from_secs(5), true).unwrap();

    let (raw, ping) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(client.send_raw(command), client.ping())
    })
    .await
    .expect("raw and typed commands did not complete together");
    assert_eq!(raw.unwrap(), success_response(&header).to_vec());
    assert!(ping.unwrap());
    assert_eq!(client.pending_len().await, 0);

    client.close().await;
}

#[tokio::test]
async fn duplicate_raw_request_id_cannot_cross_resolve() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (seen_tx, seen_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).await.unwrap();
        stream.write_all(&init_response(&init)).await.unwrap();

        let mut request = [0u8; 64];
        stream.read_exact(&mut request).await.unwrap();
        let response = success_response(&request);
        let _ = seen_tx.send(response);
        let _ = release_rx.await;
        stream.write_all(&response).await.unwrap();
    });

    let client = Client::connect(address).await.unwrap();
    let request_id = Id16::from_bytes([0x51; 16]);
    let first = RawCommand::from_parts(
        custom_header(0x44, request_id),
        None,
        Duration::from_secs(5),
        true,
    )
    .unwrap();
    let second = first.clone();

    let first_client = client.clone();
    let first_task = tokio::spawn(async move { first_client.send_raw(first).await });

    let expected = tokio::time::timeout(Duration::from_secs(2), seen_rx)
        .await
        .expect("server did not receive the first raw command")
        .unwrap();

    let collision = client.send_raw(second).await;
    match collision {
        Err(SlockError::Protocol(message)) => {
            assert!(
                message.contains("duplicate"),
                "unexpected collision error: {message}"
            );
        }
        other => panic!("duplicate request id was not rejected: {other:?}"),
    }

    release_tx.send(()).unwrap();
    let first_response = tokio::time::timeout(Duration::from_secs(2), first_task)
        .await
        .expect("first raw command did not complete")
        .unwrap()
        .unwrap();
    assert_eq!(first_response, expected);
    assert_eq!(client.pending_len().await, 0);

    client.close().await;
}

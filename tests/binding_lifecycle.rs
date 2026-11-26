#![cfg(feature = "aio")]

//! Lifecycle contracts for the native binding: strict `open` vs. managed
//! `try_open`, and cancellation of the initial reconnect by `close`.
//!
//! The listener below is deterministic and controllable: while `accepting` is
//! false it completes the Init handshake with a refusal result and then drops
//! the socket, which lets the test observe retries without relying on OS-level
//! connection refusal. When `accepting` becomes true it answers Init and Ping
//! successfully.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use ruslock::aio::Client;
use ruslock::protocol::constants::*;
use ruslock::{ClientOptions, SlockError};

#[derive(Clone)]
struct ControlledServer {
    address: String,
    accepting: Arc<AtomicBool>,
    connection_count: Arc<AtomicUsize>,
    init_ids: Arc<Mutex<Vec<[u8; 16]>>>,
}

async fn start_controlled_server() -> ControlledServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let accepting = Arc::new(AtomicBool::new(false));
    let connection_count = Arc::new(AtomicUsize::new(0));
    let init_ids = Arc::new(Mutex::new(Vec::new()));

    let server_accepting = Arc::clone(&accepting);
    let server_count = Arc::clone(&connection_count);
    let server_ids = Arc::clone(&init_ids);
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            server_count.fetch_add(1, Ordering::SeqCst);
            let accepting = Arc::clone(&server_accepting);
            let ids = Arc::clone(&server_ids);
            tokio::spawn(async move {
                let mut init = [0u8; 64];
                if stream.read_exact(&mut init).await.is_err() {
                    return;
                }
                if init[2] != COMMAND_TYPE_INIT {
                    return;
                }

                let mut client_id = [0u8; 16];
                client_id.copy_from_slice(&init[19..35]);
                ids.lock().unwrap().push(client_id);

                if !accepting.load(Ordering::SeqCst) {
                    let refusal = init_response(&init, COMMAND_RESULT_STATE_ERROR);
                    let _ = stream.write_all(&refusal).await;
                    return;
                }

                let success = init_response(&init, COMMAND_RESULT_SUCCED);
                if stream.write_all(&success).await.is_err() {
                    return;
                }

                loop {
                    let mut command = [0u8; 64];
                    if stream.read_exact(&mut command).await.is_err() {
                        return;
                    }
                    let mut response = [0u8; 64];
                    response[0] = MAGIC;
                    response[1] = VERSION;
                    response[2] = command[2];
                    response[3..19].copy_from_slice(&command[3..19]);
                    response[19] = COMMAND_RESULT_SUCCED;
                    if stream.write_all(&response).await.is_err() {
                        return;
                    }
                }
            });
        }
    });

    ControlledServer {
        address,
        accepting,
        connection_count,
        init_ids,
    }
}

fn init_response(request: &[u8; 64], result: u8) -> [u8; 64] {
    let mut response = [0u8; 64];
    response[0] = MAGIC;
    response[1] = VERSION;
    response[2] = COMMAND_TYPE_INIT;
    response[3..19].copy_from_slice(&request[3..19]);
    response[19] = result;
    response
}

async fn wait_for<F: FnMut() -> bool>(mut condition: F, timeout: Duration, what: &str) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if condition() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn fast_reconnect_options(reconnect_interval: Duration) -> ClientOptions {
    ClientOptions {
        connect_timeout: Duration::from_secs(1),
        reconnect_interval,
        command_timeout_grace: Duration::from_millis(200),
        ..ClientOptions::default()
    }
}

#[tokio::test]
async fn try_open_recovers_after_initial_refusal() {
    let server = start_controlled_server().await;
    let client = Client::with_options(
        server.address.clone(),
        fast_reconnect_options(Duration::from_millis(20)),
    );

    let connected_now = client.try_open().await.unwrap();
    assert!(
        !connected_now,
        "try_open must report false when the first handshake is refused"
    );

    wait_for(
        || !server.init_ids.lock().unwrap().is_empty(),
        Duration::from_secs(2),
        "the first refused Init",
    )
    .await;

    // Start serving; the managed reconnect must recover without another call.
    server.accepting.store(true, Ordering::SeqCst);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        match client.ping().await {
            Ok(true) => break,
            Ok(false) | Err(SlockError::NotConnected) | Err(SlockError::Io(_)) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "managed reconnect did not recover"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(err) => panic!("unexpected ping error after recovery: {err:?}"),
        }
    }

    let ids = server.init_ids.lock().unwrap().clone();
    assert!(
        ids.len() >= 2,
        "expected at least the refused and recovered Init handshakes, got {ids:?}"
    );
    assert!(
        ids.iter().all(|id| id == &ids[0]),
        "reconnect must reuse the client identity: {ids:?}"
    );

    client.close().await;
}

#[tokio::test]
async fn close_stops_initial_reconnect() {
    let server = start_controlled_server().await;
    let client = Client::with_options(
        server.address.clone(),
        fast_reconnect_options(Duration::from_millis(20)),
    );

    let connected_now = client.try_open().await.unwrap();
    assert!(!connected_now);

    wait_for(
        || server.connection_count.load(Ordering::SeqCst) >= 3,
        Duration::from_secs(3),
        "repeated reconnect attempts",
    )
    .await;

    client.close().await;
    // Let a connection attempt that was already in flight at close() settle.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let settled = server.connection_count.load(Ordering::SeqCst);

    // More than ten reconnect intervals: no new socket may be created.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        settled,
        server.connection_count.load(Ordering::SeqCst),
        "close must stop the managed initial reconnect"
    );

    assert_eq!(client.pending_len().await, 0);
    let err = client.ping().await.unwrap_err();
    assert!(
        matches!(err, SlockError::ClientClosed),
        "closed client must not publish a socket: {err:?}"
    );
}

#[tokio::test]
async fn try_open_then_strict_open_reuses_identity() {
    let server = start_controlled_server().await;
    let client = Client::with_options(
        server.address.clone(),
        // Long interval: keep the managed retry parked while we call open().
        fast_reconnect_options(Duration::from_secs(5)),
    );

    assert!(!client.try_open().await.unwrap());
    wait_for(
        || !server.init_ids.lock().unwrap().is_empty(),
        Duration::from_secs(2),
        "the first refused Init",
    )
    .await;

    server.accepting.store(true, Ordering::SeqCst);
    client.open().await.unwrap();

    wait_for(
        || server.init_ids.lock().unwrap().len() >= 2,
        Duration::from_secs(2),
        "the strict open Init",
    )
    .await;

    let ids = server.init_ids.lock().unwrap().clone();
    assert_eq!(
        ids.len(),
        2,
        "strict open must replace the parked retry, not add a parallel one: {ids:?}"
    );
    assert_eq!(ids[0], ids[1], "strict open must reuse the client identity");

    assert!(client.ping().await.unwrap());

    client.close().await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let settled = server.connection_count.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        settled,
        server.connection_count.load(Ordering::SeqCst),
        "close after a strict open must not leave a reconnect task"
    );
}

#[tokio::test]
async fn strict_open_still_fails_on_initial_refusal() {
    let server = start_controlled_server().await;
    let client = Client::with_options(
        server.address.clone(),
        fast_reconnect_options(Duration::from_millis(20)),
    );

    let err = client.open().await.unwrap_err();
    assert!(
        matches!(err, SlockError::Server { .. }),
        "strict open must surface the handshake refusal: {err:?}"
    );

    let count = server.connection_count.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        count,
        server.connection_count.load(Ordering::SeqCst),
        "strict open must not start a background reconnect"
    );

    client.close().await;
}

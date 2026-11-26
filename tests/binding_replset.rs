#![cfg(feature = "aio")]

//! Replica-set lifecycle and routing contracts for the native binding.
//!
//! The Java binding opens a replica set even when every member is currently
//! unreachable: child clients start managed reconnects and commands must later
//! discover the leader without a second explicit `open`. These tests pin that
//! behavior with deterministic, controllable nodes:
//!
//! * while `accepting` is false a node answers Init with a refusal and drops
//!   the socket, so retries are observable without relying on OS-level
//!   connection refusal;
//! * while `accepting` is true it completes Init with a fixed init type and
//!   answers Ping and Lock commands;
//! * a follower reports `COMMAND_RESULT_STATE_ERROR` for Lock so the existing
//!   redirect path must move the command to the leader.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use ruslock::aio::raw::RawCommand;
use ruslock::aio::{Client, ClientHandle, ReplsetClient};
use ruslock::protocol::constants::*;
use ruslock::{ClientOptions, SlockError};

#[derive(Clone)]
struct Node {
    address: String,
    accepting: Arc<AtomicBool>,
    connection_count: Arc<AtomicUsize>,
    init_success_count: Arc<AtomicUsize>,
    command_count: Arc<AtomicUsize>,
    ping_count: Arc<AtomicUsize>,
    lock_count: Arc<AtomicUsize>,
    state_error_count: Arc<AtomicUsize>,
    drop_after_command: Arc<AtomicBool>,
    stall_next_command: Arc<AtomicBool>,
}

async fn start_node(init_type: u8) -> Node {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let accepting = Arc::new(AtomicBool::new(false));
    let connection_count = Arc::new(AtomicUsize::new(0));
    let init_success_count = Arc::new(AtomicUsize::new(0));
    let command_count = Arc::new(AtomicUsize::new(0));
    let ping_count = Arc::new(AtomicUsize::new(0));
    let lock_count = Arc::new(AtomicUsize::new(0));
    let state_error_count = Arc::new(AtomicUsize::new(0));
    let drop_after_command = Arc::new(AtomicBool::new(false));
    let stall_next_command = Arc::new(AtomicBool::new(false));

    let server_accepting = Arc::clone(&accepting);
    let server_connections = Arc::clone(&connection_count);
    let server_init_success = Arc::clone(&init_success_count);
    let server_command_count = Arc::clone(&command_count);
    let server_ping = Arc::clone(&ping_count);
    let server_lock = Arc::clone(&lock_count);
    let server_state_error = Arc::clone(&state_error_count);
    let server_drop_after_command = Arc::clone(&drop_after_command);
    let server_stall_next_command = Arc::clone(&stall_next_command);
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            server_connections.fetch_add(1, Ordering::SeqCst);
            let accepting = Arc::clone(&server_accepting);
            let init_success = Arc::clone(&server_init_success);
            let command_count = Arc::clone(&server_command_count);
            let ping_count = Arc::clone(&server_ping);
            let lock_count = Arc::clone(&server_lock);
            let state_error_count = Arc::clone(&server_state_error);
            let drop_after_command = Arc::clone(&server_drop_after_command);
            let stall_next_command = Arc::clone(&server_stall_next_command);
            tokio::spawn(async move {
                let mut init = [0u8; 64];
                if stream.read_exact(&mut init).await.is_err() {
                    return;
                }
                if init[2] != COMMAND_TYPE_INIT {
                    return;
                }

                if !accepting.load(Ordering::SeqCst) {
                    let refusal = init_response(&init, COMMAND_RESULT_STATE_ERROR, 0);
                    let _ = stream.write_all(&refusal).await;
                    return;
                }

                let success = init_response(&init, COMMAND_RESULT_SUCCED, init_type);
                if stream.write_all(&success).await.is_err() {
                    return;
                }
                init_success.fetch_add(1, Ordering::SeqCst);

                loop {
                    let mut command = [0u8; 64];
                    if stream.read_exact(&mut command).await.is_err() {
                        return;
                    }
                    command_count.fetch_add(1, Ordering::SeqCst);
                    if drop_after_command.load(Ordering::SeqCst) {
                        // Simulate a node that received the frame but the
                        // client never learns whether it was applied.
                        return;
                    }
                    if stall_next_command.swap(false, Ordering::SeqCst) {
                        // Received the frame, but leave it unanswered while
                        // keeping the stream open so the client's own exact
                        // response deadline must release the pending entry.
                        continue;
                    }
                    let result = match command[2] {
                        COMMAND_TYPE_PING => {
                            ping_count.fetch_add(1, Ordering::SeqCst);
                            COMMAND_RESULT_SUCCED
                        }
                        COMMAND_TYPE_LOCK => {
                            lock_count.fetch_add(1, Ordering::SeqCst);
                            if (init_type & INIT_TYPE_FLAG_IS_LEADER) != 0 {
                                COMMAND_RESULT_SUCCED
                            } else {
                                state_error_count.fetch_add(1, Ordering::SeqCst);
                                COMMAND_RESULT_STATE_ERROR
                            }
                        }
                        _ => COMMAND_RESULT_SUCCED,
                    };
                    if stream
                        .write_all(&command_response(&command, result))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            });
        }
    });

    Node {
        address,
        accepting,
        connection_count,
        init_success_count,
        command_count,
        ping_count,
        lock_count,
        state_error_count,
        drop_after_command,
        stall_next_command,
    }
}

fn init_response(request: &[u8; 64], result: u8, init_type: u8) -> [u8; 64] {
    let mut response = [0u8; 64];
    response[0] = MAGIC;
    response[1] = VERSION;
    response[2] = COMMAND_TYPE_INIT;
    response[3..19].copy_from_slice(&request[3..19]);
    response[19] = result;
    response[20] = init_type;
    response
}

fn command_response(request: &[u8; 64], result: u8) -> [u8; 64] {
    let mut response = [0u8; 64];
    response[0] = MAGIC;
    response[1] = VERSION;
    response[2] = request[2];
    response[3..19].copy_from_slice(&request[3..19]);
    response[19] = result;
    if request[2] == COMMAND_TYPE_LOCK {
        response[21] = request[20];
        response[22..38].copy_from_slice(&request[21..37]);
        response[38..54].copy_from_slice(&request[37..53]);
    }
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

fn fast_options() -> ClientOptions {
    ClientOptions {
        connect_timeout: Duration::from_millis(100),
        reconnect_interval: Duration::from_millis(20),
        command_timeout_grace: Duration::from_secs(2),
        ..ClientOptions::default()
    }
}

fn exact_options() -> ClientOptions {
    ClientOptions {
        connect_timeout: Duration::from_millis(100),
        reconnect_interval: Duration::from_millis(20),
        // Deliberately much longer than the exact raw deadline below.
        command_timeout_grace: Duration::from_secs(30),
        ..ClientOptions::default()
    }
}

fn unused_address() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    drop(listener);
    address
}

fn raw_ping(expects_response: bool) -> RawCommand {
    let mut header = [0u8; 64];
    header[0] = MAGIC;
    header[1] = VERSION;
    header[2] = COMMAND_TYPE_PING;
    header[3..19].copy_from_slice(&[0x11; 16]);
    RawCommand::from_parts(header, None, Duration::from_secs(1), expects_response).unwrap()
}

fn raw_ping_exact(timeout: Duration) -> RawCommand {
    let mut header = [0u8; 64];
    header[0] = MAGIC;
    header[1] = VERSION;
    header[2] = COMMAND_TYPE_PING;
    header[3..19].copy_from_slice(&[0x22; 16]);
    RawCommand::from_parts(header, None, timeout, true)
        .unwrap()
        .with_exact_timeout()
}

#[tokio::test]
async fn try_open_reports_discovery_then_prefers_leader() {
    let follower = start_node(INIT_TYPE_FLAG_HAS_LEADER).await;
    let leader = start_node(INIT_TYPE_FLAG_IS_LEADER).await;
    let client = ReplsetClient::with_options(
        [follower.address.clone(), leader.address.clone()],
        fast_options(),
    );

    assert!(
        !client.try_open().await.unwrap(),
        "all nodes refused, so no member is usable yet"
    );
    assert_eq!(client.active_index(), None);
    assert_eq!(client.active_init_type(), 0);
    assert!(!client.is_leader());

    wait_for(
        || {
            follower.connection_count.load(Ordering::SeqCst) >= 2
                && leader.connection_count.load(Ordering::SeqCst) >= 2
        },
        Duration::from_secs(3),
        "managed reconnect attempts on both nodes",
    )
    .await;

    follower.accepting.store(true, Ordering::SeqCst);
    leader.accepting.store(true, Ordering::SeqCst);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if client.try_open().await.unwrap() && client.is_leader() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "late discovery never selected the leader"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(client.active_index(), Some(1));
    assert_eq!(client.active_init_type(), INIT_TYPE_FLAG_IS_LEADER);
    assert!(client.is_leader());

    assert!(client.ping().await.unwrap());
    wait_for(
        || leader.ping_count.load(Ordering::SeqCst) >= 1,
        Duration::from_secs(2),
        "Ping on the discovered leader",
    )
    .await;
    assert_eq!(follower.ping_count.load(Ordering::SeqCst), 0);

    client.close().await;
    assert_eq!(client.active_index(), None);
}

#[tokio::test]
async fn late_discovery_routes_commands_through_existing_redirect() {
    let follower = start_node(INIT_TYPE_FLAG_HAS_LEADER).await;
    let leader = start_node(INIT_TYPE_FLAG_IS_LEADER).await;
    let client = ReplsetClient::with_options(
        [follower.address.clone(), leader.address.clone()],
        fast_options(),
    );

    assert!(!client.try_open().await.unwrap());

    wait_for(
        || {
            follower.connection_count.load(Ordering::SeqCst) >= 2
                && leader.connection_count.load(Ordering::SeqCst) >= 2
        },
        Duration::from_secs(3),
        "managed reconnect attempts on both nodes",
    )
    .await;

    follower.accepting.store(true, Ordering::SeqCst);
    leader.accepting.store(true, Ordering::SeqCst);

    // The managed reconnects land without any routed command or a second
    // try_open, but no member has been selected as the active route yet.
    wait_for(
        || {
            follower.init_success_count.load(Ordering::SeqCst) >= 1
                && leader.init_success_count.load(Ordering::SeqCst) >= 1
        },
        Duration::from_secs(3),
        "both managed reconnects to complete their Init",
    )
    .await;
    assert_eq!(client.active_index(), None);

    // A command issued now must discover the leader: node 0 is a follower and
    // reports STATE_ERROR, so the existing redirect path moves to node 1.
    let mut lock = client.lock("binding-replset-late-route", 0, 10);
    lock.acquire().await.unwrap();

    assert_eq!(client.active_index(), Some(1));
    assert!(client.is_leader());
    assert!(
        follower.state_error_count.load(Ordering::SeqCst) >= 1,
        "the follower must have redirected the Lock command"
    );
    assert!(leader.lock_count.load(Ordering::SeqCst) >= 1);

    assert!(client.ping().await.unwrap());
    wait_for(
        || leader.ping_count.load(Ordering::SeqCst) >= 1,
        Duration::from_secs(2),
        "Ping on the redirected leader",
    )
    .await;
    assert_eq!(follower.ping_count.load(Ordering::SeqCst), 0);

    client.close().await;
}

#[tokio::test]
async fn close_stops_managed_discovery_and_rejects_commands() {
    let first = start_node(INIT_TYPE_FLAG_HAS_LEADER).await;
    let second = start_node(INIT_TYPE_FLAG_IS_LEADER).await;
    let client = ReplsetClient::with_options(
        [first.address.clone(), second.address.clone()],
        fast_options(),
    );

    assert!(!client.try_open().await.unwrap());
    wait_for(
        || {
            first.connection_count.load(Ordering::SeqCst) >= 3
                && second.connection_count.load(Ordering::SeqCst) >= 3
        },
        Duration::from_secs(3),
        "repeated managed reconnect attempts",
    )
    .await;

    client.close().await;
    assert_eq!(client.active_index(), None);
    assert_eq!(client.active_init_type(), 0);

    // Let an attempt already in flight at close() settle.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let settled_first = first.connection_count.load(Ordering::SeqCst);
    let settled_second = second.connection_count.load(Ordering::SeqCst);

    // More than ten reconnect intervals: no new socket may be created.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        settled_first,
        first.connection_count.load(Ordering::SeqCst),
        "close must stop the first managed reconnect"
    );
    assert_eq!(
        settled_second,
        second.connection_count.load(Ordering::SeqCst),
        "close must stop the second managed reconnect"
    );

    let err = client.ping().await.unwrap_err();
    assert!(
        matches!(err, SlockError::ClientClosed),
        "closed replset must not publish a socket: {err:?}"
    );
}

#[tokio::test]
async fn handle_try_open_delegates_to_replset_leader_selection() {
    let follower = start_node(INIT_TYPE_FLAG_HAS_LEADER).await;
    let leader = start_node(INIT_TYPE_FLAG_IS_LEADER).await;
    follower.accepting.store(true, Ordering::SeqCst);
    leader.accepting.store(true, Ordering::SeqCst);

    let handle = ClientHandle::with_options(
        [follower.address.clone(), leader.address.clone()],
        fast_options(),
    );

    // Both nodes are already accepting, but a retry keeps this assertion
    // independent of which member the OS completes first.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if handle.try_open().await.unwrap() && handle.is_leader() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the handle never selected the live leader"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(handle.active_index(), Some(1));
    assert_eq!(handle.active_init_type(), INIT_TYPE_FLAG_IS_LEADER);
    assert!(handle.is_leader());
    assert!(handle.ping().await.unwrap());

    wait_for(
        || leader.ping_count.load(Ordering::SeqCst) >= 1,
        Duration::from_secs(2),
        "Ping through the handle",
    )
    .await;
    assert_eq!(follower.ping_count.load(Ordering::SeqCst), 0);

    handle.close().await;
}

#[tokio::test]
async fn try_open_surfaces_error_when_managed_reconnect_is_disabled() {
    let node = start_node(INIT_TYPE_FLAG_IS_LEADER).await;
    let options = ClientOptions {
        auto_reconnect: false,
        ..fast_options()
    };
    let client = ReplsetClient::with_options([node.address.clone()], options);

    let err = client.try_open().await.unwrap_err();
    assert!(
        !matches!(err, SlockError::ClientClosed),
        "an initial refusal must surface as a connection/handshake error: {err:?}"
    );
    assert_eq!(client.active_index(), None);

    let attempts = node.connection_count.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        attempts,
        node.connection_count.load(Ordering::SeqCst),
        "without auto_reconnect try_open must not start a background retry"
    );

    client.close().await;
}

#[tokio::test]
async fn typed_lock_is_not_replayed_after_an_unknown_write_outcome() {
    let first = start_node(INIT_TYPE_FLAG_IS_LEADER).await;
    let second = start_node(INIT_TYPE_FLAG_IS_LEADER).await;
    first.accepting.store(true, Ordering::SeqCst);
    second.accepting.store(true, Ordering::SeqCst);
    // The first member receives the Lock frame and drops the connection before
    // answering. The side effect may already be applied, so no retry is safe.
    first.drop_after_command.store(true, Ordering::SeqCst);

    let client = ReplsetClient::with_options(
        [first.address.clone(), second.address.clone()],
        fast_options(),
    )
    .with_conservative_replay();

    let mut lock = client.lock("binding-replset-typed-unknown", 0, 10);
    let _err = lock.acquire().await.unwrap_err();

    wait_for(
        || first.command_count.load(Ordering::SeqCst) >= 1,
        Duration::from_secs(2),
        "the first member to receive the typed Lock",
    )
    .await;
    assert_eq!(
        second.lock_count.load(Ordering::SeqCst),
        0,
        "an unknown typed Lock outcome must not be replayed on another member"
    );
    assert_eq!(second.command_count.load(Ordering::SeqCst), 0);
    assert_eq!(second.connection_count.load(Ordering::SeqCst), 0);

    client.close().await;
}

#[tokio::test]
async fn typed_lock_fails_over_when_the_first_write_was_not_sent() {
    let unavailable = unused_address();
    let second = start_node(INIT_TYPE_FLAG_IS_LEADER).await;
    second.accepting.store(true, Ordering::SeqCst);

    let client = ReplsetClient::with_options([unavailable, second.address.clone()], fast_options());

    let mut lock = client.lock("binding-replset-typed-not-sent", 0, 10);
    lock.acquire().await.unwrap();

    assert_eq!(client.active_index(), Some(1));
    assert_eq!(second.lock_count.load(Ordering::SeqCst), 1);
    client.close().await;
}

#[tokio::test]
async fn exact_raw_timeout_ignores_client_grace_and_cleans_pending() {
    let node = start_node(INIT_TYPE_FLAG_IS_LEADER).await;
    node.accepting.store(true, Ordering::SeqCst);
    node.stall_next_command.store(true, Ordering::SeqCst);

    let client = Client::with_options(node.address.clone(), exact_options());
    client.open().await.unwrap();

    // The exact deadline is 60ms while the client grace is 30s. If the core
    // added the grace, this outer 2s guard would expire instead of returning.
    let started = std::time::Instant::now();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        client.send_raw(raw_ping_exact(Duration::from_millis(60))),
    )
    .await
    .expect("exact raw timeout must not wait for the client command grace");

    let err = result.unwrap_err();
    assert!(
        matches!(err, SlockError::CommandTimeout),
        "exact raw timeout must surface CommandTimeout, got {err:?}"
    );
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(
        client.pending_len().await,
        0,
        "a timed-out raw request must deregister its pending entry"
    );

    // The timed-out frame was fully written, so the stream must still be usable.
    let frame = client.send_raw(raw_ping(true)).await.unwrap();
    assert_eq!(frame[2], COMMAND_TYPE_PING);

    client.close().await;
}

#[tokio::test]
async fn exact_raw_timeout_is_unknown_and_not_replayed() {
    let first = start_node(INIT_TYPE_FLAG_IS_LEADER).await;
    let second = start_node(INIT_TYPE_FLAG_IS_LEADER).await;
    first.accepting.store(true, Ordering::SeqCst);
    second.accepting.store(true, Ordering::SeqCst);
    first.stall_next_command.store(true, Ordering::SeqCst);

    let client = ReplsetClient::with_options(
        [first.address.clone(), second.address.clone()],
        exact_options(),
    );

    let result = tokio::time::timeout(
        Duration::from_secs(2),
        client.send_raw(raw_ping_exact(Duration::from_millis(60))),
    )
    .await
    .expect("exact raw timeout must release before the replset retry deadline");

    let err = result.unwrap_err();
    assert!(
        matches!(err, SlockError::CommandTimeout),
        "an exact raw timeout is an unknown outcome, got {err:?}"
    );

    wait_for(
        || first.command_count.load(Ordering::SeqCst) >= 1,
        Duration::from_secs(2),
        "the first member to receive the exact raw command",
    )
    .await;
    assert_eq!(
        second.command_count.load(Ordering::SeqCst),
        0,
        "an unknown raw outcome must not be replayed on another member"
    );
    assert_eq!(second.connection_count.load(Ordering::SeqCst), 0);

    client.close().await;
}

#[tokio::test]
async fn raw_send_never_replays_an_unknown_write_outcome() {
    let first = start_node(INIT_TYPE_FLAG_IS_LEADER).await;
    let second = start_node(INIT_TYPE_FLAG_IS_LEADER).await;
    first.accepting.store(true, Ordering::SeqCst);
    second.accepting.store(true, Ordering::SeqCst);
    // The first member receives the frame and drops the connection before
    // answering, so the client cannot know whether the command was applied.
    first.drop_after_command.store(true, Ordering::SeqCst);

    let client = ReplsetClient::with_options(
        [first.address.clone(), second.address.clone()],
        fast_options(),
    );

    let _err = client.send_raw(raw_ping(true)).await.unwrap_err();
    wait_for(
        || first.command_count.load(Ordering::SeqCst) >= 1,
        Duration::from_secs(2),
        "the first member to receive the raw command",
    )
    .await;

    assert_eq!(
        second.command_count.load(Ordering::SeqCst),
        0,
        "an unknown write outcome must not be replayed on another member"
    );
    assert_eq!(second.connection_count.load(Ordering::SeqCst), 0);

    client.close().await;
}

#[tokio::test]
async fn raw_send_fails_over_when_the_first_write_was_not_sent() {
    let unavailable = unused_address();
    let second = start_node(INIT_TYPE_FLAG_IS_LEADER).await;
    second.accepting.store(true, Ordering::SeqCst);

    let handle = ClientHandle::with_options([unavailable, second.address.clone()], fast_options());

    let frame = handle.send_raw(raw_ping(true)).await.unwrap();
    assert_eq!(frame.len(), 64);
    assert_eq!(frame[2], COMMAND_TYPE_PING);
    wait_for(
        || second.command_count.load(Ordering::SeqCst) >= 1,
        Duration::from_secs(2),
        "the second member to receive the raw command",
    )
    .await;

    assert_eq!(handle.active_index(), Some(1));
    handle.close().await;
}

#[tokio::test]
async fn raw_write_fails_over_when_the_first_write_was_not_sent() {
    let unavailable = unused_address();
    let second = start_node(INIT_TYPE_FLAG_IS_LEADER).await;
    second.accepting.store(true, Ordering::SeqCst);

    let client = ReplsetClient::with_options([unavailable, second.address.clone()], fast_options());

    client.write_raw(raw_ping(false)).await.unwrap();
    wait_for(
        || second.command_count.load(Ordering::SeqCst) >= 1,
        Duration::from_secs(2),
        "the second member to receive the write-only raw command",
    )
    .await;

    assert_eq!(client.active_index(), Some(1));
    client.close().await;
}

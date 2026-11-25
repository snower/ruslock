#![cfg(feature = "aio")]

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

use ruslock::aio::Client;
use ruslock::protocol::constants::*;
use ruslock::{ClientOptions, LockData, SlockError};

fn init_response(request: &[u8; 64]) -> [u8; 64] {
    let mut response = [0u8; 64];
    response[0] = MAGIC;
    response[1] = VERSION;
    response[2] = COMMAND_TYPE_INIT;
    response[3..19].copy_from_slice(&request[3..19]);
    response[19] = COMMAND_RESULT_SUCCED;
    response
}

fn ping_response(request: &[u8; 64]) -> [u8; 64] {
    let mut response = [0u8; 64];
    response[0] = MAGIC;
    response[1] = VERSION;
    response[2] = COMMAND_TYPE_PING;
    response[3..19].copy_from_slice(&request[3..19]);
    response[19] = COMMAND_RESULT_SUCCED;
    response
}

fn lock_response(request: &[u8; 64], result: u8, data: Option<&[u8]>) -> [u8; 64] {
    let mut response = [0u8; 64];
    response[0] = MAGIC;
    response[1] = VERSION;
    response[2] = request[2];
    response[3..19].copy_from_slice(&request[3..19]);
    response[19] = result;
    response[20] = if data.is_some() {
        LOCK_FLAG_CONTAINS_DATA
    } else {
        0
    };
    response[21] = request[20];
    response[22..38].copy_from_slice(&request[21..37]);
    response[38..54].copy_from_slice(&request[37..53]);
    response
}

async fn start_server<F, Fut>(handler: F) -> String
where
    F: FnOnce(TcpStream) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        handler(stream).await;
    });
    address
}

async fn start_reconnect_server() -> (String, oneshot::Receiver<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (reconnected_tx, reconnected_rx) = oneshot::channel();
    tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        let mut init = [0u8; 64];
        first.read_exact(&mut init).await.unwrap();
        assert_eq!(init[2], COMMAND_TYPE_INIT);
        first.write_all(&init_response(&init)).await.unwrap();
        drop(first);

        let (mut second, _) = listener.accept().await.unwrap();
        let mut init = [0u8; 64];
        second.read_exact(&mut init).await.unwrap();
        assert_eq!(init[2], COMMAND_TYPE_INIT);
        second.write_all(&init_response(&init)).await.unwrap();
        reconnected_tx.send(()).unwrap();

        let mut ping = [0u8; 64];
        second.read_exact(&mut ping).await.unwrap();
        assert_eq!(ping[2], COMMAND_TYPE_PING);
        second.write_all(&ping_response(&ping)).await.unwrap();
    });
    (address, reconnected_rx)
}

async fn read_extra_if_present(stream: &mut TcpStream, request: &[u8; 64]) {
    if (request[19] & LOCK_FLAG_CONTAINS_DATA) == 0 {
        return;
    }
    let mut len_bytes = [0u8; 4];
    stream.read_exact(&mut len_bytes).await.unwrap();
    let len = u32::from_le_bytes(len_bytes) as usize;
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await.unwrap();
}

#[tokio::test]
async fn async_open_sends_init_first_and_ping_matches_request_id() {
    let address = start_server(|mut stream| async move {
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).await.unwrap();
        assert_eq!(init[2], COMMAND_TYPE_INIT);
        stream.write_all(&init_response(&init)).await.unwrap();

        let mut ping = [0u8; 64];
        stream.read_exact(&mut ping).await.unwrap();
        assert_eq!(ping[2], COMMAND_TYPE_PING);
        stream.write_all(&ping_response(&ping)).await.unwrap();
    })
    .await;

    let client = Client::connect(address).await.unwrap();
    assert!(client.ping().await.unwrap());
}

#[tokio::test]
async fn async_pending_request_is_removed_after_timeout() {
    let address = start_server(|mut stream| async move {
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).await.unwrap();
        stream.write_all(&init_response(&init)).await.unwrap();
        let mut ping = [0u8; 64];
        stream.read_exact(&mut ping).await.unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;
    })
    .await;

    let options = ClientOptions {
        command_timeout_grace: Duration::from_millis(50),
        ..ClientOptions::default()
    };
    let client = Client::with_options(address, options);
    client.open().await.unwrap();
    let err = client.ping().await.unwrap_err();
    assert!(matches!(err, SlockError::CommandTimeout));
    assert_eq!(client.pending_len().await, 0);
}

#[tokio::test]
async fn async_close_wakes_pending_request() {
    let address = start_server(|mut stream| async move {
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).await.unwrap();
        stream.write_all(&init_response(&init)).await.unwrap();
        let mut ping = [0u8; 64];
        stream.read_exact(&mut ping).await.unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
    })
    .await;

    let options = ClientOptions {
        command_timeout_grace: Duration::from_secs(5),
        ..ClientOptions::default()
    };
    let client = Client::with_options(address, options);
    client.open().await.unwrap();
    let ping_client = client.clone();
    let waiter = tokio::spawn(async move { ping_client.ping().await.unwrap_err() });
    while client.pending_len().await == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    client.close().await;
    let err = waiter.await.unwrap();
    assert!(matches!(err, SlockError::ClientClosed));
}

#[tokio::test]
async fn async_reader_task_reconnects_after_disconnect_until_close() {
    let (address, reconnected_rx) = start_reconnect_server().await;
    let options = ClientOptions {
        reconnect_interval: Duration::from_millis(20),
        command_timeout_grace: Duration::from_millis(50),
        ..ClientOptions::default()
    };
    let client = Client::with_options(address, options);
    client.open().await.unwrap();

    tokio::time::timeout(Duration::from_secs(2), reconnected_rx)
        .await
        .expect("reader task did not reconnect")
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        match client.ping().await {
            Ok(true) => break,
            Err(SlockError::Io(_))
            | Err(SlockError::NotConnected)
            | Err(SlockError::CommandTimeout)
                if tokio::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            other => panic!("ping did not succeed after reconnect: {other:?}"),
        }
    }

    client.close().await;
}

#[tokio::test]
async fn async_cancelled_command_removes_pending_request() {
    let address = start_server(|mut stream| async move {
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).await.unwrap();
        stream.write_all(&init_response(&init)).await.unwrap();
        let mut ping = [0u8; 64];
        stream.read_exact(&mut ping).await.unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
    })
    .await;

    let options = ClientOptions {
        command_timeout_grace: Duration::from_secs(5),
        ..ClientOptions::default()
    };
    let client = Client::with_options(address, options);
    client.open().await.unwrap();
    let ping_client = client.clone();
    let waiter = tokio::spawn(async move { ping_client.ping().await });
    while client.pending_len().await == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());

    for _ in 0..50 {
        if client.pending_len().await == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(client.pending_len().await, 0);
}

#[tokio::test]
async fn async_lock_success_updates_current_data() {
    let address = start_server(|mut stream| async move {
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).await.unwrap();
        stream.write_all(&init_response(&init)).await.unwrap();

        let mut acquire = [0u8; 64];
        stream.read_exact(&mut acquire).await.unwrap();
        assert_eq!(acquire[2], COMMAND_TYPE_LOCK);
        read_extra_if_present(&mut stream, &acquire).await;
        let response_data = [LOCK_DATA_COMMAND_TYPE_SET, 0, b'a', b'a', b'a'];
        stream
            .write_all(&lock_response(
                &acquire,
                COMMAND_RESULT_SUCCED,
                Some(&response_data),
            ))
            .await
            .unwrap();
        stream
            .write_all(&(response_data.len() as u32).to_le_bytes())
            .await
            .unwrap();
        stream.write_all(&response_data).await.unwrap();
    })
    .await;

    let client = Client::connect(address).await.unwrap();
    let mut lock = client.lock("async-mock-lock", 0, 10);
    lock.acquire_with_data(LockData::set("bbb")).await.unwrap();
    assert_eq!(lock.current_data().unwrap().as_string().unwrap(), "aaa");
}

#[tokio::test]
async fn async_lock_guard_releases_only_when_explicitly_awaited() {
    let address = start_server(|mut stream| async move {
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).await.unwrap();
        stream.write_all(&init_response(&init)).await.unwrap();

        let mut acquire = [0u8; 64];
        stream.read_exact(&mut acquire).await.unwrap();
        assert_eq!(acquire[2], COMMAND_TYPE_LOCK);
        stream
            .write_all(&lock_response(&acquire, COMMAND_RESULT_SUCCED, None))
            .await
            .unwrap();

        let mut release = [0u8; 64];
        stream.read_exact(&mut release).await.unwrap();
        assert_eq!(release[2], COMMAND_TYPE_UNLOCK);
        stream
            .write_all(&lock_response(&release, COMMAND_RESULT_SUCCED, None))
            .await
            .unwrap();
    })
    .await;

    let client = Client::connect(address).await.unwrap();
    let mut lock = client.lock("async-guard-lock", 0, 10);
    let guard = lock.acquire_guard().await.unwrap();
    guard.release().await.unwrap();
}

#[tokio::test]
async fn async_lock_maps_server_result_codes() {
    type ErrorMatcher = fn(&SlockError) -> bool;
    let cases: &[(u8, ErrorMatcher)] = &[
        (COMMAND_RESULT_LOCKED_ERROR, is_lock_locked),
        (COMMAND_RESULT_UNLOCK_ERROR, is_lock_unlocked),
        (COMMAND_RESULT_UNOWN_ERROR, is_lock_not_own),
        (COMMAND_RESULT_TIMEOUT, is_lock_timeout),
    ];

    for (result_code, expected) in cases {
        let result_code = *result_code;
        let address = start_server(move |mut stream| async move {
            let mut init = [0u8; 64];
            stream.read_exact(&mut init).await.unwrap();
            stream.write_all(&init_response(&init)).await.unwrap();

            let mut acquire = [0u8; 64];
            stream.read_exact(&mut acquire).await.unwrap();
            stream
                .write_all(&lock_response(&acquire, result_code, None))
                .await
                .unwrap();
        })
        .await;

        let client = Client::connect(address).await.unwrap();
        let mut lock = client.lock(format!("async-error-{result_code}"), 0, 10);
        let err = lock.acquire().await.unwrap_err();
        assert!(
            expected(&err),
            "unexpected mapping for {result_code}: {err:?}"
        );
    }
}

fn is_lock_locked(err: &SlockError) -> bool {
    matches!(err, SlockError::LockLocked(_))
}

fn is_lock_unlocked(err: &SlockError) -> bool {
    matches!(err, SlockError::LockUnlocked(_))
}

fn is_lock_not_own(err: &SlockError) -> bool {
    matches!(err, SlockError::LockNotOwn(_))
}

fn is_lock_timeout(err: &SlockError) -> bool {
    matches!(err, SlockError::LockTimeout(_))
}

#[tokio::test]
async fn async_open_times_out_when_init_response_is_silent() {
    let address = start_server(|mut stream| async move {
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).await.unwrap();
        assert_eq!(init[2], COMMAND_TYPE_INIT);
        tokio::time::sleep(Duration::from_secs(2)).await;
    })
    .await;

    let options = ClientOptions {
        connect_timeout: Duration::from_millis(50),
        ..ClientOptions::default()
    };
    let client = Client::with_options(address, options);
    let start = std::time::Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(1), client.open())
        .await
        .expect("open must be bounded by connect_timeout");
    assert!(matches!(result, Err(SlockError::CommandTimeout)));
    assert!(start.elapsed() < Duration::from_secs(1));
}

#[tokio::test]
async fn async_cancelled_open_does_not_block_next_open() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (first_seen_tx, first_seen_rx) = oneshot::channel();
    tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        let mut init = [0u8; 64];
        first.read_exact(&mut init).await.unwrap();
        first_seen_tx.send(()).unwrap();
        // Give the cancelled open time to drop its socket before accepting again.
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(first);

        let (mut second, _) = listener.accept().await.unwrap();
        let mut init = [0u8; 64];
        second.read_exact(&mut init).await.unwrap();
        second.write_all(&init_response(&init)).await.unwrap();
        let mut ping = [0u8; 64];
        second.read_exact(&mut ping).await.unwrap();
        second.write_all(&ping_response(&ping)).await.unwrap();
    });

    let options = ClientOptions {
        connect_timeout: Duration::from_secs(2),
        ..ClientOptions::default()
    };
    let client = Client::with_options(address, options);
    let opener = client.clone();
    let first_open = tokio::spawn(async move { opener.open().await });
    first_seen_rx.await.unwrap();

    first_open.abort();
    assert!(first_open.await.unwrap_err().is_cancelled());

    client.open().await.unwrap();
    assert!(client.ping().await.unwrap());
    client.close().await;
}

#[tokio::test]
async fn async_concurrent_open_waits_for_init_handshake() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (init_seen_tx, init_seen_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).await.unwrap();
        assert_eq!(init[2], COMMAND_TYPE_INIT);
        init_seen_tx.send(()).unwrap();
        release_rx.await.unwrap();
        stream.write_all(&init_response(&init)).await.unwrap();
        let mut ping = [0u8; 64];
        stream.read_exact(&mut ping).await.unwrap();
        stream.write_all(&ping_response(&ping)).await.unwrap();
    });

    let options = ClientOptions {
        connect_timeout: Duration::from_secs(2),
        ..ClientOptions::default()
    };
    let client = Client::with_options(address, options);
    let first_client = client.clone();
    let first_open = tokio::spawn(async move { first_client.open().await });
    init_seen_rx.await.unwrap();

    let second_client = client.clone();
    let second_open = tokio::spawn(async move { second_client.open().await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !first_open.is_finished(),
        "first open reported ready before INIT completed"
    );
    assert!(
        !second_open.is_finished(),
        "second open reported ready before INIT completed"
    );

    release_tx.send(()).unwrap();
    first_open.await.unwrap().unwrap();
    second_open.await.unwrap().unwrap();
    assert!(client.ping().await.unwrap());
    client.close().await;
}

#[tokio::test]
async fn async_close_then_reopen_ping_uses_new_reader() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        let mut init = [0u8; 64];
        first.read_exact(&mut init).await.unwrap();
        first.write_all(&init_response(&init)).await.unwrap();
        // Keep the first connection open. A leaked reader would stay blocked here.
        let (mut second, _) = listener.accept().await.unwrap();
        let mut init = [0u8; 64];
        second.read_exact(&mut init).await.unwrap();
        second.write_all(&init_response(&init)).await.unwrap();
        let mut ping = [0u8; 64];
        second.read_exact(&mut ping).await.unwrap();
        second.write_all(&ping_response(&ping)).await.unwrap();
        drop(first);
    });

    let client = Client::connect(address).await.unwrap();
    client.close().await;
    client.open().await.unwrap();
    assert!(client.ping().await.unwrap());
    client.close().await;
}

#[tokio::test]
async fn async_cancelled_close_drains_pending_and_reopens() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (command_seen_tx, command_seen_rx) = oneshot::channel();
    tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        let mut init = [0u8; 64];
        first.read_exact(&mut init).await.unwrap();
        first.write_all(&init_response(&init)).await.unwrap();

        // Accept a command but never answer it, leaving a pending request.
        let mut command = [0u8; 64];
        first.read_exact(&mut command).await.unwrap();
        command_seen_tx.send(()).unwrap();

        // The cancelled close's cleanup must remove the first writer, but a new
        // connection can still be accepted while the first socket drains.
        let (mut second, _) = listener.accept().await.unwrap();
        let mut init = [0u8; 64];
        second.read_exact(&mut init).await.unwrap();
        second.write_all(&init_response(&init)).await.unwrap();
        let mut ping = [0u8; 64];
        second.read_exact(&mut ping).await.unwrap();
        second.write_all(&ping_response(&ping)).await.unwrap();
        drop(first);
    });

    let options = ClientOptions {
        connect_timeout: Duration::from_secs(2),
        ..ClientOptions::default()
    };
    let client = Client::with_options(address, options);
    client.open().await.unwrap();

    let mut lock = client.lock("cancelled-close", 30, 10);
    let acquire = tokio::spawn(async move { lock.acquire().await });
    command_seen_rx.await.unwrap();
    assert_eq!(client.pending_len().await, 1);

    // Poll close exactly once so it reaches its cleanup await, then cancel it.
    let mut close = Box::pin(client.close());
    let mut polled = false;
    let mut completed = false;
    std::future::poll_fn(|cx| {
        if !polled {
            polled = true;
            if std::future::Future::poll(close.as_mut(), cx).is_ready() {
                completed = true;
            }
        }
        std::task::Poll::Ready(())
    })
    .await;
    drop(close);
    assert!(!completed, "close completed before it could be cancelled");

    // Reopen must wait for the retained cleanup task, then serve the new socket.
    client.open().await.unwrap();
    assert_eq!(client.pending_len().await, 0);
    assert!(client.ping().await.unwrap());
    let err = acquire.await.unwrap().unwrap_err();
    assert!(
        matches!(err, SlockError::ClientClosed | SlockError::Io(_)),
        "unexpected error: {err:?}"
    );
    client.close().await;
}

#[tokio::test]
async fn async_rejects_oversized_extra_frame() {
    let payload = [0u8; 16];
    let address = start_server(move |mut stream| async move {
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).await.unwrap();
        stream.write_all(&init_response(&init)).await.unwrap();

        let mut acquire = [0u8; 64];
        stream.read_exact(&mut acquire).await.unwrap();
        assert_eq!(acquire[2], COMMAND_TYPE_LOCK);
        read_extra_if_present(&mut stream, &acquire).await;
        let response = lock_response(&acquire, COMMAND_RESULT_SUCCED, Some(&payload));
        stream.write_all(&response).await.unwrap();
        stream
            .write_all(&(payload.len() as u32).to_le_bytes())
            .await
            .unwrap();
        stream.write_all(&payload).await.unwrap();
    })
    .await;

    let options = ClientOptions {
        max_frame_size: 8,
        ..ClientOptions::default()
    };
    let client = Client::with_options(address, options);
    client.open().await.unwrap();
    let mut lock = client.lock("async-max-frame", 0, 10);
    let err = lock.acquire().await.unwrap_err();
    assert!(
        matches!(err, SlockError::Protocol(_)),
        "unexpected error: {err:?}"
    );
    client.close().await;
}

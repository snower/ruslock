#![cfg(feature = "replset")]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

use ruslock::protocol::constants::*;
use ruslock::ClientOptions;

fn init_response(request: &[u8; 64]) -> [u8; 64] {
    init_response_with_type(request, 0)
}

fn init_response_with_type(request: &[u8; 64], init_type: u8) -> [u8; 64] {
    let mut response = [0u8; 64];
    response[0] = MAGIC;
    response[1] = VERSION;
    response[2] = COMMAND_TYPE_INIT;
    response[3..19].copy_from_slice(&request[3..19]);
    response[19] = COMMAND_RESULT_SUCCED;
    response[20] = init_type;
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

fn lock_response(request: &[u8; 64], result: u8) -> [u8; 64] {
    let mut response = [0u8; 64];
    response[0] = MAGIC;
    response[1] = VERSION;
    response[2] = request[2];
    response[3..19].copy_from_slice(&request[3..19]);
    response[19] = result;
    response[21] = request[20];
    response[22..38].copy_from_slice(&request[21..37]);
    response[38..54].copy_from_slice(&request[37..53]);
    response
}

#[cfg(feature = "blocking")]
fn start_blocking_server<F>(handler: F) -> String
where
    F: FnOnce(TcpStream) + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        handler(stream);
    });
    address
}

#[cfg(feature = "blocking")]
fn unused_blocking_address() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    drop(listener);
    address
}

#[cfg(feature = "blocking")]
#[test]
fn blocking_replset_falls_back_to_first_live_node() {
    let address = start_blocking_server(|mut stream| {
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).unwrap();
        assert_eq!(init[2], COMMAND_TYPE_INIT);
        stream.write_all(&init_response(&init)).unwrap();

        let mut ping = [0u8; 64];
        stream.read_exact(&mut ping).unwrap();
        assert_eq!(ping[2], COMMAND_TYPE_PING);
        stream.write_all(&ping_response(&ping)).unwrap();
    });
    let options = ClientOptions {
        connect_timeout: Duration::from_millis(50),
        ..ClientOptions::default()
    };
    let nodes = format!("127.0.0.1:1,{address}");
    let client = ruslock::blocking::ReplsetClient::with_options(nodes, options);

    client.open().unwrap();
    assert!(client.ping().unwrap());
}

#[cfg(feature = "blocking")]
#[test]
fn blocking_replset_prefers_leader_over_first_live_node() {
    let follower = start_blocking_server(|mut stream| {
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).unwrap();
        assert_eq!(init[2], COMMAND_TYPE_INIT);
        stream
            .write_all(&init_response_with_type(&init, INIT_TYPE_FLAG_HAS_LEADER))
            .unwrap();
        thread::sleep(Duration::from_millis(250));
    });
    let leader = start_blocking_server(|mut stream| {
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).unwrap();
        assert_eq!(init[2], COMMAND_TYPE_INIT);
        stream
            .write_all(&init_response_with_type(&init, INIT_TYPE_FLAG_IS_LEADER))
            .unwrap();

        let mut ping = [0u8; 64];
        stream.read_exact(&mut ping).unwrap();
        assert_eq!(ping[2], COMMAND_TYPE_PING);
        stream.write_all(&ping_response(&ping)).unwrap();
    });
    let options = ClientOptions {
        command_timeout_grace: Duration::from_millis(50),
        ..ClientOptions::default()
    };
    let client =
        ruslock::blocking::ReplsetClient::with_options(format!("{follower},{leader}"), options);

    client.open().unwrap();
    assert!(client.ping().unwrap());
}

#[cfg(feature = "blocking")]
#[test]
fn blocking_replset_retries_lock_state_error_on_next_live_node() {
    let first = start_blocking_server(|mut stream| {
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).unwrap();
        stream.write_all(&init_response(&init)).unwrap();

        let mut lock = [0u8; 64];
        stream.read_exact(&mut lock).unwrap();
        assert_eq!(lock[2], COMMAND_TYPE_LOCK);
        stream
            .write_all(&lock_response(&lock, COMMAND_RESULT_STATE_ERROR))
            .unwrap();
    });
    let second = start_blocking_server(|mut stream| {
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).unwrap();
        stream.write_all(&init_response(&init)).unwrap();

        let mut lock = [0u8; 64];
        stream.read_exact(&mut lock).unwrap();
        assert_eq!(lock[2], COMMAND_TYPE_LOCK);
        stream
            .write_all(&lock_response(&lock, COMMAND_RESULT_SUCCED))
            .unwrap();
    });
    let client = ruslock::blocking::ReplsetClient::connect(format!("{first},{second}")).unwrap();

    let mut lock = client.lock("replset-state-retry", 0, 10);
    lock.acquire().unwrap();
}

#[cfg(feature = "blocking")]
#[test]
fn blocking_replset_retries_lock_after_transport_failure() {
    let first = start_blocking_server(|mut stream| {
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).unwrap();
        stream.write_all(&init_response(&init)).unwrap();

        let mut lock = [0u8; 64];
        stream.read_exact(&mut lock).unwrap();
        assert_eq!(lock[2], COMMAND_TYPE_LOCK);
    });
    let second = start_blocking_server(|mut stream| {
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).unwrap();
        stream.write_all(&init_response(&init)).unwrap();

        let mut lock = [0u8; 64];
        stream.read_exact(&mut lock).unwrap();
        assert_eq!(lock[2], COMMAND_TYPE_LOCK);
        stream
            .write_all(&lock_response(&lock, COMMAND_RESULT_SUCCED))
            .unwrap();
    });
    let client = ruslock::blocking::ReplsetClient::connect(format!("{first},{second}")).unwrap();

    let mut lock = client.lock("replset-transport-retry", 0, 10);
    lock.acquire().unwrap();
}

#[cfg(feature = "blocking")]
#[test]
fn blocking_replset_waits_for_node_to_appear_after_all_nodes_are_down() {
    let address = unused_blocking_address();
    let server_address = address.clone();
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        let listener = TcpListener::bind(server_address).unwrap();
        let (mut stream, _) = listener.accept().unwrap();

        let mut init = [0u8; 64];
        stream.read_exact(&mut init).unwrap();
        stream.write_all(&init_response(&init)).unwrap();

        let mut lock = [0u8; 64];
        stream.read_exact(&mut lock).unwrap();
        assert_eq!(lock[2], COMMAND_TYPE_LOCK);
        stream
            .write_all(&lock_response(&lock, COMMAND_RESULT_SUCCED))
            .unwrap();
    });
    let options = ClientOptions {
        connect_timeout: Duration::from_millis(20),
        command_timeout_grace: Duration::from_secs(1),
        ..ClientOptions::default()
    };
    let client = ruslock::blocking::ReplsetClient::with_options(address, options);

    let mut lock = client.lock("replset-pending-wakeup", 0, 10);
    lock.acquire().unwrap();
}

#[cfg(feature = "aio")]
async fn start_async_server<F, Fut>(handler: F) -> String
where
    F: FnOnce(tokio::net::TcpStream) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        handler(stream).await;
    });
    address
}

#[cfg(feature = "aio")]
fn unused_async_address() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    drop(listener);
    address
}

#[cfg(feature = "aio")]
#[tokio::test]
async fn async_replset_falls_back_to_first_live_node() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let address = start_async_server(|mut stream| async move {
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
    let options = ClientOptions {
        connect_timeout: Duration::from_millis(50),
        ..ClientOptions::default()
    };
    let nodes = format!("127.0.0.1:1,{address}");
    let client = ruslock::aio::ReplsetClient::with_options(nodes, options);

    client.open().await.unwrap();
    assert!(client.ping().await.unwrap());
}

#[cfg(feature = "aio")]
#[tokio::test]
async fn async_replset_prefers_leader_over_first_live_node() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let follower = start_async_server(|mut stream| async move {
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).await.unwrap();
        assert_eq!(init[2], COMMAND_TYPE_INIT);
        stream
            .write_all(&init_response_with_type(&init, INIT_TYPE_FLAG_HAS_LEADER))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;
    })
    .await;
    let leader = start_async_server(|mut stream| async move {
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).await.unwrap();
        assert_eq!(init[2], COMMAND_TYPE_INIT);
        stream
            .write_all(&init_response_with_type(&init, INIT_TYPE_FLAG_IS_LEADER))
            .await
            .unwrap();

        let mut ping = [0u8; 64];
        stream.read_exact(&mut ping).await.unwrap();
        assert_eq!(ping[2], COMMAND_TYPE_PING);
        stream.write_all(&ping_response(&ping)).await.unwrap();
    })
    .await;
    let options = ClientOptions {
        command_timeout_grace: Duration::from_millis(50),
        ..ClientOptions::default()
    };
    let client = ruslock::aio::ReplsetClient::with_options(format!("{follower},{leader}"), options);

    client.open().await.unwrap();
    assert!(client.ping().await.unwrap());
}

#[cfg(feature = "aio")]
#[tokio::test]
async fn async_replset_retries_lock_state_error_on_next_live_node() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let first = start_async_server(|mut stream| async move {
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).await.unwrap();
        stream.write_all(&init_response(&init)).await.unwrap();

        let mut lock = [0u8; 64];
        stream.read_exact(&mut lock).await.unwrap();
        assert_eq!(lock[2], COMMAND_TYPE_LOCK);
        stream
            .write_all(&lock_response(&lock, COMMAND_RESULT_STATE_ERROR))
            .await
            .unwrap();
    })
    .await;
    let second = start_async_server(|mut stream| async move {
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).await.unwrap();
        stream.write_all(&init_response(&init)).await.unwrap();

        let mut lock = [0u8; 64];
        stream.read_exact(&mut lock).await.unwrap();
        assert_eq!(lock[2], COMMAND_TYPE_LOCK);
        stream
            .write_all(&lock_response(&lock, COMMAND_RESULT_SUCCED))
            .await
            .unwrap();
    })
    .await;
    let client = ruslock::aio::ReplsetClient::connect(format!("{first},{second}"))
        .await
        .unwrap();

    let mut lock = client.lock("replset-state-retry", 0, 10);
    lock.acquire().await.unwrap();
}

#[cfg(feature = "aio")]
#[tokio::test]
async fn async_replset_retries_lock_after_transport_failure() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let first = start_async_server(|mut stream| async move {
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).await.unwrap();
        stream.write_all(&init_response(&init)).await.unwrap();

        let mut lock = [0u8; 64];
        stream.read_exact(&mut lock).await.unwrap();
        assert_eq!(lock[2], COMMAND_TYPE_LOCK);
    })
    .await;
    let second = start_async_server(|mut stream| async move {
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).await.unwrap();
        stream.write_all(&init_response(&init)).await.unwrap();

        let mut lock = [0u8; 64];
        stream.read_exact(&mut lock).await.unwrap();
        assert_eq!(lock[2], COMMAND_TYPE_LOCK);
        stream
            .write_all(&lock_response(&lock, COMMAND_RESULT_SUCCED))
            .await
            .unwrap();
    })
    .await;
    let client = ruslock::aio::ReplsetClient::connect(format!("{first},{second}"))
        .await
        .unwrap();

    let mut lock = client.lock("replset-transport-retry", 0, 10);
    lock.acquire().await.unwrap();
}

#[cfg(feature = "aio")]
#[tokio::test]
async fn async_replset_waits_for_node_to_appear_after_all_nodes_are_down() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let address = unused_async_address();
    let server_address = address.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let listener = tokio::net::TcpListener::bind(server_address).await.unwrap();
        let (mut stream, _) = listener.accept().await.unwrap();

        let mut init = [0u8; 64];
        stream.read_exact(&mut init).await.unwrap();
        stream.write_all(&init_response(&init)).await.unwrap();

        let mut lock = [0u8; 64];
        stream.read_exact(&mut lock).await.unwrap();
        assert_eq!(lock[2], COMMAND_TYPE_LOCK);
        stream
            .write_all(&lock_response(&lock, COMMAND_RESULT_SUCCED))
            .await
            .unwrap();
    });
    let options = ClientOptions {
        connect_timeout: Duration::from_millis(20),
        command_timeout_grace: Duration::from_secs(1),
        ..ClientOptions::default()
    };
    let client = ruslock::aio::ReplsetClient::with_options(address, options);

    let mut lock = client.lock("replset-pending-wakeup", 0, 10);
    lock.acquire().await.unwrap();
}

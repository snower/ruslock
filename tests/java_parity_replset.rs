#![cfg(all(feature = "blocking", feature = "aio", feature = "replset"))]

use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

fn replset_nodes() -> String {
    std::env::var("SLOCK_REPLSET_NODES").unwrap_or_else(|_| {
        let host = std::env::var("SLOCK_TEST_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
        let port = std::env::var("SLOCK_TEST_PORT").unwrap_or_else(|_| "5658".to_string());
        format!("{host}:{port}")
    })
}

fn slock_available() -> bool {
    let first = replset_nodes().split(',').next().unwrap_or_default().to_string();
    let Ok(mut addrs) = first.to_socket_addrs() else {
        return false;
    };
    let Some(addr) = addrs.next() else {
        return false;
    };
    TcpStream::connect_timeout(&addr, Duration::from_millis(100)).is_ok()
}

#[test]
fn test_replset_client_lock() {
    if !slock_available() {
        eprintln!("skipping parity test because local slock is not available");
        return;
    }
    let client = ruslock::blocking::ReplsetClient::connect(replset_nodes()).unwrap();
    let mut lock = client.lock("test2", 5, 5);
    lock.acquire().unwrap();
    lock.release().unwrap();
}

#[tokio::test]
async fn test_replset_client_async_lock() {
    if !slock_available() {
        eprintln!("skipping parity test because local slock is not available");
        return;
    }
    let client = ruslock::aio::ReplsetClient::connect(replset_nodes()).await.unwrap();
    let mut lock = client.lock("test_async2", 5, 5);
    lock.acquire().await.unwrap();
    lock.release().await.unwrap();
}

#![cfg(all(feature = "blocking", feature = "aio"))]

use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

fn endpoint() -> String {
    let host = std::env::var("SLOCK_TEST_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = std::env::var("SLOCK_TEST_PORT").unwrap_or_else(|_| "5658".to_string());
    format!("{host}:{port}")
}

fn slock_available() -> bool {
    let address = endpoint();
    let Ok(mut addrs) = address.to_socket_addrs() else {
        return false;
    };
    let Some(addr) = addrs.next() else {
        return false;
    };
    TcpStream::connect_timeout(&addr, Duration::from_millis(100)).is_ok()
}

#[test]
fn test_tree_lock() {
    if !slock_available() {
        eprintln!("skipping parity test because local slock is not available");
        return;
    }
    let client = ruslock::blocking::Client::connect(endpoint()).unwrap();
    let mut root = client.tree_lock("treeLock1", 0, 60);
    root.acquire().unwrap();
    root.release().unwrap();
}

#[test]
fn test_max_concurrent_flow() {
    if !slock_available() {
        eprintln!("skipping parity test because local slock is not available");
        return;
    }
    let client = ruslock::blocking::Client::connect(endpoint()).unwrap();
    let mut flow = client.max_concurrent_flow("maxconcurrentflow1", 5, 0, 10);
    flow.acquire().unwrap();
    flow.release().unwrap();
}

#[tokio::test]
async fn test_max_concurrent_flow_async() {
    if !slock_available() {
        eprintln!("skipping parity test because local slock is not available");
        return;
    }
    let client = ruslock::aio::Client::connect(endpoint()).await.unwrap();
    let mut flow = client.max_concurrent_flow("maxconcurrentflow1", 5, 0, 60);
    flow.acquire().await.unwrap();
    flow.release().await.unwrap();
}

#[test]
fn test_token_bucket_flow() {
    if !slock_available() {
        eprintln!("skipping parity test because local slock is not available");
        return;
    }
    let client = ruslock::blocking::Client::connect(endpoint()).unwrap();
    let mut flow = client.token_bucket_flow("tokenbucketflow1", 5, 0, 0.1);
    flow.acquire().unwrap();
}

#[tokio::test]
async fn test_token_bucket_flow_async() {
    if !slock_available() {
        eprintln!("skipping parity test because local slock is not available");
        return;
    }
    let client = ruslock::aio::Client::connect(endpoint()).await.unwrap();
    let mut flow = client.token_bucket_flow("tokenbucketflow2", 5, 0, 0.1);
    flow.acquire().await.unwrap();
}

#[test]
fn test_priority_lock() {
    if !slock_available() {
        eprintln!("skipping parity test because local slock is not available");
        return;
    }
    let client = ruslock::blocking::Client::connect(endpoint()).unwrap();
    let mut lock = client.priority_lock("testPriorityLock", 10, 5, 10);
    assert_eq!(lock.priority(), 10);
    lock.acquire().unwrap();
    lock.release().unwrap();
}

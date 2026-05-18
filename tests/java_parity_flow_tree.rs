#![cfg(all(feature = "blocking", feature = "aio"))]

use std::net::{TcpStream, ToSocketAddrs};
use std::thread;
use std::time::Duration;

use ruslock::SlockError;

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

fn unique_key(prefix: &str) -> String {
    format!("{prefix}-{}", std::process::id())
}

#[test]
fn test_tree_lock() {
    if !slock_available() {
        eprintln!("skipping parity test because local slock is not available");
        return;
    }
    let client = ruslock::blocking::Client::connect(endpoint()).unwrap();
    let root_key = unique_key("treeLock1");
    let mut root = client.tree_lock(&root_key, 5, 10);
    root.acquire().unwrap();
    root.release().unwrap();
    root.wait(1).unwrap();

    let mut child = root.new_child(unique_key("treeLock1-child"));
    child.acquire().unwrap();
    child.release().unwrap();

    root.wait(1).unwrap();
    let mut test_lock = client.lock(root.lock_key().as_bytes(), 0, 0);
    test_lock.acquire().unwrap();
}

#[test]
fn test_max_concurrent_flow() {
    if !slock_available() {
        eprintln!("skipping parity test because local slock is not available");
        return;
    }
    let client = ruslock::blocking::Client::connect(endpoint()).unwrap();
    let key = unique_key("maxconcurrentflow1");
    let mut flow1 = client.max_concurrent_flow(&key, 5, 0, 10);
    let mut flow2 = client.max_concurrent_flow(&key, 5, 0, 10);
    flow1.acquire().unwrap();
    flow2.acquire().unwrap();
    flow1.release().unwrap();
    flow2.release().unwrap();

    let mut lock = client.lock(&key, 0, 10);
    lock.acquire().unwrap();
    lock.release().unwrap();

    let mut flows = Vec::new();
    for _ in 0..5 {
        let mut flow = client.max_concurrent_flow(&key, 5, 0, 10);
        flow.acquire().unwrap();
        flows.push(flow);
    }
    let err = flow1.acquire().unwrap_err();
    assert!(matches!(err, SlockError::LockTimeout(_)));
    for flow in &mut flows {
        flow.release().unwrap();
    }
    flow1.acquire().unwrap();
    flow1.release().unwrap();
}

#[tokio::test]
async fn test_max_concurrent_flow_async() {
    if !slock_available() {
        eprintln!("skipping parity test because local slock is not available");
        return;
    }
    let client = ruslock::aio::Client::connect(endpoint()).await.unwrap();
    let key = unique_key("maxconcurrentflow1-async");
    let mut flow1 = client.max_concurrent_flow(&key, 5, 0, 60);
    let mut flow2 = client.max_concurrent_flow(&key, 5, 0, 60);
    flow1.acquire().await.unwrap();
    flow2.acquire().await.unwrap();
    flow1.release().await.unwrap();
    flow2.release().await.unwrap();
}

#[test]
fn test_token_bucket_flow() {
    if !slock_available() {
        eprintln!("skipping parity test because local slock is not available");
        return;
    }
    let client = ruslock::blocking::Client::connect(endpoint()).unwrap();
    let key = unique_key("tokenbucketflow1");
    let mut flow1 = client.token_bucket_flow(&key, 5, 0, 0.1);
    let mut flow2 = client.token_bucket_flow(&key, 5, 0, 0.1);
    flow1.acquire().unwrap();
    flow2.acquire().unwrap();

    thread::sleep(Duration::from_millis(200));
    let mut lock = client.lock(&key, 0, 10);
    lock.acquire().unwrap();
    lock.release().unwrap();
}

#[tokio::test]
async fn test_token_bucket_flow_async() {
    if !slock_available() {
        eprintln!("skipping parity test because local slock is not available");
        return;
    }
    let client = ruslock::aio::Client::connect(endpoint()).await.unwrap();
    let key = unique_key("tokenbucketflow2");
    let mut flow1 = client.token_bucket_flow(&key, 5, 0, 0.1);
    let mut flow2 = client.token_bucket_flow(&key, 5, 0, 0.1);
    flow1.acquire().await.unwrap();
    flow2.acquire().await.unwrap();
}

#[test]
fn test_priority_lock() {
    if !slock_available() {
        eprintln!("skipping parity test because local slock is not available");
        return;
    }
    let client = ruslock::blocking::Client::connect(endpoint()).unwrap();
    let mut lock = client.priority_lock(unique_key("testPriorityLock"), 10, 5, 10);
    assert_eq!(lock.priority(), 10);
    lock.acquire().unwrap();
    lock.release().unwrap();
}

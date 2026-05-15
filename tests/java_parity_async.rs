#![cfg(feature = "aio")]

use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use ruslock::aio::Client;

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

async fn client_or_skip() -> Option<Client> {
    if !slock_available() {
        eprintln!("skipping parity test because local slock is not available");
        return None;
    }
    Some(Client::connect(endpoint()).await.unwrap())
}

#[tokio::test]
async fn test_client_async_lock() {
    let Some(client) = client_or_skip().await else {
        return;
    };
    let mut lock = client.lock("test_async1", 5, 5);
    lock.acquire().await.unwrap();
    lock.release().await.unwrap();
}

#[tokio::test]
async fn test_event_async_default_seted() {
    let Some(client) = client_or_skip().await else {
        return;
    };
    let mut event = client.event("event_async1", 5, 60, true);
    event.clear().await.unwrap();
    event.set().await.unwrap();
    event.wait(2).await.unwrap();
}

#[tokio::test]
async fn test_event_async_default_unseted() {
    let Some(client) = client_or_skip().await else {
        return;
    };
    let mut event = client.event("event_async2", 5, 60, false);
    event.set().await.unwrap();
    event.clear().await.unwrap();
}

#[tokio::test]
async fn test_group_event_async() {
    let Some(client) = client_or_skip().await else {
        return;
    };
    let mut group_event = client.group_event("groupEvent2", 1, 1, 5, 10);
    let _ = group_event.is_set().await.unwrap();
    group_event.clear().await.unwrap();
    group_event.wakeup(None).await.unwrap();
    assert_eq!(group_event.version_id(), 2);
}

#[tokio::test]
async fn test_read_write_lock_async() {
    let Some(client) = client_or_skip().await else {
        return;
    };
    let mut lock = client.read_write_lock("readWriteLock2", 0, 60);
    lock.acquire_read().await.unwrap();
    lock.release_read().await.unwrap();
}

#[tokio::test]
async fn test_reentrant_lock_async() {
    let Some(client) = client_or_skip().await else {
        return;
    };
    let mut lock = client.reentrant_lock("reentrantLock2", 5, 60);
    lock.acquire().await.unwrap();
    lock.release().await.unwrap();
}

#[tokio::test]
async fn test_semaphore_async() {
    let Some(client) = client_or_skip().await else {
        return;
    };
    let mut semaphore = client.semaphore("semaphore2", 10, 0, 60);
    semaphore.acquire().await.unwrap();
    semaphore.release().await.unwrap();
}

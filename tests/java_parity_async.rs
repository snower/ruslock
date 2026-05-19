#![cfg(feature = "aio")]

use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use ruslock::aio::Client;
use ruslock::{LockData, SlockError};

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

fn unique_key(prefix: &str) -> String {
    format!("{prefix}-{}", std::process::id())
}

#[tokio::test]
async fn test_client_async_lock() {
    let Some(client) = client_or_skip().await else {
        return;
    };
    let mut lock = client.lock("test_async1", 5, 5);
    lock.acquire().await.unwrap();
    lock.release().await.unwrap();

    let key = unique_key("test-async1-data");
    let mut lock1 = client.lock(&key, 5, 5);
    lock1.set_count(10);
    let mut lock2 = client.lock(&key, 5, 5);
    lock2.set_count(10);
    lock1.acquire_with_data(LockData::set("aaa")).await.unwrap();
    assert!(lock1.current_data().is_none());
    lock2.acquire_with_data(LockData::set("bbb")).await.unwrap();
    assert_eq!(lock2.current_data().unwrap().as_string().unwrap(), "aaa");
    lock1.release_with_data(LockData::set("ccc")).await.unwrap();
    assert_eq!(lock1.current_data().unwrap().as_string().unwrap(), "bbb");
    lock2.release().await.unwrap();
    assert_eq!(lock2.current_data().unwrap().as_string().unwrap(), "ccc");
}

#[tokio::test]
async fn test_event_async_default_seted() {
    let Some(client) = client_or_skip().await else {
        return;
    };
    let key = unique_key("event_async1");
    let mut event = client.event(&key, 5, 60, true);
    assert!(event.is_set().await.unwrap());
    event.clear().await.unwrap();
    assert!(!event.is_set().await.unwrap());
    event.set().await.unwrap();
    assert!(event.is_set().await.unwrap());
    event.wait(2).await.unwrap();

    let mut event = client.event(&key, 5, 60, true);
    assert!(event.is_set().await.unwrap());
    event.clear().await.unwrap();
    assert!(!event.is_set().await.unwrap());
    event
        .set_with_data(Some(LockData::set("aaa")))
        .await
        .unwrap();
    assert!(event.is_set().await.unwrap());
    event.wait(2).await.unwrap();
    assert_eq!(event.current_data().unwrap().as_string().unwrap(), "aaa");
}

#[tokio::test]
async fn test_event_async_default_unseted() {
    let Some(client) = client_or_skip().await else {
        return;
    };
    let key = unique_key("event_async2");
    let mut event = client.event(&key, 5, 60, false);
    assert!(!event.is_set().await.unwrap());
    event.set().await.unwrap();
    assert!(event.is_set().await.unwrap());
    event.clear().await.unwrap();
    assert!(!event.is_set().await.unwrap());
    event.set().await.unwrap();
    assert!(event.is_set().await.unwrap());
    event.wait(2).await.unwrap();
    event.clear().await.unwrap();

    let mut event = client.event(&key, 5, 60, false);
    assert!(!event.is_set().await.unwrap());
    event
        .set_with_data(Some(LockData::set("aaa")))
        .await
        .unwrap();
    assert!(event.is_set().await.unwrap());
    event.clear().await.unwrap();
    assert!(!event.is_set().await.unwrap());
    event
        .set_with_data(Some(LockData::set("bbb")))
        .await
        .unwrap();
    assert!(event.is_set().await.unwrap());
    event.wait(2).await.unwrap();
    assert_eq!(event.current_data().unwrap().as_string().unwrap(), "bbb");
    event.clear().await.unwrap();
}

#[tokio::test]
async fn test_group_event_async() {
    let Some(client) = client_or_skip().await else {
        return;
    };
    let key = unique_key("groupEvent2");
    let mut group_event = client.group_event(&key, 1, 1, 5, 10);
    let mut waiter = client.group_event(&key, 2, 0, 5, 60);
    assert!(group_event.is_set().await.unwrap());
    group_event.clear().await.unwrap();
    assert!(!group_event.is_set().await.unwrap());
    group_event
        .wakeup(Some(LockData::set("aaa")))
        .await
        .unwrap();
    assert_eq!(group_event.version_id(), 2);
    assert!(!group_event.is_set().await.unwrap());
    waiter.wait(2).await.unwrap();
    assert_eq!(waiter.version_id(), 2);
    assert_eq!(waiter.current_data().unwrap().as_string().unwrap(), "aaa");
    group_event
        .wakeup(Some(LockData::set("bbb")))
        .await
        .unwrap();
    assert_eq!(group_event.version_id(), 3);
    assert!(!group_event.is_set().await.unwrap());
    waiter.wait(2).await.unwrap();
    assert_eq!(waiter.version_id(), 3);
    assert_eq!(waiter.current_data().unwrap().as_string().unwrap(), "bbb");
    group_event.set().await.unwrap();
    assert!(group_event.is_set().await.unwrap());
    group_event.wait(2).await.unwrap();
    assert_eq!(group_event.version_id(), 3);
}

#[tokio::test]
async fn test_read_write_lock_async() {
    let Some(client) = client_or_skip().await else {
        return;
    };
    let key = unique_key("readWriteLock2");
    let mut read_lock = client.read_write_lock(&key, 0, 60);
    let mut write_lock = client.read_write_lock(&key, 0, 60);
    read_lock.acquire_read().await.unwrap();
    read_lock.acquire_read().await.unwrap();
    read_lock.release_read().await.unwrap();
    read_lock.release_read().await.unwrap();

    read_lock.acquire_read().await.unwrap();
    let err = write_lock.acquire_write().await.unwrap_err();
    assert!(matches!(err, SlockError::LockTimeout(_)));
    read_lock.release_read().await.unwrap();

    write_lock.acquire_write().await.unwrap();
    let err = read_lock.acquire_read().await.unwrap_err();
    assert!(matches!(err, SlockError::LockTimeout(_)));
    write_lock.release_write().await.unwrap();
    read_lock.acquire_read().await.unwrap();
    read_lock.acquire_read().await.unwrap();
    read_lock.release_read().await.unwrap();
    read_lock.release_read().await.unwrap();
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

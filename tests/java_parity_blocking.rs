#![cfg(feature = "blocking")]

use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use ruslock::blocking::Client;
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

fn client_or_skip() -> Option<Client> {
    if !slock_available() {
        eprintln!("skipping parity test because local slock is not available");
        return None;
    }
    Some(Client::connect(endpoint()).unwrap())
}

fn unique_key(prefix: &str) -> String {
    format!("{prefix}-{}", std::process::id())
}

#[test]
fn test_client_lock() {
    let Some(client) = client_or_skip() else {
        return;
    };
    let mut lock = client.lock("test1", 5, 5);
    lock.acquire().unwrap();
    lock.release().unwrap();

    let mut lock1 = client.lock("test1", 5, 5);
    lock1.set_count(10);
    let mut lock2 = client.lock("test1", 5, 5);
    lock2.set_count(10);
    lock1.acquire_with_data(LockData::set("aaa")).unwrap();
    assert!(lock1.current_data().is_none());
    lock2.acquire_with_data(LockData::set("bbb")).unwrap();
    assert_eq!(lock2.current_data().unwrap().as_string().unwrap(), "aaa");
    lock1.release_with_data(LockData::set("ccc")).unwrap();
    assert_eq!(lock1.current_data().unwrap().as_string().unwrap(), "bbb");
    lock2.release().unwrap();
    assert_eq!(lock2.current_data().unwrap().as_string().unwrap(), "ccc");
}

#[test]
fn test_event_default_seted() {
    let Some(client) = client_or_skip() else {
        return;
    };
    let key = unique_key("event1");
    let mut event = client.event(&key, 5, 60, true);
    assert!(event.is_set().unwrap());
    event.clear().unwrap();
    assert!(!event.is_set().unwrap());
    event.set().unwrap();
    assert!(event.is_set().unwrap());
    event.wait(2).unwrap();

    let mut event = client.event(&key, 5, 60, true);
    assert!(event.is_set().unwrap());
    event.clear().unwrap();
    assert!(!event.is_set().unwrap());
    event.set_with_data(Some(LockData::set("aaa"))).unwrap();
    assert!(event.is_set().unwrap());
    event.wait(2).unwrap();
    assert_eq!(event.current_data().unwrap().as_string().unwrap(), "aaa");
}

#[test]
fn test_event_default_unseted() {
    let Some(client) = client_or_skip() else {
        return;
    };
    let key = unique_key("event2");
    let mut event = client.event(&key, 5, 60, false);
    assert!(!event.is_set().unwrap());
    event.set().unwrap();
    assert!(event.is_set().unwrap());
    event.clear().unwrap();
    assert!(!event.is_set().unwrap());
    event.set().unwrap();
    assert!(event.is_set().unwrap());
    event.wait(2).unwrap();
    event.clear().unwrap();

    let mut event = client.event(&key, 5, 60, false);
    assert!(!event.is_set().unwrap());
    event.set_with_data(Some(LockData::set("aaa"))).unwrap();
    assert!(event.is_set().unwrap());
    event.clear().unwrap();
    assert!(!event.is_set().unwrap());
    event.set_with_data(Some(LockData::set("bbb"))).unwrap();
    assert!(event.is_set().unwrap());
    event.wait(2).unwrap();
    assert_eq!(event.current_data().unwrap().as_string().unwrap(), "bbb");
    event.clear().unwrap();
}

#[test]
fn test_group_event() {
    let Some(client) = client_or_skip() else {
        return;
    };
    let key = unique_key("groupEvent1");
    let mut group_event = client.group_event(&key, 1, 1, 5, 60);
    let mut waiter = client.group_event(&key, 2, 0, 5, 60);
    assert!(group_event.is_set().unwrap());
    group_event.clear().unwrap();
    assert!(!group_event.is_set().unwrap());
    group_event.wakeup(Some(LockData::set("aaa"))).unwrap();
    assert_eq!(group_event.version_id(), 2);
    assert!(!group_event.is_set().unwrap());
    waiter.wait(2).unwrap();
    assert_eq!(waiter.version_id(), 2);
    assert_eq!(waiter.current_data().unwrap().as_string().unwrap(), "aaa");
    group_event.wakeup(Some(LockData::set("bbb"))).unwrap();
    assert_eq!(group_event.version_id(), 3);
    assert!(!group_event.is_set().unwrap());
    waiter.wait(2).unwrap();
    assert_eq!(waiter.version_id(), 3);
    assert_eq!(waiter.current_data().unwrap().as_string().unwrap(), "bbb");
    group_event.set().unwrap();
    assert!(group_event.is_set().unwrap());
    group_event.wait(2).unwrap();
    assert_eq!(group_event.version_id(), 3);
}

#[test]
fn test_read_write_lock() {
    let Some(client) = client_or_skip() else {
        return;
    };
    let key = unique_key("readWriteLock1");
    let mut read_lock = client.read_write_lock(&key, 0, 60);
    let mut write_lock = client.read_write_lock(&key, 0, 60);
    read_lock.acquire_read().unwrap();
    read_lock.acquire_read().unwrap();
    read_lock.release_read().unwrap();
    read_lock.release_read().unwrap();

    read_lock.acquire_read().unwrap();
    let err = write_lock.acquire_write().unwrap_err();
    assert!(matches!(err, SlockError::LockTimeout(_)));
    read_lock.release_read().unwrap();

    write_lock.acquire_write().unwrap();
    let err = read_lock.acquire_read().unwrap_err();
    assert!(matches!(err, SlockError::LockTimeout(_)));
    write_lock.release_write().unwrap();
    read_lock.acquire_read().unwrap();
    read_lock.acquire_read().unwrap();
    read_lock.release_read().unwrap();
    read_lock.release_read().unwrap();
}

#[test]
fn test_reentrant_lock() {
    let Some(client) = client_or_skip() else {
        return;
    };
    let mut lock = client.reentrant_lock("reentrantLock1", 5, 60);
    for _ in 0..10 {
        lock.acquire().unwrap();
    }
    for _ in 0..10 {
        lock.release().unwrap();
    }
}

#[test]
fn test_semaphore() {
    let Some(client) = client_or_skip() else {
        return;
    };
    let mut semaphore = client.semaphore("semaphore1", 10, 0, 60);
    semaphore.acquire().unwrap();
    semaphore.release().unwrap();
}

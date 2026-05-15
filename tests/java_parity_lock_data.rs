#![cfg(feature = "blocking")]

use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use ruslock::blocking::Client;
use ruslock::LockData;

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
fn test_lock_data() {
    if !slock_available() {
        eprintln!("skipping parity test because local slock is not available");
        return;
    }
    let client = Client::connect(endpoint()).unwrap();
    let mut lock1 = client.lock("lockdata1", 0, 10);
    lock1.set_count(10);
    let mut lock2 = client.lock("lockdata1", 0, 10);
    lock2.set_count(10);
    lock1.acquire_with_data(LockData::set("aaa")).unwrap();
    assert!(lock1.current_data().is_none());
    lock2.acquire_with_data(LockData::set("bbb")).unwrap();
    assert_eq!(lock2.current_data().unwrap().as_string().unwrap(), "aaa");

    let encoded = LockData::pipeline(vec![LockData::set("aaa"), LockData::push("bbb")])
        .encode()
        .unwrap();
    assert_eq!(encoded[4], ruslock::protocol::constants::LOCK_DATA_COMMAND_TYPE_PIPELINE);
}

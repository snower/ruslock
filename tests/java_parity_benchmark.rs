#![cfg(feature = "blocking")]

use std::net::{TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

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
#[ignore = "benchmark parity is ignored by default"]
fn test_benchmark() {
    if !slock_available() {
        eprintln!("skipping benchmark because local slock is not available");
        return;
    }
    let client = ruslock::blocking::Client::connect(endpoint()).unwrap();
    let start = Instant::now();
    for index in 0..1000 {
        let mut lock = client.lock(format!("benchmark{index}"), 5, 10);
        lock.acquire().unwrap();
        lock.release().unwrap();
    }
    eprintln!("benchmark parity sample completed in {:?}", start.elapsed());
}

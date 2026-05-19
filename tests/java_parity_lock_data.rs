#![cfg(feature = "blocking")]

use std::net::{TcpStream, ToSocketAddrs};
use std::thread;
use std::time::Duration;

use ruslock::blocking::Client;
use ruslock::protocol::command::LockCommand;
use ruslock::protocol::constants::COMMAND_TYPE_LOCK;
use ruslock::{Id16, Key16, LockData, PackedTime, SlockError};

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
fn test_lock_data() {
    if !slock_available() {
        eprintln!("skipping parity test because local slock is not available");
        return;
    }
    let client = Client::connect(endpoint()).unwrap();
    let mut lock1 = client.lock(unique_key("lockdata1"), 0, 10);
    lock1.set_count(10);
    let mut lock2 = client.lock(unique_key("lockdata1"), 0, 10);
    lock2.set_count(10);
    lock1.acquire_with_data(LockData::set("aaa")).unwrap();
    assert!(lock1.current_data().is_none());
    lock2.acquire_with_data(LockData::set("bbb")).unwrap();
    assert_eq!(lock2.current_data().unwrap().as_string().unwrap(), "aaa");
    lock1.release_with_data(LockData::set("ccc")).unwrap();
    assert_eq!(lock1.current_data().unwrap().as_string().unwrap(), "bbb");
    lock2.release().unwrap();
    assert_eq!(lock2.current_data().unwrap().as_string().unwrap(), "ccc");

    let mut lock1 = client.lock(unique_key("lockdata2"), 0, 10);
    lock1.set_count(10);
    let mut lock2 = client.lock(unique_key("lockdata2"), 0, 10);
    lock2.set_count(10);
    lock1.acquire_with_data(LockData::incr(1)).unwrap();
    assert!(lock1.current_data().is_none());
    lock2.acquire_with_data(LockData::incr(-3)).unwrap();
    assert_eq!(lock2.current_data().unwrap().as_i64(), 1);
    lock1.release_with_data(LockData::incr(4)).unwrap();
    assert_eq!(lock1.current_data().unwrap().as_i64(), -2);
    lock2.release().unwrap();
    assert_eq!(lock2.current_data().unwrap().as_i64(), 2);

    let mut lock1 = client.lock(unique_key("lockdata3"), 0, 10);
    lock1.set_count(10);
    let mut lock2 = client.lock(unique_key("lockdata3"), 0, 10);
    lock2.set_count(10);
    lock1.acquire_with_data(LockData::append("aaa")).unwrap();
    assert!(lock1.current_data().is_none());
    lock2.acquire_with_data(LockData::append("bbb")).unwrap();
    assert_eq!(lock2.current_data().unwrap().as_string().unwrap(), "aaa");
    lock1.release_with_data(LockData::append("ccc")).unwrap();
    assert_eq!(lock1.current_data().unwrap().as_string().unwrap(), "aaabbb");
    lock2.release().unwrap();
    assert_eq!(
        lock2.current_data().unwrap().as_string().unwrap(),
        "aaabbbccc"
    );

    let mut lock1 = client.lock(unique_key("lockdata4"), 0, 10);
    lock1.set_count(10);
    let mut lock2 = client.lock(unique_key("lockdata4"), 0, 10);
    lock2.set_count(10);
    lock1.acquire_with_data(LockData::set("aaabbbccc")).unwrap();
    assert!(lock1.current_data().is_none());
    lock2.acquire_with_data(LockData::shift(4)).unwrap();
    assert_eq!(
        lock2.current_data().unwrap().as_string().unwrap(),
        "aaabbbccc"
    );
    lock1.release_with_data(LockData::shift(2)).unwrap();
    assert_eq!(lock1.current_data().unwrap().as_string().unwrap(), "bbccc");
    lock2.release().unwrap();
    assert_eq!(lock2.current_data().unwrap().as_string().unwrap(), "ccc");

    let target_key = Key16::new(unique_key("lockdata5-target"));
    let mut lock1 = client.lock(unique_key("lockdata5"), 0, 10);
    let nested = LockCommand::new(
        COMMAND_TYPE_LOCK,
        0,
        0,
        Id16::new(),
        target_key,
        Id16::new(),
        PackedTime::new(0),
        PackedTime::new(10),
        0,
        0,
        None,
    );
    lock1
        .acquire_with_data(LockData::execute(&nested).unwrap())
        .unwrap();
    thread::sleep(Duration::from_millis(100));
    let mut lock2 = client.lock(target_key.as_bytes(), 0, 10);
    let err = lock2.acquire().unwrap_err();
    assert!(matches!(err, SlockError::LockTimeout(_)));
    lock1.release().unwrap();

    let target_key = Key16::new(unique_key("lockdata6-target"));
    let mut lock1 = client.lock(unique_key("lockdata6"), 0, 10);
    let nested = LockCommand::new(
        COMMAND_TYPE_LOCK,
        0,
        0,
        Id16::new(),
        target_key,
        Id16::new(),
        PackedTime::new(0),
        PackedTime::new(10),
        0,
        0,
        None,
    );
    lock1
        .acquire_with_data(LockData::pipeline(vec![
            LockData::set("aaa"),
            LockData::execute(&nested).unwrap(),
        ]))
        .unwrap();
    thread::sleep(Duration::from_millis(100));
    let mut lock2 = client.lock(target_key.as_bytes(), 0, 10);
    let err = lock2.acquire().unwrap_err();
    assert!(matches!(err, SlockError::LockTimeout(_)));
    lock1.release().unwrap();
    assert_eq!(lock1.current_data().unwrap().as_string().unwrap(), "aaa");

    let mut lock1 = client.lock(unique_key("lockdata7"), 0, 10);
    lock1.set_count(10);
    let mut lock2 = client.lock(unique_key("lockdata7"), 0, 10);
    lock2.set_count(10);
    lock1.acquire_with_data(LockData::push("aaa")).unwrap();
    assert!(lock1.current_data().is_none());
    lock2.acquire_with_data(LockData::push("bbb")).unwrap();
    assert_eq!(
        lock2.current_data().unwrap().as_string_list().unwrap(),
        vec!["aaa"]
    );
    lock1.release_with_data(LockData::push("ccc")).unwrap();
    assert_eq!(
        lock1.current_data().unwrap().as_string_list().unwrap(),
        vec!["aaa", "bbb"]
    );
    lock2.release().unwrap();
    assert_eq!(
        lock2.current_data().unwrap().as_string_list().unwrap(),
        vec!["aaa", "bbb", "ccc"]
    );

    let mut lock1 = client.lock(unique_key("lockdata8"), 0, 10);
    lock1.set_count(10);
    let mut lock2 = client.lock(unique_key("lockdata8"), 0, 10);
    lock2.set_count(10);
    lock1.acquire_with_data(LockData::push("aaa")).unwrap();
    assert!(lock1.current_data().is_none());
    lock1.update(Some(LockData::push("bbb"))).unwrap();
    assert!(lock1.current_data().is_some());
    lock1.update(Some(LockData::push("ccc"))).unwrap();
    assert!(lock1.current_data().is_some());
    lock2.acquire_with_data(LockData::pop(1)).unwrap();
    assert_eq!(
        lock2.current_data().unwrap().as_string_list().unwrap(),
        vec!["aaa", "bbb", "ccc"]
    );
    lock1.release_with_data(LockData::pop(4)).unwrap();
    assert_eq!(
        lock1.current_data().unwrap().as_string_list().unwrap(),
        vec!["bbb", "ccc"]
    );
    lock2.release().unwrap();
    assert_eq!(
        lock2.current_data().unwrap().as_string_list().unwrap(),
        Vec::<String>::new()
    );

    let encoded = LockData::pipeline(vec![LockData::set("aaa"), LockData::push("bbb")])
        .encode()
        .unwrap();
    assert_eq!(
        encoded[4],
        ruslock::protocol::constants::LOCK_DATA_COMMAND_TYPE_PIPELINE
    );
}

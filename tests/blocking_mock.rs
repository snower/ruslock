#![cfg(feature = "blocking")]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use ruslock::blocking::Client;
use ruslock::protocol::constants::*;
use ruslock::{ClientOptions, LockData, SlockError};

fn init_response(request: &[u8; 64]) -> [u8; 64] {
    let mut response = [0u8; 64];
    response[0] = MAGIC;
    response[1] = VERSION;
    response[2] = COMMAND_TYPE_INIT;
    response[3..19].copy_from_slice(&request[3..19]);
    response[19] = COMMAND_RESULT_SUCCED;
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

fn lock_response(request: &[u8; 64], result: u8, data: Option<&[u8]>) -> [u8; 64] {
    let mut response = [0u8; 64];
    response[0] = MAGIC;
    response[1] = VERSION;
    response[2] = request[2];
    response[3..19].copy_from_slice(&request[3..19]);
    response[19] = result;
    response[20] = if data.is_some() {
        LOCK_FLAG_CONTAINS_DATA
    } else {
        0
    };
    response[21] = request[20];
    response[22..38].copy_from_slice(&request[21..37]);
    response[38..54].copy_from_slice(&request[37..53]);
    response
}

fn start_server<F>(handler: F) -> String
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

fn start_reconnect_server() -> (String, mpsc::Receiver<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (reconnected_tx, reconnected_rx) = mpsc::channel();
    thread::spawn(move || {
        let (mut first, _) = listener.accept().unwrap();
        let mut init = [0u8; 64];
        first.read_exact(&mut init).unwrap();
        assert_eq!(init[2], COMMAND_TYPE_INIT);
        first.write_all(&init_response(&init)).unwrap();
        drop(first);

        let (mut second, _) = listener.accept().unwrap();
        let mut init = [0u8; 64];
        second.read_exact(&mut init).unwrap();
        assert_eq!(init[2], COMMAND_TYPE_INIT);
        second.write_all(&init_response(&init)).unwrap();
        reconnected_tx.send(()).unwrap();

        let mut ping = [0u8; 64];
        second.read_exact(&mut ping).unwrap();
        assert_eq!(ping[2], COMMAND_TYPE_PING);
        second.write_all(&ping_response(&ping)).unwrap();
    });
    (address, reconnected_rx)
}

fn read_extra_if_present(stream: &mut TcpStream, request: &[u8; 64]) {
    if (request[19] & LOCK_FLAG_CONTAINS_DATA) == 0 {
        return;
    }
    let mut len_bytes = [0u8; 4];
    stream.read_exact(&mut len_bytes).unwrap();
    let len = u32::from_le_bytes(len_bytes) as usize;
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).unwrap();
}

#[test]
fn open_sends_init_first_and_ping_matches_request_id() {
    let address = start_server(|mut stream| {
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).unwrap();
        assert_eq!(init[2], COMMAND_TYPE_INIT);
        stream.write_all(&init_response(&init)).unwrap();

        let mut ping = [0u8; 64];
        stream.read_exact(&mut ping).unwrap();
        assert_eq!(ping[2], COMMAND_TYPE_PING);
        stream.write_all(&ping_response(&ping)).unwrap();
    });

    let client = Client::connect(address).unwrap();
    assert!(client.ping().unwrap());
}

#[test]
fn pending_request_is_removed_after_timeout() {
    let address = start_server(|mut stream| {
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).unwrap();
        stream.write_all(&init_response(&init)).unwrap();
        let mut ping = [0u8; 64];
        stream.read_exact(&mut ping).unwrap();
        thread::sleep(Duration::from_millis(250));
    });

    let options = ClientOptions {
        command_timeout_grace: Duration::from_millis(50),
        ..ClientOptions::default()
    };
    let client = Client::with_options(address, options);
    client.open().unwrap();
    let err = client.ping().unwrap_err();
    assert!(matches!(err, SlockError::CommandTimeout));
    assert_eq!(client.pending_len(), 0);
}

#[test]
fn close_wakes_pending_request() {
    let address = start_server(|mut stream| {
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).unwrap();
        stream.write_all(&init_response(&init)).unwrap();
        let mut ping = [0u8; 64];
        stream.read_exact(&mut ping).unwrap();
        thread::sleep(Duration::from_secs(2));
    });

    let options = ClientOptions {
        command_timeout_grace: Duration::from_secs(5),
        ..ClientOptions::default()
    };
    let client = Client::with_options(address, options);
    client.open().unwrap();
    let ping_client = client.clone();
    let waiter = thread::spawn(move || ping_client.ping().unwrap_err());
    while client.pending_len() == 0 {
        thread::sleep(Duration::from_millis(5));
    }
    client.close();
    let err = waiter.join().unwrap();
    assert!(matches!(err, SlockError::ClientClosed));
}

#[test]
fn reader_thread_reconnects_after_disconnect_until_close() {
    let (address, reconnected_rx) = start_reconnect_server();
    let options = ClientOptions {
        reconnect_interval: Duration::from_millis(20),
        command_timeout_grace: Duration::from_millis(50),
        ..ClientOptions::default()
    };
    let client = Client::with_options(address, options);
    client.open().unwrap();

    reconnected_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("reader thread did not reconnect");

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match client.ping() {
            Ok(true) => break,
            Err(SlockError::Io(_))
            | Err(SlockError::NotConnected)
            | Err(SlockError::CommandTimeout)
                if Instant::now() < deadline =>
            {
                thread::sleep(Duration::from_millis(10));
            }
            other => panic!("ping did not succeed after reconnect: {other:?}"),
        }
    }

    client.close();
}

#[test]
fn blocking_lock_success_updates_current_data() {
    let address = start_server(|mut stream| {
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).unwrap();
        stream.write_all(&init_response(&init)).unwrap();

        let mut acquire = [0u8; 64];
        stream.read_exact(&mut acquire).unwrap();
        assert_eq!(acquire[2], COMMAND_TYPE_LOCK);
        read_extra_if_present(&mut stream, &acquire);
        let response_data = [LOCK_DATA_COMMAND_TYPE_SET, 0, b'a', b'a', b'a'];
        stream
            .write_all(&lock_response(
                &acquire,
                COMMAND_RESULT_SUCCED,
                Some(&response_data),
            ))
            .unwrap();
        stream
            .write_all(&(response_data.len() as u32).to_le_bytes())
            .unwrap();
        stream.write_all(&response_data).unwrap();
    });

    let client = Client::connect(address).unwrap();
    let mut lock = client.lock("mock-lock", 0, 10);
    lock.acquire_with_data(LockData::set("bbb")).unwrap();
    assert_eq!(lock.current_data().unwrap().as_string().unwrap(), "aaa");
}

#[test]
fn blocking_lock_maps_server_result_codes() {
    for (result, expected) in [
        (COMMAND_RESULT_LOCKED_ERROR, "locked"),
        (COMMAND_RESULT_UNLOCK_ERROR, "unlocked"),
        (COMMAND_RESULT_UNOWN_ERROR, "unown"),
        (COMMAND_RESULT_TIMEOUT, "timeout"),
    ] {
        let address = start_server(move |mut stream| {
            let mut init = [0u8; 64];
            stream.read_exact(&mut init).unwrap();
            stream.write_all(&init_response(&init)).unwrap();
            let mut acquire = [0u8; 64];
            stream.read_exact(&mut acquire).unwrap();
            stream
                .write_all(&lock_response(&acquire, result, None))
                .unwrap();
        });

        let client = Client::connect(address).unwrap();
        let mut lock = client.lock("mock-lock-error", 0, 10);
        let err = lock.acquire().unwrap_err();
        match expected {
            "locked" => assert!(matches!(err, SlockError::LockLocked(_))),
            "unlocked" => assert!(matches!(err, SlockError::LockUnlocked(_))),
            "unown" => assert!(matches!(err, SlockError::LockNotOwn(_))),
            "timeout" => assert!(matches!(err, SlockError::LockTimeout(_))),
            _ => unreachable!(),
        }
    }
}

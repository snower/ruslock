use std::sync::{Arc, Mutex};
use std::time::Duration;

use ruslock::callback::Client;
use ruslock::protocol::constants::*;
use ruslock::ClientOptions;

fn init_response(request: &[u8]) -> [u8; 64] {
    let mut response = [0u8; 64];
    response[0] = MAGIC;
    response[1] = VERSION;
    response[2] = COMMAND_TYPE_INIT;
    response[3..19].copy_from_slice(&request[3..19]);
    response[19] = COMMAND_RESULT_SUCCED;
    response
}

fn lock_response(request: &[u8], data: Option<&[u8]>) -> [u8; 64] {
    let mut response = [0u8; 64];
    response[0] = MAGIC;
    response[1] = VERSION;
    response[2] = request[2];
    response[3..19].copy_from_slice(&request[3..19]);
    response[19] = COMMAND_RESULT_SUCCED;
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

fn init_client(client: &Client) {
    assert!(!client.handle_init().unwrap());
    let init = client.writer_buffer().drain();
    client.reader_buffer().push(&init_response(&init));
    assert!(client.handle_init().unwrap());
}

#[test]
fn callback_reentrant_callback_can_start_new_command() {
    let client = Client::new();
    init_client(&client);
    let lock = client.lock("callback-reentrant", 1, 1);
    let nested_started = Arc::new(Mutex::new(false));
    let nested_started_clone = nested_started.clone();
    let lock_clone = lock.clone();

    lock.acquire(move |result| {
        result.unwrap();
        lock_clone
            .release(move |_| {
                *nested_started_clone.lock().unwrap() = true;
            })
            .unwrap();
    })
    .unwrap();

    let acquire = client.writer_buffer().drain();
    client.reader_buffer().push(&lock_response(&acquire, None));
    assert_eq!(client.handle_read().unwrap(), 1);
    let release = client.writer_buffer().drain();
    assert_eq!(release[2], COMMAND_TYPE_UNLOCK);
    client.reader_buffer().push(&lock_response(&release, None));
    assert_eq!(client.handle_read().unwrap(), 1);
    assert!(*nested_started.lock().unwrap());
}

#[test]
fn callback_rejects_oversized_extra_frame_without_allocating_it() {
    let options = ClientOptions {
        max_frame_size: 4,
        ..Default::default()
    };
    let client = Client::with_options(options);
    init_client(&client);
    let lock = client.lock("callback-max-frame", 1, 1);
    lock.acquire(|_| {}).unwrap();
    let request = client.writer_buffer().drain();
    client
        .reader_buffer()
        .push(&lock_response(&request, Some(b"12345")));
    client.reader_buffer().push(&5u32.to_le_bytes());
    assert!(client.handle_read().is_err());
}

#[test]
fn callback_timeout_deadline_uses_encoded_timeout_plus_grace() {
    let options = ClientOptions {
        command_timeout_grace: Duration::from_secs(2),
        ..Default::default()
    };
    let client = Client::with_options(options);
    init_client(&client);
    let lock = client.lock("callback-deadline", 3, 1);
    lock.acquire(|_| {}).unwrap();
    let deadline = client.next_deadline().unwrap();
    let early = deadline - Duration::from_millis(1);
    assert_eq!(client.handle_timeout(early).unwrap(), 0);
    assert_eq!(client.handle_timeout(deadline).unwrap(), 1);
}

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ruslock::callback::Client;
use ruslock::protocol::constants::*;
use ruslock::SlockError;

fn init_response(request: &[u8]) -> [u8; 64] {
    let mut response = [0u8; 64];
    response[0] = MAGIC;
    response[1] = VERSION;
    response[2] = COMMAND_TYPE_INIT;
    response[3..19].copy_from_slice(&request[3..19]);
    response[19] = COMMAND_RESULT_SUCCED;
    response[20] = INIT_TYPE_FLAG_IS_LEADER;
    response
}

fn ping_response(request: &[u8]) -> [u8; 64] {
    let mut response = [0u8; 64];
    response[0] = MAGIC;
    response[1] = VERSION;
    response[2] = COMMAND_TYPE_PING;
    response[3..19].copy_from_slice(&request[3..19]);
    response[19] = COMMAND_RESULT_SUCCED;
    response
}

fn init_client(client: &Client) {
    assert!(!client.handle_init().unwrap());
    let init = client.writer_buffer().drain();
    client.reader_buffer().push(&init_response(&init));
    assert!(client.handle_init().unwrap());
}

#[test]
fn callback_init_is_incremental_and_reuses_client_id_after_disconnect() {
    let client = Client::new();

    assert!(!client.handle_init().unwrap());
    let first_init = client.writer_buffer().drain();
    assert_eq!(first_init.len(), 64);
    assert!(!client.handle_init().unwrap());
    assert!(client.writer_buffer().is_empty());

    let response = init_response(&first_init);
    client.reader_buffer().push(&response[..20]);
    assert!(!client.handle_init().unwrap());
    assert_eq!(client.reader_buffer().len(), 20);
    client.reader_buffer().push(&response[20..]);
    assert!(client.handle_init().unwrap());
    assert!(client.is_inited());
    assert_eq!(client.init_type(), INIT_TYPE_FLAG_IS_LEADER);

    assert!(client.handle_disconnect().unwrap());
    assert!(!client.handle_init().unwrap());
    let second_init = client.writer_buffer().drain();
    assert_eq!(&second_init[19..35], &first_init[19..35]);
}

#[test]
fn callback_ping_resolves_by_request_id_and_cancel_ignores_late_response() {
    let client = Client::new();
    init_client(&client);

    let calls = Arc::new(Mutex::new(0));
    let cancelled_calls = calls.clone();
    let handle = client
        .ping(move |_| {
            *cancelled_calls.lock().unwrap() += 1;
        })
        .unwrap();
    let request = client.writer_buffer().drain();
    assert_eq!(client.pending_len(), 1);
    assert!(handle.cancel().unwrap());
    assert_eq!(client.pending_len(), 0);
    client.reader_buffer().push(&ping_response(&request));
    assert_eq!(client.handle_read().unwrap(), 0);
    assert_eq!(*calls.lock().unwrap(), 0);

    let result = Arc::new(Mutex::new(None));
    let result_clone = result.clone();
    client
        .ping(move |value| {
            *result_clone.lock().unwrap() = Some(value.unwrap().result);
        })
        .unwrap();
    let request = client.writer_buffer().drain();
    client.reader_buffer().push(&ping_response(&request));
    assert_eq!(client.handle_read().unwrap(), 1);
    assert_eq!(*result.lock().unwrap(), Some(COMMAND_RESULT_SUCCED));
}

#[test]
fn callback_disconnect_and_timeout_fail_pending_callbacks() {
    let client = Client::new();
    init_client(&client);

    let disconnect_error = Arc::new(Mutex::new(None));
    let disconnect_error_clone = disconnect_error.clone();
    client
        .ping(move |value| {
            *disconnect_error_clone.lock().unwrap() = Some(value.unwrap_err().to_string());
        })
        .unwrap();
    assert_eq!(client.pending_len(), 1);
    assert!(client.handle_disconnect().unwrap());
    assert_eq!(
        disconnect_error.lock().unwrap().as_deref(),
        Some("client disconnected")
    );

    init_client(&client);
    let timeout_error = Arc::new(Mutex::new(None));
    let timeout_error_clone = timeout_error.clone();
    client
        .ping(move |value| {
            *timeout_error_clone.lock().unwrap() = Some(value.unwrap_err().to_string());
        })
        .unwrap();
    assert!(client.next_deadline().unwrap() > Instant::now());
    assert_eq!(
        client
            .handle_timeout(Instant::now() + Duration::from_secs(121))
            .unwrap(),
        1
    );
    assert_eq!(
        timeout_error.lock().unwrap().as_deref(),
        Some("command timeout")
    );
}

#[test]
fn callback_unknown_response_is_protocol_error() {
    let client = Client::new();
    init_client(&client);

    let mut response = [0u8; 64];
    response[0] = MAGIC;
    response[1] = VERSION;
    response[2] = COMMAND_TYPE_PING;
    response[19] = COMMAND_RESULT_SUCCED;
    client.reader_buffer().push(&response);
    assert!(matches!(client.handle_read(), Err(SlockError::Protocol(_))));
}

use std::sync::{Arc, Mutex};

use ruslock::callback::Client;
use ruslock::protocol::constants::*;
use ruslock::{LockData, PackedTime, SlockError};

fn init_response(request: &[u8]) -> [u8; 64] {
    let mut response = [0u8; 64];
    response[0] = MAGIC;
    response[1] = VERSION;
    response[2] = COMMAND_TYPE_INIT;
    response[3..19].copy_from_slice(&request[3..19]);
    response[19] = COMMAND_RESULT_SUCCED;
    response
}

fn lock_response(request: &[u8], result: u8, data: Option<&[u8]>) -> [u8; 64] {
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

fn push_lock_response(client: &Client, request: &[u8], result: u8, data: Option<&[u8]>) {
    client
        .reader_buffer()
        .push(&lock_response(request, result, data));
    if let Some(data) = data {
        client
            .reader_buffer()
            .push(&(data.len() as u32).to_le_bytes());
        client.reader_buffer().push(data);
    }
}

fn init_client(client: &Client) {
    assert!(!client.handle_init().unwrap());
    let init = client.writer_buffer().drain();
    client.reader_buffer().push(&init_response(&init));
    assert!(client.handle_init().unwrap());
}

fn request_count(request: &[u8]) -> u16 {
    u16::from_le_bytes(request[61..63].try_into().unwrap())
}

fn request_timeout_bits(request: &[u8]) -> u32 {
    u32::from_le_bytes(request[53..57].try_into().unwrap())
}

fn request_expired_bits(request: &[u8]) -> u32 {
    u32::from_le_bytes(request[57..61].try_into().unwrap())
}

fn request_lock_id(request: &[u8]) -> [u8; 16] {
    request[21..37].try_into().unwrap()
}

#[test]
fn callback_lock_updates_current_data_before_callback_and_preserves_extra_layout() {
    let client = Client::new();
    init_client(&client);
    let lock = client.lock("callback-lock", 3, 7);
    let seen = Arc::new(Mutex::new(None));
    let seen_clone = seen.clone();
    let lock_clone = lock.clone();

    lock.acquire_with_data(LockData::set("bbb"), move |result| {
        assert_eq!(
            lock_clone.current_data().unwrap().as_string().unwrap(),
            "aaa"
        );
        *seen_clone.lock().unwrap() = Some(result.unwrap().result);
    })
    .unwrap();

    let request = client.writer_buffer().drain();
    assert_eq!(request[2], COMMAND_TYPE_LOCK);
    assert_eq!(
        request[19] & LOCK_FLAG_CONTAINS_DATA,
        LOCK_FLAG_CONTAINS_DATA
    );
    let payload = [LOCK_DATA_COMMAND_TYPE_SET, 0, b'a', b'a', b'a'];
    push_lock_response(&client, &request, COMMAND_RESULT_SUCCED, Some(&payload));
    assert_eq!(client.handle_read().unwrap(), 1);
    assert_eq!(*seen.lock().unwrap(), Some(COMMAND_RESULT_SUCCED));
    assert_eq!(lock.current_data().unwrap().raw()[0..4], [0, 0, 0, 0]);
}

#[test]
fn callback_lock_result_errors_match_blocking_mapping() {
    let client = Client::new();
    init_client(&client);
    let lock = client.lock("callback-lock-error", 3, 7);
    let error = Arc::new(Mutex::new(None));
    let error_clone = error.clone();

    lock.acquire(move |result| {
        *error_clone.lock().unwrap() = Some(matches!(result, Err(SlockError::LockLocked(_))));
    })
    .unwrap();

    let request = client.writer_buffer().drain();
    push_lock_response(&client, &request, COMMAND_RESULT_LOCKED_ERROR, None);
    assert_eq!(client.handle_read().unwrap(), 1);
    assert_eq!(*error.lock().unwrap(), Some(true));
}

#[test]
fn callback_event_and_flow_commands_match_existing_facades() {
    let client = Client::new();
    init_client(&client);
    let db = client.select_database(2);

    let event = db.event("callback-event", 3, 7, false);
    event
        .set_with_data(Some(LockData::set("payload")), |_| {})
        .unwrap();
    let set = client.writer_buffer().drain();
    assert_eq!(set[2], COMMAND_TYPE_LOCK);
    assert_eq!(
        set[19],
        LOCK_FLAG_UPDATE_WHEN_LOCKED | LOCK_FLAG_CONTAINS_DATA
    );
    assert_eq!(set[20], 2);
    assert_eq!(request_count(&set), 1);
    push_lock_response(&client, &set, COMMAND_RESULT_SUCCED, None);
    assert_eq!(client.handle_read().unwrap(), 1);

    let priority = db.priority_lock("callback-priority", 9, 5, 6);
    priority.acquire(|_| {}).unwrap();
    let request = client.writer_buffer().drain();
    assert_eq!(request[63], 9);
    assert_eq!(
        request_timeout_bits(&request),
        PackedTime::with_flags(5, TIMEOUT_FLAG_RCOUNT_IS_PRIORITY).bits()
    );
}

#[test]
fn callback_remaining_primitive_commands_match_existing_facades() {
    let client = Client::new();
    init_client(&client);
    let db = client.select_database(3);

    let reentrant = db.reentrant_lock("callback-reentrant-fields", 4, 5);
    reentrant.acquire(|_| {}).unwrap();
    let request = client.writer_buffer().drain();
    assert_eq!(request[63], 0xff);
    push_lock_response(&client, &request, COMMAND_RESULT_SUCCED, None);
    assert_eq!(client.handle_read().unwrap(), 1);

    let semaphore = db.semaphore("callback-semaphore-fields", 10, 5, 6);
    semaphore.acquire(|_| {}).unwrap();
    let request = client.writer_buffer().drain();
    assert_eq!(request_count(&request), 9);
    push_lock_response(&client, &request, COMMAND_RESULT_SUCCED, None);
    assert_eq!(client.handle_read().unwrap(), 1);

    semaphore.release(|_| {}).unwrap();
    let request = client.writer_buffer().drain();
    assert_eq!(request[2], COMMAND_TYPE_UNLOCK);
    assert_eq!(request[19], UNLOCK_FLAG_UNLOCK_FIRST_LOCK_WHEN_UNLOCKED);
    assert_eq!(request_lock_id(&request), [0; 16]);
    assert_eq!(request_count(&request), 9);
    push_lock_response(&client, &request, COMMAND_RESULT_SUCCED, None);
    assert_eq!(client.handle_read().unwrap(), 1);

    let read_write = db.read_write_lock("callback-rw-fields", 7, 8);
    read_write.acquire_write(|_| {}).unwrap();
    let request = client.writer_buffer().drain();
    assert_eq!(request_count(&request), 0);
    push_lock_response(&client, &request, COMMAND_RESULT_SUCCED, None);
    assert_eq!(client.handle_read().unwrap(), 1);

    read_write.acquire_read(|_| {}).unwrap();
    let request = client.writer_buffer().drain();
    assert_eq!(request_count(&request), u16::MAX);
    push_lock_response(&client, &request, COMMAND_RESULT_SUCCED, None);
    assert_eq!(client.handle_read().unwrap(), 1);

    read_write.release_read(|_| {}).unwrap();
    let request = client.writer_buffer().drain();
    assert_eq!(request[2], COMMAND_TYPE_UNLOCK);
    assert_eq!(request_count(&request), u16::MAX);
    push_lock_response(&client, &request, COMMAND_RESULT_SUCCED, None);
    assert_eq!(client.handle_read().unwrap(), 1);

    let max_flow = db.max_concurrent_flow("callback-max-flow-fields", 5, 6, 7);
    max_flow.acquire(|_| {}).unwrap();
    let request = client.writer_buffer().drain();
    assert_eq!(request_count(&request), 4);
    push_lock_response(&client, &request, COMMAND_RESULT_SUCCED, None);
    assert_eq!(client.handle_read().unwrap(), 1);

    let token_flow = db.token_bucket_flow("callback-token-flow-fields", 3, 2, 0.25);
    token_flow.acquire(|_| {}).unwrap();
    let request = client.writer_buffer().drain();
    assert_eq!(request_count(&request), 2);
    assert_eq!(
        request_expired_bits(&request),
        PackedTime::with_flags(250, EXPRIED_FLAG_MILLISECOND_TIME).bits()
    );
}

#[test]
fn callback_tree_leaf_child_acquire_uses_continuation_and_single_user_callback() {
    let client = Client::new();
    init_client(&client);
    let root = client.tree_lock("callback-tree-root", 5, 6);
    let child = root.load_child("callback-tree-child");
    let leaf = child.new_leaf_lock();
    let calls = Arc::new(Mutex::new(0));
    let calls_clone = calls.clone();

    leaf.acquire(move |result| {
        result.unwrap();
        *calls_clone.lock().unwrap() += 1;
    })
    .unwrap();

    let child_check = client.writer_buffer().drain();
    assert_eq!(child_check[19], LOCK_FLAG_LOCK_TREE_LOCK);
    push_lock_response(&client, &child_check, COMMAND_RESULT_LOCKED_ERROR, None);
    assert_eq!(client.handle_read().unwrap(), 1);
    assert_eq!(*calls.lock().unwrap(), 0);

    let parent_check = client.writer_buffer().drain();
    assert_eq!(parent_check[19], 0);
    push_lock_response(&client, &parent_check, COMMAND_RESULT_SUCCED, None);
    assert_eq!(client.handle_read().unwrap(), 1);
    assert_eq!(*calls.lock().unwrap(), 0);

    let leaf_acquire = client.writer_buffer().drain();
    assert_eq!(leaf_acquire[19], 0);
    push_lock_response(&client, &leaf_acquire, COMMAND_RESULT_SUCCED, None);
    assert_eq!(client.handle_read().unwrap(), 1);
    assert_eq!(*calls.lock().unwrap(), 1);
}

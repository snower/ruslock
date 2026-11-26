#![cfg(feature = "aio")]

//! Wire-level parity tests for the typed binding primitives.
//!
//! Each test drives one or more `Binding*` objects against an in-process mock
//! Slock server and inspects the exact command frames the Rust primitive sends.
//! This is the only way to prove that binding objects preserve the Java facade
//! semantics (`count - 1`, priority flags, FIFO read locks, packed flags) rather
//! than merely returning successful futures.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use ruslock::aio::Client;
use ruslock::binding::{
    BindingGroupEvent, BindingMaxConcurrentFlow, BindingPriorityLock, BindingReadWriteLock,
    BindingReentrantLock, BindingSemaphore, BindingTokenBucketFlow, BindingTreeLock,
};
use ruslock::key::Key16;
use ruslock::protocol::constants::*;
use ruslock::time::PackedTime;
use ruslock::LockData;

type Captured = Arc<Mutex<Vec<[u8; 64]>>>;

fn init_response(request: &[u8; 64]) -> [u8; 64] {
    let mut response = [0u8; 64];
    response[0] = MAGIC;
    response[1] = VERSION;
    response[2] = COMMAND_TYPE_INIT;
    response[3..19].copy_from_slice(&request[3..19]);
    response[19] = COMMAND_RESULT_SUCCED;
    response
}

fn lock_response(request: &[u8; 64], result: u8) -> [u8; 64] {
    let mut response = [0u8; 64];
    response[0] = MAGIC;
    response[1] = VERSION;
    response[2] = request[2];
    response[3..19].copy_from_slice(&request[3..19]);
    response[19] = result;
    // Response flag is independent of the request flag; tests that return
    // payload data set the contains-data bit explicitly.
    response[20] = 0;
    response[21] = request[20];
    response[22..38].copy_from_slice(&request[21..37]);
    response[38..54].copy_from_slice(&request[37..53]);
    response
}

fn u16_at(header: &[u8; 64], offset: usize) -> u16 {
    u16::from_le_bytes(header[offset..offset + 2].try_into().unwrap())
}

fn u32_at(header: &[u8; 64], offset: usize) -> u32 {
    u32::from_le_bytes(header[offset..offset + 4].try_into().unwrap())
}

fn timeout_bits(header: &[u8; 64]) -> u32 {
    u32_at(header, 53)
}

fn expired_bits(header: &[u8; 64]) -> u32 {
    u32_at(header, 57)
}

fn count(header: &[u8; 64]) -> u16 {
    u16_at(header, 61)
}

fn r_count(header: &[u8; 64]) -> u8 {
    header[63]
}

fn lock_id(header: &[u8; 64]) -> [u8; 16] {
    header[21..37].try_into().unwrap()
}

fn lock_key(header: &[u8; 64]) -> [u8; 16] {
    header[37..53].try_into().unwrap()
}

/// Reads the optional request payload (`4-byte length + payload`) that the
/// client writes after a contains-data command header.
async fn read_request_extra(stream: &mut tokio::net::TcpStream, header: &[u8; 64]) -> Vec<u8> {
    if header[19] & LOCK_FLAG_CONTAINS_DATA == 0 {
        return Vec::new();
    }
    let mut len_bytes = [0u8; 4];
    stream.read_exact(&mut len_bytes).await.unwrap();
    let len = u32::from_le_bytes(len_bytes) as usize;
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).await.unwrap();
    let mut raw = len_bytes.to_vec();
    raw.extend_from_slice(&body);
    raw
}

fn expected_packed(value: u16, flags: u16) -> u32 {
    PackedTime::with_flags(value, flags).bits()
}

/// Starts a mock server that answers every command in order with the supplied
/// result code (success once the list is exhausted) and captures each frame.
fn spawn_mock<F>(results: Vec<u8>, customize: F) -> (String, Captured)
where
    F: Fn(usize, &[u8; 64], &mut [u8; 64]) + Send + 'static,
{
    let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    std_listener.set_nonblocking(true).unwrap();
    let address = std_listener.local_addr().unwrap().to_string();
    let captured: Captured = Arc::new(Mutex::new(Vec::new()));
    let capture = Arc::clone(&captured);

    tokio::spawn(async move {
        let listener = TcpListener::from_std(std_listener).unwrap();
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut init = [0u8; 64];
        if stream.read_exact(&mut init).await.is_err() {
            return;
        }
        let _ = stream.write_all(&init_response(&init)).await;

        let mut index = 0usize;
        loop {
            let mut command = [0u8; 64];
            if stream.read_exact(&mut command).await.is_err() {
                break;
            }
            capture.lock().unwrap().push(command);

            let result = results.get(index).copied().unwrap_or(COMMAND_RESULT_SUCCED);
            let mut response = lock_response(&command, result);
            customize(index, &command, &mut response);
            if stream.write_all(&response).await.is_err() {
                break;
            }
            index += 1;
        }
    });

    (address, captured)
}

fn last_command(captured: &Captured) -> [u8; 64] {
    *captured.lock().unwrap().last().unwrap()
}

fn commands(captured: &Captured) -> Vec<[u8; 64]> {
    captured.lock().unwrap().clone()
}

#[tokio::test]
async fn semaphore_reentrant_and_readwrite_wire_fields() {
    let (address, captured) = spawn_mock(Vec::new(), |_, _, _| {});
    let client = Client::connect(address).await.unwrap();
    let database = client.select_database(3);

    // Semaphore: raw count 10 -> wire count 9, r_count 0.
    let semaphore = BindingSemaphore::new(
        database.clone(),
        "binding-sem",
        10,
        PackedTime::new(5),
        PackedTime::new(6),
    );
    semaphore.acquire().await.unwrap();
    let command = last_command(&captured);
    assert_eq!(command[2], COMMAND_TYPE_LOCK);
    assert_eq!(command[19], 0);
    assert_eq!(command[20], 3);
    assert_eq!(count(&command), 9);
    assert_eq!(r_count(&command), 0);
    assert_eq!(timeout_bits(&command), 5);
    assert_eq!(expired_bits(&command), 6);

    // Semaphore release uses the unlock-first path and an all-zero lock id.
    semaphore.release().await.unwrap();
    let command = last_command(&captured);
    assert_eq!(command[2], COMMAND_TYPE_UNLOCK);
    assert_eq!(command[19], UNLOCK_FLAG_UNLOCK_FIRST_LOCK_WHEN_UNLOCKED);
    assert_eq!(lock_id(&command), [0u8; 16]);
    assert_eq!(count(&command), 9);

    // Reentrant lock reuses one lock id and sets r_count = 0xff.
    let reentrant = BindingReentrantLock::new(
        database.clone(),
        "binding-reentrant",
        PackedTime::new(2),
        PackedTime::new(3),
    );
    reentrant.acquire().await.unwrap();
    reentrant.acquire().await.unwrap();
    let frames = commands(&captured);
    let first = &frames[frames.len() - 2];
    let second = &frames[frames.len() - 1];
    assert_eq!(lock_id(first), lock_id(second));
    assert_eq!(r_count(second), 0xff);
    assert_eq!(count(second), 0);
    assert_eq!(timeout_bits(second), 2);
    assert_eq!(expired_bits(second), 3);

    // Read/write lock: write count 0, read count 0xffff, FIFO release.
    let read_write = BindingReadWriteLock::new(
        database.clone(),
        "binding-readwrite",
        PackedTime::new(1),
        PackedTime::new(2),
    );
    read_write.acquire_write().await.unwrap();
    let command = last_command(&captured);
    assert_eq!(count(&command), 0);
    assert_eq!(r_count(&command), 0);
    assert_eq!(lock_id(&command), *read_write.write_lock_id().as_bytes());

    read_write.acquire_read().await.unwrap();
    let first_read = last_command(&captured);
    assert_eq!(count(&first_read), u16::MAX);
    assert_eq!(r_count(&first_read), 0);
    read_write.acquire_read().await.unwrap();
    let second_read = last_command(&captured);
    assert_ne!(lock_id(&first_read), lock_id(&second_read));
    assert_eq!(read_write.read_lock_count(), 2);

    read_write.release_read().await.unwrap();
    let command = last_command(&captured);
    assert_eq!(command[2], COMMAND_TYPE_UNLOCK);
    assert_eq!(
        lock_id(&command),
        lock_id(&first_read),
        "release_read must release the oldest read lock (FIFO)"
    );
    assert_eq!(count(&command), u16::MAX);
    assert_eq!(read_write.read_lock_count(), 1);

    // Empty read release is a no-op, matching Java `NoSuchElementException`.
    read_write.release_read().await.unwrap();
    let command = last_command(&captured);
    assert_eq!(
        lock_id(&command),
        lock_id(&second_read),
        "second release must release the remaining read lock"
    );
    assert_eq!(read_write.read_lock_count(), 0);
    read_write.release_read().await.unwrap();

    client.close().await;
}

#[tokio::test]
async fn group_event_preserves_packed_flags_and_updates_version() {
    // Wakeup returns version 42 in the first 8 lock-id bytes.
    let (address, captured) = spawn_mock(Vec::new(), |index, _command, response| {
        if index == 2 {
            let mut id = [0u8; 16];
            id[..8].copy_from_slice(&42u64.to_le_bytes());
            response[22..38].copy_from_slice(&id);
        }
    });
    let client = Client::connect(address).await.unwrap();
    let database = client.select_database(0);
    database.set_default_timeout_flags(TIMEOUT_FLAG_RCOUNT_IS_PRIORITY);
    database.set_default_expired_flags(EXPRIED_FLAG_MILLISECOND_TIME);

    let caller_timeout = PackedTime::with_flags(7, TIMEOUT_FLAG_MINUTE_TIME);
    let caller_expired = PackedTime::with_flags(9, EXPRIED_FLAG_MINUTE_TIME);
    let base_timeout = expected_packed(
        7,
        TIMEOUT_FLAG_MINUTE_TIME | TIMEOUT_FLAG_RCOUNT_IS_PRIORITY,
    );
    let base_expired = expected_packed(9, EXPRIED_FLAG_MINUTE_TIME | EXPRIED_FLAG_MILLISECOND_TIME);

    let event = BindingGroupEvent::new(
        database.clone(),
        "binding-group",
        7,
        1,
        caller_timeout,
        caller_expired,
    );

    event.set().await.unwrap();
    let command = last_command(&captured);
    assert_eq!(command[2], COMMAND_TYPE_UNLOCK);
    assert_eq!(command[19], UNLOCK_FLAG_UNLOCK_FIRST_LOCK_WHEN_UNLOCKED);
    assert_eq!(timeout_bits(&command), base_timeout);
    assert_eq!(expired_bits(&command), base_expired);

    // clear() updates the event with the version flag and the current version.
    event.clear().await.unwrap();
    let command = last_command(&captured);
    assert_eq!(command[2], COMMAND_TYPE_LOCK);
    assert_eq!(command[19], LOCK_FLAG_UPDATE_WHEN_LOCKED);
    assert_eq!(
        timeout_bits(&command),
        base_timeout | expected_packed(0, TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED)
    );
    assert_eq!(expired_bits(&command), base_expired);
    let mut clear_id = [0u8; 16];
    clear_id[..8].copy_from_slice(&1u64.to_le_bytes());
    assert_eq!(lock_id(&command), clear_id);

    event.wakeup().await.unwrap();
    let command = last_command(&captured);
    assert_eq!(command[2], COMMAND_TYPE_UNLOCK);
    assert_eq!(
        command[19],
        UNLOCK_FLAG_UNLOCK_FIRST_LOCK_WHEN_UNLOCKED | UNLOCK_FLAG_SUCCED_TO_LOCK_WAIT
    );
    assert_eq!(
        timeout_bits(&command),
        base_timeout | expected_packed(0, TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED)
    );
    assert_eq!(expired_bits(&command), base_expired);

    assert_eq!(
        event.version_id(),
        42,
        "wakeup must adopt the returned version"
    );

    // wait() uses the raw caller timeout plus the version flag; expired is 0
    // and database default flags are *not* merged into this probe.
    event.wait(5).await.unwrap();
    let command = last_command(&captured);
    assert_eq!(
        timeout_bits(&command),
        expected_packed(5, TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED)
    );
    assert_eq!(expired_bits(&command), 0);
    assert_eq!(count(&command), 0);
    assert_eq!(r_count(&command), 0);
    let mut expected_id = [0u8; 16];
    expected_id[..8].copy_from_slice(&42u64.to_le_bytes());
    expected_id[8..].copy_from_slice(&7u64.to_le_bytes());
    assert_eq!(lock_id(&command), expected_id);

    // isSet() probes with literal zero timeout/expired.
    event.is_set().await.unwrap();
    let command = last_command(&captured);
    assert_eq!(timeout_bits(&command), 0);
    assert_eq!(expired_bits(&command), 0);

    client.close().await;
}

#[tokio::test]
async fn group_event_set_and_clear_with_data_and_current_data() {
    let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    std_listener.set_nonblocking(true).unwrap();
    let address = std_listener.local_addr().unwrap().to_string();
    let captured = Arc::new(Mutex::new(Vec::<([u8; 64], Vec<u8>)>::new()));
    let capture = Arc::clone(&captured);

    tokio::spawn(async move {
        let listener = TcpListener::from_std(std_listener).unwrap();
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).await.unwrap();
        stream.write_all(&init_response(&init)).await.unwrap();

        // set_with_data
        let mut set = [0u8; 64];
        stream.read_exact(&mut set).await.unwrap();
        let set_extra = read_request_extra(&mut stream, &set).await;
        capture.lock().unwrap().push((set, set_extra));
        stream
            .write_all(&lock_response(&set, COMMAND_RESULT_SUCCED))
            .await
            .unwrap();

        // clear_with_data
        let mut clear = [0u8; 64];
        stream.read_exact(&mut clear).await.unwrap();
        let clear_extra = read_request_extra(&mut stream, &clear).await;
        capture.lock().unwrap().push((clear, clear_extra));
        stream
            .write_all(&lock_response(&clear, COMMAND_RESULT_SUCCED))
            .await
            .unwrap();

        // wait: answer with a result payload of "server".
        let mut wait = [0u8; 64];
        stream.read_exact(&mut wait).await.unwrap();
        capture.lock().unwrap().push((wait, Vec::new()));
        let mut response = lock_response(&wait, COMMAND_RESULT_SUCCED);
        response[20] = LOCK_FLAG_CONTAINS_DATA;
        stream.write_all(&response).await.unwrap();
        let value = b"server";
        let mut payload = Vec::new();
        payload.extend_from_slice(&((value.len() + 2) as u32).to_le_bytes());
        payload.push(0); // stage current, command type set
        payload.push(0); // no data flags
        payload.extend_from_slice(value);
        stream.write_all(&payload).await.unwrap();
    });

    let client = Client::connect(address).await.unwrap();
    let database = client.select_database(0);
    let event = BindingGroupEvent::new(
        database,
        "binding-group-data",
        1,
        0,
        PackedTime::new(5),
        PackedTime::new(10),
    );

    event.set_with_data(LockData::set("hello")).await.unwrap();
    assert!(
        event.current_data().is_none(),
        "set_with_data must not update the facade current data"
    );

    event.clear_with_data(LockData::set("bye")).await.unwrap();
    assert!(
        event.current_data().is_none(),
        "clear_with_data must not update the facade current data"
    );

    event.wait(5).await.unwrap();
    let data = event.current_data().expect("wait must expose result data");
    assert_eq!(data.as_string().unwrap(), "server");

    let frames = captured.lock().unwrap();
    let (set_header, set_payload) = &frames[0];
    assert_eq!(set_header[2], COMMAND_TYPE_UNLOCK);
    assert_eq!(
        set_header[19],
        UNLOCK_FLAG_UNLOCK_FIRST_LOCK_WHEN_UNLOCKED | UNLOCK_FLAG_CONTAINS_DATA
    );
    assert_eq!(
        set_payload,
        &vec![7, 0, 0, 0, 0, 0, b'h', b'e', b'l', b'l', b'o']
    );

    let (clear_header, clear_payload) = &frames[1];
    assert_eq!(clear_header[2], COMMAND_TYPE_LOCK);
    assert_eq!(
        clear_header[19],
        LOCK_FLAG_UPDATE_WHEN_LOCKED | LOCK_FLAG_CONTAINS_DATA
    );
    assert_eq!(clear_payload, &vec![5, 0, 0, 0, 0, 0, b'b', b'y', b'e']);
    assert_eq!(
        timeout_bits(clear_header),
        expected_packed(5, TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED)
    );

    let (wait_header, wait_payload) = &frames[2];
    assert_eq!(wait_header[2], COMMAND_TYPE_LOCK);
    assert!(wait_payload.is_empty());

    client.close().await;
}

#[tokio::test]
async fn group_event_wait_does_not_block_wakeup_on_same_handle() {
    let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    std_listener.set_nonblocking(true).unwrap();
    let address = std_listener.local_addr().unwrap().to_string();
    let (wait_seen_tx, wait_seen_rx) = oneshot::channel();

    tokio::spawn(async move {
        let listener = TcpListener::from_std(std_listener).unwrap();
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).await.unwrap();
        stream.write_all(&init_response(&init)).await.unwrap();

        // The wait command must reach the wire before wakeup is issued.
        let mut wait = [0u8; 64];
        stream.read_exact(&mut wait).await.unwrap();
        assert_eq!(wait[2], COMMAND_TYPE_LOCK);
        wait_seen_tx.send(()).unwrap();

        // While the wait is unanswered, wakeup on the same handle must proceed.
        let mut wakeup = [0u8; 64];
        stream.read_exact(&mut wakeup).await.unwrap();
        assert_eq!(wakeup[2], COMMAND_TYPE_UNLOCK);
        stream
            .write_all(&lock_response(&wakeup, COMMAND_RESULT_SUCCED))
            .await
            .unwrap();
        stream
            .write_all(&lock_response(&wait, COMMAND_RESULT_SUCCED))
            .await
            .unwrap();
    });

    let client = Client::connect(address).await.unwrap();
    let database = client.select_database(0);
    let event = BindingGroupEvent::new(
        database,
        "binding-group-concurrent",
        1,
        0,
        PackedTime::new(5),
        PackedTime::new(10),
    );

    let wait_task = tokio::spawn({
        let event = event.clone();
        async move { event.wait(5).await }
    });
    tokio::time::timeout(Duration::from_secs(2), wait_seen_rx)
        .await
        .expect("wait command was not sent within the bound")
        .unwrap();

    tokio::time::timeout(Duration::from_secs(2), event.wakeup())
        .await
        .expect("wakeup was blocked behind the in-flight wait")
        .unwrap();

    tokio::time::timeout(Duration::from_secs(2), wait_task)
        .await
        .expect("wait did not complete within the bound")
        .unwrap()
        .unwrap();

    client.close().await;
}

#[tokio::test]
async fn priority_and_max_flow_wire_fields() {
    let (address, captured) = spawn_mock(Vec::new(), |_, _, _| {});
    let client = Client::connect(address).await.unwrap();
    let database = client.select_database(0);

    let priority = BindingPriorityLock::new(
        database.clone(),
        "binding-priority",
        9,
        5,
        PackedTime::with_flags(5, TIMEOUT_FLAG_MINUTE_TIME),
        PackedTime::with_flags(6, EXPRIED_FLAG_MINUTE_TIME),
    );
    priority.acquire().await.unwrap();
    let command = last_command(&captured);
    assert_eq!(r_count(&command), 9);
    assert_eq!(
        count(&command),
        4,
        "priority count 5 must become 4 on the wire"
    );
    assert_eq!(
        timeout_bits(&command),
        expected_packed(
            5,
            TIMEOUT_FLAG_MINUTE_TIME | TIMEOUT_FLAG_RCOUNT_IS_PRIORITY
        )
    );
    assert_eq!(
        expired_bits(&command),
        expected_packed(6, EXPRIED_FLAG_MINUTE_TIME)
    );

    // Max flow without priority must not set the priority timeout flag.
    let flow = BindingMaxConcurrentFlow::new(
        database.clone(),
        "binding-max-no-priority",
        4,
        0,
        PackedTime::new(5),
        PackedTime::new(6),
    );
    flow.acquire().await.unwrap();
    let command = last_command(&captured);
    assert_eq!(timeout_bits(&command), 5);
    assert_eq!(count(&command), 3);
    assert_eq!(r_count(&command), 0);

    // Max flow with priority sets the flag and stores the priority in r_count.
    let flow = BindingMaxConcurrentFlow::new(
        database.clone(),
        "binding-max-priority",
        4,
        5,
        PackedTime::new(5),
        PackedTime::new(6),
    );
    flow.acquire().await.unwrap();
    let command = last_command(&captured);
    assert_eq!(
        timeout_bits(&command),
        expected_packed(5, TIMEOUT_FLAG_RCOUNT_IS_PRIORITY)
    );
    assert_eq!(count(&command), 3);
    assert_eq!(r_count(&command), 5);

    client.close().await;
}

#[tokio::test]
async fn token_bucket_period_and_timeout_retry() {
    // Command 0: short period success. Command 1: long period times out.
    // Command 2: long period retry succeeds.
    let (address, captured) = spawn_mock(
        vec![
            COMMAND_RESULT_SUCCED,
            COMMAND_RESULT_TIMEOUT,
            COMMAND_RESULT_SUCCED,
        ],
        |_, _, _| {},
    );
    let client = Client::connect(address).await.unwrap();
    let database = client.select_database(0);

    // period < 3: expired is ceil(period * 1000) with the millisecond flag;
    // timeout carries the priority flag only when priority > 0.
    let short = BindingTokenBucketFlow::new(
        database.clone(),
        "binding-bucket-short",
        10,
        PackedTime::new(5),
        1.5,
        3,
    );
    short.acquire().await.unwrap();
    let command = last_command(&captured);
    assert_eq!(count(&command), 9);
    assert_eq!(r_count(&command), 0);
    assert_eq!(
        timeout_bits(&command),
        expected_packed(5, TIMEOUT_FLAG_RCOUNT_IS_PRIORITY)
    );
    assert_eq!(
        expired_bits(&command),
        expected_packed(1500, EXPRIED_FLAG_MILLISECOND_TIME)
    );

    // period >= 3: first attempt uses timeout 0 and the remaining time to the
    // next period boundary; the retry uses the configured timeout and one full
    // period as expired.
    let long = BindingTokenBucketFlow::new(
        database.clone(),
        "binding-bucket-long",
        3,
        PackedTime::new(5),
        4.0,
        2,
    );
    long.acquire().await.unwrap();
    let frames = commands(&captured);
    let first = &frames[1];
    let retry = &frames[2];

    assert_eq!(timeout_bits(first), 0);
    assert_eq!(expired_bits(first) >> 16, 0);
    let remaining = expired_bits(first) & 0xffff;
    assert!(
        (1..=4).contains(&remaining),
        "remaining seconds must fall inside one period, got {remaining}"
    );
    assert_eq!(count(first), 2);
    assert_eq!(r_count(first), 0);

    assert_eq!(
        timeout_bits(retry),
        expected_packed(5, TIMEOUT_FLAG_RCOUNT_IS_PRIORITY)
    );
    assert_eq!(expired_bits(retry), 4);
    assert_eq!(count(retry), 2);
    assert_eq!(r_count(retry), 0);

    client.close().await;
}

#[tokio::test]
async fn semaphore_set_count_after_first_acquire_uses_exact_encoded_count() {
    let (address, captured) = spawn_mock(Vec::new(), |_, _, _| {});
    let client = Client::connect(address).await.unwrap();
    let database = client.select_database(0);
    database.set_default_timeout_flags(0);
    database.set_default_expired_flags(0);

    let semaphore = BindingSemaphore::new(
        database,
        "binding-sem-setcount",
        10,
        PackedTime::new(5),
        PackedTime::new(6),
    );
    // Constructor raw count 10 -> encoded 9, exposed like Java getCount().
    assert_eq!(semaphore.count(), 9);

    semaphore.acquire().await.unwrap();
    assert_eq!(count(&last_command(&captured)), 9);

    // setCount stores the command count verbatim: the next fresh Lock must send
    // 3, not 2 (no second decrement).
    semaphore.set_count(3);
    semaphore.acquire().await.unwrap();
    let command = last_command(&captured);
    assert_eq!(count(&command), 3);
    assert_eq!(r_count(&command), 0);
    assert_eq!(timeout_bits(&command), 5);
    assert_eq!(expired_bits(&command), 6);

    // A release also uses the updated encoded count.
    semaphore.release().await.unwrap();
    let command = last_command(&captured);
    assert_eq!(command[2], COMMAND_TYPE_UNLOCK);
    assert_eq!(command[19], UNLOCK_FLAG_UNLOCK_FIRST_LOCK_WHEN_UNLOCKED);
    assert_eq!(lock_id(&command), [0u8; 16]);
    assert_eq!(count(&command), 3);

    // 0xffff is a legal encoded short count; it must survive without an
    // off-by-one wrap.
    semaphore.set_count(u16::MAX);
    semaphore.acquire().await.unwrap();
    assert_eq!(count(&last_command(&captured)), u16::MAX);

    client.close().await;
}

#[tokio::test]
async fn token_bucket_set_count_after_first_acquire_uses_exact_encoded_count() {
    // 0: constructor acquire (short period). 1: acquire after set_count.
    // 2: long-period first attempt (timeout). 3: long-period retry.
    let (address, captured) = spawn_mock(
        vec![
            COMMAND_RESULT_SUCCED,
            COMMAND_RESULT_SUCCED,
            COMMAND_RESULT_TIMEOUT,
            COMMAND_RESULT_SUCCED,
        ],
        |_, _, _| {},
    );
    let client = Client::connect(address).await.unwrap();
    let database = client.select_database(0);
    database.set_default_timeout_flags(0);

    let short = BindingTokenBucketFlow::new(
        database.clone(),
        "binding-bucket-setcount",
        10,
        PackedTime::new(5),
        1.5,
        0,
    );
    // Constructor raw count 10 -> encoded 9, exposed like Java getCount().
    assert_eq!(short.count(), 9);
    short.acquire().await.unwrap();
    assert_eq!(count(&last_command(&captured)), 9);

    // setCount is encoded: the next fresh acquire Lock sends 3, not 2.
    short.set_count(3);
    short.acquire().await.unwrap();
    assert_eq!(count(&last_command(&captured)), 3);

    // The long-period retry path also uses the encoded count verbatim.
    let long = BindingTokenBucketFlow::new(
        database,
        "binding-bucket-setcount-long",
        3,
        PackedTime::new(5),
        4.0,
        0,
    );
    long.set_count(7);
    long.acquire().await.unwrap();
    let frames = commands(&captured);
    let first = &frames[frames.len() - 2];
    let retry = &frames[frames.len() - 1];
    assert_eq!(count(first), 7);
    assert_eq!(count(retry), 7);

    client.close().await;
}

#[tokio::test]
async fn tree_lock_check_leaf_and_child_command_order() {
    let (address, captured) = spawn_mock(Vec::new(), |_, _, _| {});
    let client = Client::connect(address).await.unwrap();
    let database = client.select_database(0);

    let root = BindingTreeLock::new(
        database.clone(),
        "binding-tree-root",
        PackedTime::new(5),
        PackedTime::new(10),
    );
    assert!(root.is_root());
    let root_key = *root.lock_key().as_bytes();

    root.acquire().await.unwrap();
    let frames = commands(&captured);
    assert_eq!(frames.len(), 2);
    let check = &frames[0];
    assert_eq!(check[2], COMMAND_TYPE_LOCK);
    assert_eq!(check[19], 0);
    assert_eq!(lock_key(check), root_key);
    assert_eq!(count(check), 0);
    assert_eq!(r_count(check), 0);
    assert_eq!(timeout_bits(check), 5);
    assert_eq!(expired_bits(check), 0);

    let leaf = &frames[1];
    assert_eq!(leaf[2], COMMAND_TYPE_LOCK);
    assert_eq!(leaf[19], 0);
    assert_eq!(lock_key(leaf), root_key);
    assert_eq!(count(leaf), u16::MAX);
    assert_eq!(r_count(leaf), 1);
    assert_eq!(
        timeout_bits(leaf),
        expected_packed(5, TIMEOUT_FLAG_RCOUNT_IS_PRIORITY)
    );
    assert_eq!(expired_bits(leaf), 10);

    let leaf_id = root.current_leaf_id().unwrap();

    // A repeat acquire only re-runs the check and reuses the cached leaf.
    root.acquire().await.unwrap();
    let frames = commands(&captured);
    assert_eq!(frames.len(), 3);
    assert_eq!(frames[2][19], 0);
    assert_eq!(count(&frames[2]), 0);

    root.release().await.unwrap();
    let command = last_command(&captured);
    assert_eq!(command[2], COMMAND_TYPE_UNLOCK);
    assert_eq!(command[19], UNLOCK_FLAG_UNLOCK_TREE_LOCK);
    assert_eq!(lock_id(&command), *leaf_id.as_bytes());
    assert_eq!(count(&command), u16::MAX);
    assert_eq!(r_count(&command), 1);
    assert_eq!(root.current_leaf_id(), None);

    // wait() is a raw probe: caller timeout, zero expired, count 0.
    root.wait(7).await.unwrap();
    let command = last_command(&captured);
    assert_eq!(command[2], COMMAND_TYPE_LOCK);
    assert_eq!(count(&command), 0);
    assert_eq!(r_count(&command), 0);
    assert_eq!(timeout_bits(&command), 7);
    assert_eq!(expired_bits(&command), 0);

    // Child acquire: check, child-check (tree flag), parent-check, leaf.
    let parent_key = Key16::new("binding-tree-root");
    let child = BindingTreeLock::with_parent(
        database.clone(),
        Some(parent_key),
        "binding-tree-child",
        PackedTime::new(5),
        PackedTime::new(10),
    );
    assert!(!child.is_root());
    let child_key = *child.lock_key().as_bytes();
    let before = commands(&captured).len();
    child.acquire().await.unwrap();
    let frames = commands(&captured);
    let frames = &frames[before..];
    assert_eq!(frames.len(), 4, "child acquire must issue four commands");

    // Root check.
    assert_eq!(frames[0][19], 0);
    assert_eq!(lock_key(&frames[0]), child_key);
    assert_eq!(count(&frames[0]), 0);

    // Child check: child key + parent key as lock id, tree-lock flag.
    assert_eq!(frames[1][19], LOCK_FLAG_LOCK_TREE_LOCK);
    assert_eq!(lock_key(&frames[1]), child_key);
    assert_eq!(lock_id(&frames[1]), *parent_key.as_bytes());
    assert_eq!(count(&frames[1]), u16::MAX);
    assert_eq!(r_count(&frames[1]), 1);
    assert_eq!(
        timeout_bits(&frames[1]),
        expected_packed(0, TIMEOUT_FLAG_RCOUNT_IS_PRIORITY)
    );
    assert_eq!(expired_bits(&frames[1]), 10);

    // Parent check: parent key + child key as lock id.
    assert_eq!(frames[2][19], 0);
    assert_eq!(lock_key(&frames[2]), *parent_key.as_bytes());
    assert_eq!(lock_id(&frames[2]), child_key);
    assert_eq!(count(&frames[2]), u16::MAX);
    assert_eq!(r_count(&frames[2]), 1);
    assert_eq!(
        timeout_bits(&frames[2]),
        expected_packed(0, TIMEOUT_FLAG_RCOUNT_IS_PRIORITY)
    );

    // Leaf acquire under the child key.
    assert_eq!(frames[3][19], 0);
    assert_eq!(lock_key(&frames[3]), child_key);
    assert_eq!(count(&frames[3]), u16::MAX);
    assert_eq!(r_count(&frames[3]), 1);
    assert_eq!(
        timeout_bits(&frames[3]),
        expected_packed(5, TIMEOUT_FLAG_RCOUNT_IS_PRIORITY)
    );
    assert_eq!(expired_bits(&frames[3]), 10);

    client.close().await;
}

#[tokio::test]
async fn tree_leaf_keeps_construction_timing_after_tree_set_timing() {
    let (address, captured) = spawn_mock(Vec::new(), |_, _, _| {});
    let client = Client::connect(address).await.unwrap();
    let database = client.select_database(0);
    database.set_default_timeout_flags(0);
    database.set_default_expired_flags(0);

    let base_timeout = PackedTime::new(5);
    let base_expired = PackedTime::new(10);
    let root = BindingTreeLock::new(database, "binding-tree-timing", base_timeout, base_expired);
    let root_key = *root.lock_key().as_bytes();

    // A leaf created before the tree timing change owns the construction bits.
    let old_leaf = root.new_leaf_lock();
    let old_leaf_id = old_leaf.lock_id();

    let new_timeout = PackedTime::with_flags(25, TIMEOUT_FLAG_MILLISECOND_TIME);
    let new_expired = PackedTime::with_flags(35, EXPRIED_FLAG_MILLISECOND_TIME);
    root.set_timing(new_timeout, new_expired);

    assert_eq!(old_leaf.lock_id(), old_leaf_id);
    assert_eq!(old_leaf.lock_key(), root.lock_key());

    old_leaf.acquire().await.unwrap();
    let command = last_command(&captured);
    assert_eq!(command[2], COMMAND_TYPE_LOCK);
    assert_eq!(command[19], 0);
    assert_eq!(lock_key(&command), root_key);
    assert_eq!(lock_id(&command), *old_leaf_id.as_bytes());
    assert_eq!(count(&command), u16::MAX);
    assert_eq!(r_count(&command), 1);
    assert_eq!(
        timeout_bits(&command),
        expected_packed(5, TIMEOUT_FLAG_RCOUNT_IS_PRIORITY),
        "an existing leaf must keep its construction timeout"
    );
    assert_eq!(
        expired_bits(&command),
        10,
        "an existing leaf must keep its construction expired bits"
    );

    // The live tree still acquires with its updated timing (check + leaf).
    root.acquire().await.unwrap();
    let frames = commands(&captured);
    let check = &frames[1];
    assert_eq!(check[2], COMMAND_TYPE_LOCK);
    assert_eq!(lock_key(check), root_key);
    assert_eq!(timeout_bits(check), new_timeout.bits());
    assert_eq!(expired_bits(check), 0);
    let tree_leaf = &frames[2];
    assert_eq!(lock_key(tree_leaf), root_key);
    assert_eq!(count(tree_leaf), u16::MAX);
    assert_eq!(r_count(tree_leaf), 1);
    assert_eq!(
        timeout_bits(tree_leaf),
        new_timeout
            .merge_flags(TIMEOUT_FLAG_RCOUNT_IS_PRIORITY)
            .bits()
    );
    assert_eq!(expired_bits(tree_leaf), new_expired.bits());

    // wait() remains the raw probe with zero expired.
    root.wait(7).await.unwrap();
    let wait = last_command(&captured);
    assert_eq!(wait[2], COMMAND_TYPE_LOCK);
    assert_eq!(lock_key(&wait), root_key);
    assert_eq!(count(&wait), 0);
    assert_eq!(r_count(&wait), 0);
    assert_eq!(timeout_bits(&wait), 7);
    assert_eq!(expired_bits(&wait), 0);

    // Leaves created after the tree timing change inherit the new bits.
    let new_leaf = root.new_leaf_lock();
    let new_leaf_id = new_leaf.lock_id();
    assert_ne!(new_leaf_id, old_leaf_id);
    new_leaf.acquire().await.unwrap();
    let command = last_command(&captured);
    assert_eq!(lock_key(&command), root_key);
    assert_eq!(lock_id(&command), *new_leaf_id.as_bytes());
    assert_eq!(count(&command), u16::MAX);
    assert_eq!(r_count(&command), 1);
    assert_eq!(
        timeout_bits(&command),
        new_timeout
            .merge_flags(TIMEOUT_FLAG_RCOUNT_IS_PRIORITY)
            .bits()
    );
    client.close().await;
}

#[tokio::test]
async fn read_write_timing_freezes_write_only_and_reads_use_current_timing() {
    let (address, captured) = spawn_mock(Vec::new(), |_, _, _| {});
    let client = Client::connect(address).await.unwrap();
    let database = client.select_database(0);
    database.set_default_timeout_flags(0);
    database.set_default_expired_flags(0);

    let write_timeout = PackedTime::with_flags(5, TIMEOUT_FLAG_MILLISECOND_TIME);
    let write_expired = PackedTime::with_flags(6, EXPRIED_FLAG_MILLISECOND_TIME);
    let read_timeout = PackedTime::with_flags(25, TIMEOUT_FLAG_MILLISECOND_TIME);
    let read_expired = PackedTime::with_flags(26, EXPRIED_FLAG_MILLISECOND_TIME);
    let later_timeout = PackedTime::with_flags(35, TIMEOUT_FLAG_MILLISECOND_TIME);
    let later_expired = PackedTime::with_flags(36, EXPRIED_FLAG_MILLISECOND_TIME);

    // A: write lock materialized before timing changes, then reads are fresh.
    let rw = BindingReadWriteLock::new(
        database.clone(),
        "binding-rw-timing",
        write_timeout,
        write_expired,
    );
    rw.acquire_write().await.unwrap();
    let write_id = lock_id(&last_command(&captured));
    rw.release_write().await.unwrap();

    rw.set_timing(read_timeout, read_expired);
    rw.acquire_read().await.unwrap();
    let first_read_id = lock_id(&last_command(&captured));

    // A read Lock keeps the timing it was constructed with on release.
    rw.set_timing(later_timeout, later_expired);
    rw.release_read().await.unwrap();
    rw.acquire_read().await.unwrap();
    let second_read_id = lock_id(&last_command(&captured));
    rw.release_read().await.unwrap();

    // Cached write identity/timing survive the set_timing calls.
    rw.acquire_write().await.unwrap();
    rw.release_write().await.unwrap();

    // B: timing set before the very first write materialization is adopted.
    let rw2 = BindingReadWriteLock::new(
        database,
        "binding-rw-timing-b",
        write_timeout,
        write_expired,
    );
    rw2.set_timing(read_timeout, read_expired);
    rw2.acquire_read().await.unwrap();
    rw2.release_read().await.unwrap();
    rw2.acquire_write().await.unwrap();
    let first_write_b = lock_id(&last_command(&captured));
    rw2.set_timing(later_timeout, later_expired);
    rw2.acquire_read().await.unwrap();
    rw2.release_read().await.unwrap();
    rw2.acquire_write().await.unwrap();
    rw2.release_write().await.unwrap();

    let frames = commands(&captured);

    // Frames 0/1: first write materialization uses construction timing.
    assert_eq!(timeout_bits(&frames[0]), write_timeout.bits());
    assert_eq!(expired_bits(&frames[0]), write_expired.bits());
    assert_eq!(lock_id(&frames[0]), write_id);
    assert_eq!(timeout_bits(&frames[1]), write_timeout.bits());
    assert_eq!(expired_bits(&frames[1]), write_expired.bits());
    assert_eq!(lock_id(&frames[1]), write_id);

    // Frame 2: a read after set_timing uses the current timing, not the cached
    // write timing, and gets its own identity.
    assert_eq!(timeout_bits(&frames[2]), read_timeout.bits());
    assert_eq!(expired_bits(&frames[2]), read_expired.bits());
    assert_eq!(lock_id(&frames[2]), first_read_id);
    assert_ne!(first_read_id, write_id);

    // Frame 3: releasing that read uses its acquisition timing, not the later
    // value written to the shared timing state.
    assert_eq!(timeout_bits(&frames[3]), read_timeout.bits());
    assert_eq!(expired_bits(&frames[3]), read_expired.bits());
    assert_eq!(lock_id(&frames[3]), first_read_id);

    // Frame 4/5: the next fresh read adopts the latest timing.
    assert_eq!(timeout_bits(&frames[4]), later_timeout.bits());
    assert_eq!(expired_bits(&frames[4]), later_expired.bits());
    assert_eq!(lock_id(&frames[4]), second_read_id);
    assert_ne!(second_read_id, first_read_id);
    assert_eq!(timeout_bits(&frames[5]), later_timeout.bits());
    assert_eq!(expired_bits(&frames[5]), later_expired.bits());
    assert_eq!(lock_id(&frames[5]), second_read_id);

    // Frames 6/7: write release/reacquire reuse the frozen identity and timing.
    assert_eq!(timeout_bits(&frames[6]), write_timeout.bits());
    assert_eq!(expired_bits(&frames[6]), write_expired.bits());
    assert_eq!(lock_id(&frames[6]), write_id);
    assert_eq!(timeout_bits(&frames[7]), write_timeout.bits());
    assert_eq!(expired_bits(&frames[7]), write_expired.bits());
    assert_eq!(lock_id(&frames[7]), write_id);

    // Frame 8: a read before the first write uses current timing; it releases
    // on frame 9 with that same acquisition timing.
    assert_eq!(timeout_bits(&frames[8]), read_timeout.bits());
    assert_eq!(expired_bits(&frames[8]), read_expired.bits());
    assert_eq!(timeout_bits(&frames[9]), read_timeout.bits());
    assert_eq!(expired_bits(&frames[9]), read_expired.bits());

    // Frame 10: first write materializes with the timing current at that point.
    assert_eq!(timeout_bits(&frames[10]), read_timeout.bits());
    assert_eq!(expired_bits(&frames[10]), read_expired.bits());
    assert_eq!(lock_id(&frames[10]), first_write_b);

    // Frames 11/12: later reads still follow current timing.
    assert_eq!(timeout_bits(&frames[11]), later_timeout.bits());
    assert_eq!(expired_bits(&frames[11]), later_expired.bits());
    assert_eq!(timeout_bits(&frames[12]), later_timeout.bits());
    assert_eq!(expired_bits(&frames[12]), later_expired.bits());

    // Frames 13/14: write stays on the timing captured at first materialization.
    assert_eq!(timeout_bits(&frames[13]), read_timeout.bits());
    assert_eq!(expired_bits(&frames[13]), read_expired.bits());
    assert_eq!(lock_id(&frames[13]), first_write_b);
    assert_eq!(timeout_bits(&frames[14]), read_timeout.bits());
    assert_eq!(expired_bits(&frames[14]), read_expired.bits());
    assert_eq!(lock_id(&frames[14]), first_write_b);

    client.close().await;
}

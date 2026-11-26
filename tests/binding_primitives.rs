#![cfg(feature = "aio")]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use ruslock::aio::Client;
use ruslock::binding::{BindingEvent, BindingLock, BindingMaxConcurrentFlow};
use ruslock::key::Key16;
use ruslock::protocol::constants::*;
use ruslock::time::PackedTime;
use ruslock::LockData;

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
    response[21] = request[20];
    response[22..38].copy_from_slice(&request[21..37]);
    response[38..54].copy_from_slice(&request[37..53]);
    response
}

fn read_u32(header: &[u8; 64], offset: usize) -> u32 {
    u32::from_le_bytes(header[offset..offset + 4].try_into().unwrap())
}

fn timeout_bits(header: &[u8; 64]) -> u32 {
    read_u32(header, 53)
}

fn expired_bits(header: &[u8; 64]) -> u32 {
    read_u32(header, 57)
}

fn lock_id(header: &[u8; 64]) -> [u8; 16] {
    header[21..37].try_into().unwrap()
}

fn count(header: &[u8; 64]) -> u16 {
    u16::from_le_bytes([header[61], header[62]])
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

/// Encodes the `LockData` payload the client writes on the wire.
fn encoded_set_data(value: &[u8]) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&((value.len() + 2) as u32).to_le_bytes());
    payload.push(0); // stage current, command type set
    payload.push(0); // no data flags
    payload.extend_from_slice(value);
    payload
}

/// `LockUnsetData`: two-byte empty value, command type unset.
fn encoded_unset_data() -> Vec<u8> {
    vec![2, 0, 0, 0, 1, 0]
}

/// Accepts one client, completes INIT, captures the first command header and
/// then answers it with a success result.
async fn capture_one_command() -> (String, oneshot::Receiver<[u8; 64]>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (captured_tx, captured_rx) = oneshot::channel();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).await.unwrap();
        stream.write_all(&init_response(&init)).await.unwrap();

        let mut command = [0u8; 64];
        stream.read_exact(&mut command).await.unwrap();
        let _ = captured_tx.send(command);
        stream
            .write_all(&lock_response(&command, COMMAND_RESULT_SUCCED))
            .await
            .unwrap();
    });
    (address, captured_rx)
}

#[tokio::test]
async fn event_wait_allows_set_on_same_handle() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (wait_seen_tx, wait_seen_rx) = oneshot::channel();

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).await.unwrap();
        stream.write_all(&init_response(&init)).await.unwrap();

        // The wait command must reach the wire before the test issues set().
        let mut wait = [0u8; 64];
        stream.read_exact(&mut wait).await.unwrap();
        assert_eq!(wait[2], COMMAND_TYPE_LOCK);
        assert_ne!(
            timeout_bits(&wait) & ((TIMEOUT_FLAG_LOCK_WAIT_WHEN_UNLOCK as u32) << 16),
            0,
            "wait command lost the wait-when-unlocked timeout flag"
        );
        wait_seen_tx.send(()).unwrap();

        // While the wait is still unanswered, the same handle must be able to set.
        let mut set = [0u8; 64];
        stream.read_exact(&mut set).await.unwrap();
        assert_eq!(set[2], COMMAND_TYPE_LOCK);
        assert_ne!(
            set[19] & LOCK_FLAG_UPDATE_WHEN_LOCKED,
            0,
            "event set did not use the update-locked path"
        );
        stream
            .write_all(&lock_response(&set, COMMAND_RESULT_SUCCED))
            .await
            .unwrap();
        stream
            .write_all(&lock_response(&wait, COMMAND_RESULT_SUCCED))
            .await
            .unwrap();
    });

    let client = Client::connect(address).await.unwrap();
    let event = BindingEvent::new(
        client.select_database(0),
        "binding-wait-set",
        PackedTime::new(5),
        PackedTime::new(10),
        false,
    );

    let wait_task = tokio::spawn(event.wait(PackedTime::new(5)));
    tokio::time::timeout(Duration::from_secs(2), wait_seen_rx)
        .await
        .expect("wait command was not sent within the bound")
        .unwrap();

    tokio::time::timeout(Duration::from_secs(2), event.set())
        .await
        .expect("set was blocked behind the in-flight wait")
        .unwrap();

    tokio::time::timeout(Duration::from_secs(2), wait_task)
        .await
        .expect("wait did not complete within the bound")
        .unwrap()
        .unwrap();

    client.close().await;
}

#[tokio::test]
async fn packed_time_keeps_caller_and_database_flags() {
    let db_timeout_flags = TIMEOUT_FLAG_RCOUNT_IS_PRIORITY;
    let db_expired_flags = EXPRIED_FLAG_MILLISECOND_TIME;
    let caller_timeout = PackedTime::with_flags(7, TIMEOUT_FLAG_MILLISECOND_TIME);
    let caller_expired = PackedTime::with_flags(9, EXPRIED_FLAG_MINUTE_TIME);
    let expected_timeout = caller_timeout.bits() | ((db_timeout_flags as u32) << 16);
    let expected_expired = caller_expired.bits() | ((db_expired_flags as u32) << 16);

    // Lock acquire must transmit the merged 32-bit timeout/expired values.
    let (address, captured_rx) = capture_one_command().await;
    let client = Client::connect(address).await.unwrap();
    let database = client.select_database(0);
    database.set_default_timeout_flags(db_timeout_flags);
    database.set_default_expired_flags(db_expired_flags);
    let lock = BindingLock::new(
        database,
        "binding-packed-lock",
        caller_timeout,
        caller_expired,
    );
    lock.acquire().await.unwrap();
    let lock_command = captured_rx.await.unwrap();
    assert_eq!(
        timeout_bits(&lock_command),
        expected_timeout,
        "lock acquire dropped caller or database timeout flags"
    );
    assert_eq!(
        expired_bits(&lock_command),
        expected_expired,
        "lock acquire dropped caller or database expired flags"
    );
    client.close().await;

    // Event set goes through event_lock; it must not truncate the packed times.
    let (address, captured_rx) = capture_one_command().await;
    let client = Client::connect(address).await.unwrap();
    let database = client.select_database(0);
    database.set_default_timeout_flags(db_timeout_flags);
    database.set_default_expired_flags(db_expired_flags);
    let event = BindingEvent::new(
        database,
        "binding-packed-event",
        caller_timeout,
        caller_expired,
        false,
    );
    event.set().await.unwrap();
    let event_command = captured_rx.await.unwrap();
    assert_eq!(
        timeout_bits(&event_command),
        expected_timeout,
        "event set dropped caller or database timeout flags"
    );
    assert_eq!(
        expired_bits(&event_command),
        expected_expired,
        "event set dropped caller or database expired flags"
    );
    client.close().await;
}

#[tokio::test]
async fn event_clear_with_data_uses_default_set_polarity_without_current_data() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let captured = Arc::new(Mutex::new(Vec::<([u8; 64], Vec<u8>)>::new()));
    let capture = Arc::clone(&captured);

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).await.unwrap();
        stream.write_all(&init_response(&init)).await.unwrap();

        for _ in 0..2 {
            let mut command = [0u8; 64];
            stream.read_exact(&mut command).await.unwrap();
            let extra = read_request_extra(&mut stream, &command).await;
            capture.lock().unwrap().push((command, extra));
            stream
                .write_all(&lock_response(&command, COMMAND_RESULT_SUCCED))
                .await
                .unwrap();
        }
    });

    let client = Client::connect(address).await.unwrap();
    let database = client.select_database(0);
    database.set_default_timeout_flags(0);
    database.set_default_expired_flags(0);
    let base_timeout = PackedTime::with_flags(5, TIMEOUT_FLAG_MILLISECOND_TIME);
    let base_expired = PackedTime::with_flags(10, EXPRIED_FLAG_MILLISECOND_TIME);
    let key = Key16::new("binding-clear-data");

    let default_set = BindingEvent::new(
        database.clone(),
        "binding-clear-data",
        base_timeout,
        base_expired,
        true,
    );
    default_set
        .clear_with_data(Some(LockData::set("off")))
        .await
        .unwrap();
    assert!(
        default_set.current_data().is_none(),
        "clear must not update the facade current data"
    );

    let not_default = BindingEvent::new(
        database,
        "binding-clear-data",
        base_timeout,
        base_expired,
        false,
    );
    not_default
        .clear_with_data(Some(LockData::set("bye")))
        .await
        .unwrap();
    assert!(
        not_default.current_data().is_none(),
        "clear must not update the facade current data"
    );

    let frames = captured.lock().unwrap();

    // defaultSeted=true: update the event lock with a contains-data acquire.
    let (set_header, set_payload) = &frames[0];
    assert_eq!(set_header[2], COMMAND_TYPE_LOCK);
    assert_eq!(
        set_header[19],
        LOCK_FLAG_UPDATE_WHEN_LOCKED | LOCK_FLAG_CONTAINS_DATA
    );
    assert_eq!(lock_id(set_header), key.into_bytes());
    assert_eq!(timeout_bits(set_header), base_timeout.bits());
    assert_eq!(expired_bits(set_header), base_expired.bits());
    assert_eq!(count(set_header), 0);
    assert_eq!(set_payload, &encoded_set_data(b"off"));

    // defaultSeted=false: release the event lock with a contains-data unlock.
    let (clear_header, clear_payload) = &frames[1];
    assert_eq!(clear_header[2], COMMAND_TYPE_UNLOCK);
    assert_eq!(clear_header[19], UNLOCK_FLAG_CONTAINS_DATA);
    assert_eq!(lock_id(clear_header), key.into_bytes());
    assert_eq!(timeout_bits(clear_header), base_timeout.bits());
    assert_eq!(expired_bits(clear_header), base_expired.bits());
    assert_eq!(count(clear_header), 1);
    assert_eq!(clear_payload, &encoded_set_data(b"bye"));

    client.close().await;
}

#[tokio::test]
async fn event_wait_and_timeout_retry_clear_packed_compensates_for_default_set() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let captured = Arc::new(Mutex::new(Vec::<([u8; 64], Vec<u8>)>::new()));
    let capture = Arc::clone(&captured);

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).await.unwrap();
        stream.write_all(&init_response(&init)).await.unwrap();

        // Timed wait: answer with a protocol timeout to trigger compensation.
        let mut wait = [0u8; 64];
        stream.read_exact(&mut wait).await.unwrap();
        let wait_extra = read_request_extra(&mut stream, &wait).await;
        capture.lock().unwrap().push((wait, wait_extra));
        stream
            .write_all(&lock_response(&wait, COMMAND_RESULT_TIMEOUT))
            .await
            .unwrap();

        // Compensation: update the event lock, then release it.
        let mut update = [0u8; 64];
        stream.read_exact(&mut update).await.unwrap();
        let update_extra = read_request_extra(&mut stream, &update).await;
        capture.lock().unwrap().push((update, update_extra));
        stream
            .write_all(&lock_response(&update, COMMAND_RESULT_SUCCED))
            .await
            .unwrap();

        let mut release = [0u8; 64];
        stream.read_exact(&mut release).await.unwrap();
        let release_extra = read_request_extra(&mut stream, &release).await;
        capture.lock().unwrap().push((release, release_extra));
        stream
            .write_all(&lock_response(&release, COMMAND_RESULT_SUCCED))
            .await
            .unwrap();
    });

    let client = Client::connect(address).await.unwrap();
    let database = client.select_database(0);
    database.set_default_timeout_flags(0);
    database.set_default_expired_flags(0);
    let base_timeout = PackedTime::with_flags(5, TIMEOUT_FLAG_MILLISECOND_TIME);
    let base_expired = PackedTime::with_flags(10, EXPRIED_FLAG_MILLISECOND_TIME);
    let wait_timeout = PackedTime::with_flags(3, TIMEOUT_FLAG_MILLISECOND_TIME);
    let key = Key16::new("binding-retry-clear");

    let event = BindingEvent::new(
        database,
        "binding-retry-clear",
        base_timeout,
        base_expired,
        true,
    );
    event
        .wait_and_timeout_retry_clear_packed(wait_timeout)
        .await
        .unwrap();
    assert!(
        event.current_data().is_none(),
        "the timed-out wait result had no data"
    );

    let frames = captured.lock().unwrap();

    let (wait_header, wait_payload) = &frames[0];
    assert_eq!(wait_header[2], COMMAND_TYPE_LOCK);
    assert_eq!(wait_header[19], LOCK_FLAG_CONTAINS_DATA);
    assert_eq!(lock_id(wait_header), [0u8; 16]);
    assert_eq!(timeout_bits(wait_header), wait_timeout.bits());
    assert_eq!(expired_bits(wait_header), 0);
    assert_eq!(count(wait_header), 0);
    assert_eq!(wait_payload, &encoded_unset_data());

    let (update_header, update_payload) = &frames[1];
    assert_eq!(update_header[2], COMMAND_TYPE_LOCK);
    assert_eq!(
        update_header[19],
        LOCK_FLAG_UPDATE_WHEN_LOCKED | LOCK_FLAG_CONTAINS_DATA
    );
    assert_eq!(lock_id(update_header), key.into_bytes());
    assert_eq!(timeout_bits(update_header), base_timeout.bits());
    assert_eq!(expired_bits(update_header), base_expired.bits());
    assert_eq!(update_payload, &encoded_unset_data());

    let (release_header, release_payload) = &frames[2];
    assert_eq!(release_header[2], COMMAND_TYPE_UNLOCK);
    assert_eq!(release_header[19], 0);
    assert_eq!(lock_id(release_header), key.into_bytes());
    assert!(release_payload.is_empty());

    client.close().await;
}

#[tokio::test]
async fn event_wait_and_timeout_retry_clear_packed_releases_after_non_default_wait() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let captured = Arc::new(Mutex::new(Vec::<([u8; 64], Vec<u8>)>::new()));
    let capture = Arc::clone(&captured);

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).await.unwrap();
        stream.write_all(&init_response(&init)).await.unwrap();

        let mut wait = [0u8; 64];
        stream.read_exact(&mut wait).await.unwrap();
        let wait_extra = read_request_extra(&mut stream, &wait).await;
        capture.lock().unwrap().push((wait, wait_extra));
        let mut response = lock_response(&wait, COMMAND_RESULT_SUCCED);
        response[20] = LOCK_FLAG_CONTAINS_DATA;
        stream.write_all(&response).await.unwrap();
        stream.write_all(&encoded_set_data(b"hi")).await.unwrap();

        let mut release = [0u8; 64];
        stream.read_exact(&mut release).await.unwrap();
        let release_extra = read_request_extra(&mut stream, &release).await;
        capture.lock().unwrap().push((release, release_extra));
        stream
            .write_all(&lock_response(&release, COMMAND_RESULT_SUCCED))
            .await
            .unwrap();
    });

    let client = Client::connect(address).await.unwrap();
    let database = client.select_database(0);
    database.set_default_timeout_flags(0);
    database.set_default_expired_flags(0);
    let base_timeout = PackedTime::with_flags(5, TIMEOUT_FLAG_MILLISECOND_TIME);
    let base_expired = PackedTime::with_flags(10, EXPRIED_FLAG_MILLISECOND_TIME);
    let wait_timeout = PackedTime::with_flags(3, TIMEOUT_FLAG_MILLISECOND_TIME);
    let key = Key16::new("binding-retry-release");

    let event = BindingEvent::new(
        database,
        "binding-retry-release",
        base_timeout,
        base_expired,
        false,
    );
    event
        .wait_and_timeout_retry_clear_packed(wait_timeout)
        .await
        .unwrap();
    assert_eq!(
        event.current_data().unwrap().as_string().unwrap(),
        "hi",
        "a successful wait must expose its result payload"
    );

    let frames = captured.lock().unwrap();

    let (wait_header, wait_payload) = &frames[0];
    assert_eq!(wait_header[2], COMMAND_TYPE_LOCK);
    assert_eq!(wait_header[19], LOCK_FLAG_CONTAINS_DATA);
    assert_eq!(lock_id(wait_header), [0u8; 16]);
    assert_eq!(
        timeout_bits(wait_header),
        wait_timeout
            .merge_flags(TIMEOUT_FLAG_LOCK_WAIT_WHEN_UNLOCK)
            .bits()
    );
    assert_eq!(expired_bits(wait_header), 0);
    assert_eq!(count(wait_header), 1);
    assert_eq!(wait_payload, &encoded_unset_data());

    let (release_header, release_payload) = &frames[1];
    assert_eq!(release_header[2], COMMAND_TYPE_UNLOCK);
    assert_eq!(release_header[19], 0);
    assert_eq!(lock_id(release_header), key.into_bytes());
    assert_eq!(count(release_header), 1);
    assert!(release_payload.is_empty());
    client.close().await;
}

#[tokio::test]
async fn binding_lock_timing_setters_apply_to_next_request_and_preserve_identity() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let captured = Arc::new(Mutex::new(Vec::<[u8; 64]>::new()));
    let capture = Arc::clone(&captured);

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).await.unwrap();
        stream.write_all(&init_response(&init)).await.unwrap();

        for _ in 0..2 {
            let mut command = [0u8; 64];
            stream.read_exact(&mut command).await.unwrap();
            capture.lock().unwrap().push(command);
            stream
                .write_all(&lock_response(&command, COMMAND_RESULT_SUCCED))
                .await
                .unwrap();
        }
    });

    let client = Client::connect(address).await.unwrap();
    let database = client.select_database(0);
    database.set_default_timeout_flags(0);
    database.set_default_expired_flags(0);

    let lock = BindingLock::new(
        database,
        "binding-timing-lock",
        PackedTime::new(10),
        PackedTime::new(20),
    );
    let expected_id = lock.lock_id();
    lock.acquire().await.unwrap();

    let timeout = PackedTime::with_flags(30, TIMEOUT_FLAG_MILLISECOND_TIME);
    let expired = PackedTime::with_flags(40, EXPRIED_FLAG_MILLISECOND_TIME);
    lock.set_timing(timeout, expired);

    assert_eq!(lock.timeout(), timeout);
    assert_eq!(lock.expired(), expired);
    assert_eq!(
        lock.lock_id(),
        expected_id,
        "changing timing must not replace the lock identity"
    );

    lock.acquire().await.unwrap();

    let frames = captured.lock().unwrap();
    assert_eq!(timeout_bits(&frames[0]), PackedTime::new(10).bits());
    assert_eq!(expired_bits(&frames[0]), PackedTime::new(20).bits());
    assert_eq!(timeout_bits(&frames[1]), timeout.bits());
    assert_eq!(expired_bits(&frames[1]), expired.bits());
    assert_eq!(lock_id(&frames[1]), expected_id.into_bytes());

    client.close().await;
}

#[tokio::test]
async fn max_concurrent_flow_expired_flag_set_before_first_acquire_reaches_wire() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let captured = Arc::new(Mutex::new(Vec::<[u8; 64]>::new()));
    let capture = Arc::clone(&captured);

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut init = [0u8; 64];
        stream.read_exact(&mut init).await.unwrap();
        stream.write_all(&init_response(&init)).await.unwrap();

        let mut command = [0u8; 64];
        stream.read_exact(&mut command).await.unwrap();
        capture.lock().unwrap().push(command);
        stream
            .write_all(&lock_response(&command, COMMAND_RESULT_SUCCED))
            .await
            .unwrap();
    });

    let client = Client::connect(address).await.unwrap();
    let database = client.select_database(0);
    database.set_default_timeout_flags(0);
    database.set_default_expired_flags(0);

    // Baseline ClientTest sets the millisecond expired flag before the first
    // acquire, while the constructor still holds a 100-second default.
    let flow = BindingMaxConcurrentFlow::new(
        database,
        "binding-timing-max-flow",
        5,
        2,
        PackedTime::new(100),
        PackedTime::new(100),
    );
    let expected_id = flow.lock_id();
    flow.set_expired(PackedTime::with_flags(100, EXPRIED_FLAG_MILLISECOND_TIME));

    assert_eq!(
        flow.lock_id(),
        expected_id,
        "timing must not reset the identity"
    );
    flow.acquire().await.unwrap();

    let frames = captured.lock().unwrap();
    let command = &frames[0];
    assert_eq!(
        expired_bits(command),
        PackedTime::with_flags(100, EXPRIED_FLAG_MILLISECOND_TIME).bits(),
        "the millisecond expired flag must reach the wire"
    );
    assert_eq!(
        timeout_bits(command),
        PackedTime::with_flags(100, TIMEOUT_FLAG_RCOUNT_IS_PRIORITY).bits(),
        "setting expired must not disturb timeout or the priority flag"
    );
    assert_eq!(lock_id(command), expected_id.into_bytes());
    assert_eq!(count(command), 4);

    client.close().await;
}

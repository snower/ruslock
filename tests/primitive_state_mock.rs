#[cfg(feature = "blocking")]
mod blocking_state {
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::thread;

    use ruslock::blocking::Client;
    use ruslock::protocol::constants::*;
    use ruslock::{Key16, LockData, PackedTime, SlockError};

    fn init_response(request: &[u8; 64]) -> [u8; 64] {
        let mut response = [0u8; 64];
        response[0] = MAGIC;
        response[1] = VERSION;
        response[2] = COMMAND_TYPE_INIT;
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

    fn lock_response_with_lock_id(
        request: &[u8; 64],
        result: u8,
        data: Option<&[u8]>,
        lock_id: [u8; 16],
    ) -> [u8; 64] {
        let mut response = lock_response(request, result, data);
        response[22..38].copy_from_slice(&lock_id);
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

    fn read_frame(stream: &mut TcpStream) -> [u8; 64] {
        let mut frame = [0u8; 64];
        stream.read_exact(&mut frame).unwrap();
        frame
    }

    fn read_extra_if_present(stream: &mut TcpStream, request: &[u8; 64]) -> Option<Vec<u8>> {
        if (request[19] & (LOCK_FLAG_CONTAINS_DATA | UNLOCK_FLAG_CONTAINS_DATA)) == 0 {
            return None;
        }
        let mut len_bytes = [0u8; 4];
        stream.read_exact(&mut len_bytes).unwrap();
        let len = u32::from_le_bytes(len_bytes) as usize;
        let mut payload = vec![0u8; len];
        stream.read_exact(&mut payload).unwrap();
        Some(payload)
    }

    fn write_lock_success(stream: &mut TcpStream, request: &[u8; 64]) {
        stream
            .write_all(&lock_response(request, COMMAND_RESULT_SUCCED, None))
            .unwrap();
    }

    fn write_lock_success_with_data(stream: &mut TcpStream, request: &[u8; 64], data: &[u8]) {
        stream
            .write_all(&lock_response(request, COMMAND_RESULT_SUCCED, Some(data)))
            .unwrap();
        stream
            .write_all(&(data.len() as u32).to_le_bytes())
            .unwrap();
        stream.write_all(data).unwrap();
    }

    fn request_lock_id(request: &[u8; 64]) -> [u8; 16] {
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&request[21..37]);
        bytes
    }

    fn request_key(request: &[u8; 64]) -> [u8; 16] {
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&request[37..53]);
        bytes
    }

    fn request_timeout_bits(request: &[u8; 64]) -> u32 {
        u32::from_le_bytes(request[53..57].try_into().unwrap())
    }

    fn request_expired_bits(request: &[u8; 64]) -> u32 {
        u32::from_le_bytes(request[57..61].try_into().unwrap())
    }

    fn request_count(request: &[u8; 64]) -> u16 {
        u16::from_le_bytes(request[61..63].try_into().unwrap())
    }

    fn group_lock_id(client_id: u64, version_id: u64) -> [u8; 16] {
        let mut bytes = [0u8; 16];
        bytes[0..8].copy_from_slice(&version_id.to_le_bytes());
        bytes[8..16].copy_from_slice(&client_id.to_le_bytes());
        bytes
    }

    #[test]
    fn event_default_unset_uses_update_release_and_wait_flags() {
        let key = Key16::new("event-state").into_bytes();
        let address = start_server(move |mut stream| {
            let init = read_frame(&mut stream);
            stream.write_all(&init_response(&init)).unwrap();

            let set = read_frame(&mut stream);
            assert_eq!(set[2], COMMAND_TYPE_LOCK);
            assert_eq!(
                set[19],
                LOCK_FLAG_UPDATE_WHEN_LOCKED | LOCK_FLAG_CONTAINS_DATA
            );
            assert_eq!(set[20], 1);
            assert_eq!(request_lock_id(&set), key);
            assert_eq!(request_key(&set), key);
            assert_eq!(request_count(&set), 1);
            assert_eq!(request_timeout_bits(&set), PackedTime::new(3).bits());
            assert_eq!(request_expired_bits(&set), PackedTime::new(7).bits());
            assert!(read_extra_if_present(&mut stream, &set).is_some());
            write_lock_success(&mut stream, &set);

            let clear = read_frame(&mut stream);
            assert_eq!(clear[2], COMMAND_TYPE_UNLOCK);
            assert_eq!(clear[19], 0);
            assert_eq!(request_lock_id(&clear), key);
            assert_eq!(request_count(&clear), 1);
            write_lock_success(&mut stream, &clear);

            let wait = read_frame(&mut stream);
            assert_eq!(wait[2], COMMAND_TYPE_LOCK);
            assert_eq!(wait[19], 0);
            assert_eq!(request_key(&wait), key);
            assert_eq!(request_count(&wait), 1);
            assert_eq!(
                request_timeout_bits(&wait),
                PackedTime::with_flags(9, TIMEOUT_FLAG_LOCK_WAIT_WHEN_UNLOCK).bits()
            );
            assert_eq!(request_expired_bits(&wait), PackedTime::new(0).bits());
            let data = [LOCK_DATA_COMMAND_TYPE_SET, 0, b'r', b'e', b'a', b'd', b'y'];
            write_lock_success_with_data(&mut stream, &wait, &data);
        });

        let client = Client::connect(address).unwrap();
        let db = client.select_database(1);
        let mut event = db.event("event-state", 3, 7, false);
        event.set_with_data(Some(LockData::set("payload"))).unwrap();
        event.clear().unwrap();
        event.wait(9).unwrap();
        assert_eq!(event.current_data().unwrap().as_string().unwrap(), "ready");
    }

    #[test]
    fn shared_primitives_send_expected_counts_and_release_state() {
        let address = start_server(move |mut stream| {
            let init = read_frame(&mut stream);
            stream.write_all(&init_response(&init)).unwrap();

            let reentrant_acquire = read_frame(&mut stream);
            assert_eq!(reentrant_acquire[2], COMMAND_TYPE_LOCK);
            assert_eq!(request_count(&reentrant_acquire), 0);
            assert_eq!(reentrant_acquire[63], 0xff);
            write_lock_success(&mut stream, &reentrant_acquire);

            let reentrant_release = read_frame(&mut stream);
            assert_eq!(reentrant_release[2], COMMAND_TYPE_UNLOCK);
            assert_eq!(request_count(&reentrant_release), 0);
            assert_eq!(reentrant_release[63], 0xff);
            write_lock_success(&mut stream, &reentrant_release);

            let semaphore_acquire = read_frame(&mut stream);
            assert_eq!(semaphore_acquire[2], COMMAND_TYPE_LOCK);
            assert_eq!(request_count(&semaphore_acquire), 2);
            write_lock_success(&mut stream, &semaphore_acquire);

            let semaphore_release = read_frame(&mut stream);
            assert_eq!(semaphore_release[2], COMMAND_TYPE_UNLOCK);
            assert_eq!(
                semaphore_release[19],
                UNLOCK_FLAG_UNLOCK_FIRST_LOCK_WHEN_UNLOCKED
            );
            assert_eq!(request_lock_id(&semaphore_release), [0u8; 16]);
            assert_eq!(request_count(&semaphore_release), 2);
            write_lock_success(&mut stream, &semaphore_release);

            let write_acquire = read_frame(&mut stream);
            assert_eq!(write_acquire[2], COMMAND_TYPE_LOCK);
            assert_eq!(request_count(&write_acquire), 0);
            let write_lock_id = request_lock_id(&write_acquire);
            write_lock_success(&mut stream, &write_acquire);

            let write_release = read_frame(&mut stream);
            assert_eq!(write_release[2], COMMAND_TYPE_UNLOCK);
            assert_eq!(request_lock_id(&write_release), write_lock_id);
            assert_eq!(request_count(&write_release), 0);
            write_lock_success(&mut stream, &write_release);

            let read_acquire = read_frame(&mut stream);
            assert_eq!(read_acquire[2], COMMAND_TYPE_LOCK);
            assert_eq!(request_count(&read_acquire), u16::MAX);
            let read_lock_id = request_lock_id(&read_acquire);
            write_lock_success(&mut stream, &read_acquire);

            let read_release = read_frame(&mut stream);
            assert_eq!(read_release[2], COMMAND_TYPE_UNLOCK);
            assert_eq!(request_lock_id(&read_release), read_lock_id);
            assert_eq!(request_count(&read_release), u16::MAX);
            write_lock_success(&mut stream, &read_release);

            let priority_acquire = read_frame(&mut stream);
            assert_eq!(priority_acquire[2], COMMAND_TYPE_LOCK);
            assert_eq!(
                request_timeout_bits(&priority_acquire),
                PackedTime::with_flags(5, TIMEOUT_FLAG_RCOUNT_IS_PRIORITY).bits()
            );
            assert_eq!(priority_acquire[63], 7);
            write_lock_success(&mut stream, &priority_acquire);

            let priority_release = read_frame(&mut stream);
            assert_eq!(priority_release[2], COMMAND_TYPE_UNLOCK);
            assert_eq!(priority_release[63], 7);
            write_lock_success(&mut stream, &priority_release);

            let max_flow_acquire = read_frame(&mut stream);
            assert_eq!(max_flow_acquire[2], COMMAND_TYPE_LOCK);
            assert_eq!(request_count(&max_flow_acquire), 4);
            write_lock_success(&mut stream, &max_flow_acquire);

            let max_flow_release = read_frame(&mut stream);
            assert_eq!(max_flow_release[2], COMMAND_TYPE_UNLOCK);
            assert_eq!(request_count(&max_flow_release), 4);
            write_lock_success(&mut stream, &max_flow_release);

            let token_flow_acquire = read_frame(&mut stream);
            assert_eq!(token_flow_acquire[2], COMMAND_TYPE_LOCK);
            assert_eq!(request_count(&token_flow_acquire), 2);
            assert_eq!(
                request_expired_bits(&token_flow_acquire),
                PackedTime::with_flags(1250, EXPRIED_FLAG_MILLISECOND_TIME).bits()
            );
            write_lock_success(&mut stream, &token_flow_acquire);

            let tree_acquire = read_frame(&mut stream);
            assert_eq!(tree_acquire[2], COMMAND_TYPE_LOCK);
            assert_eq!(tree_acquire[19], LOCK_FLAG_LOCK_TREE_LOCK);
            assert_eq!(request_count(&tree_acquire), u16::MAX);
            assert_eq!(tree_acquire[63], 1);
            assert_eq!(
                request_timeout_bits(&tree_acquire),
                PackedTime::with_flags(5, TIMEOUT_FLAG_RCOUNT_IS_PRIORITY).bits()
            );
            write_lock_success(&mut stream, &tree_acquire);

            let tree_release = read_frame(&mut stream);
            assert_eq!(tree_release[2], COMMAND_TYPE_UNLOCK);
            assert_eq!(tree_release[19], UNLOCK_FLAG_UNLOCK_TREE_LOCK);
            assert_eq!(request_count(&tree_release), u16::MAX);
            assert_eq!(tree_release[63], 1);
            write_lock_success(&mut stream, &tree_release);

            let leaf_acquire = read_frame(&mut stream);
            assert_eq!(leaf_acquire[2], COMMAND_TYPE_LOCK);
            assert_eq!(leaf_acquire[19], 0);
            assert_eq!(request_count(&leaf_acquire), u16::MAX);
            assert_eq!(leaf_acquire[63], 1);
            write_lock_success(&mut stream, &leaf_acquire);

            let leaf_release = read_frame(&mut stream);
            assert_eq!(leaf_release[2], COMMAND_TYPE_UNLOCK);
            assert_eq!(leaf_release[19], UNLOCK_FLAG_UNLOCK_TREE_LOCK);
            assert_eq!(request_count(&leaf_release), u16::MAX);
            assert_eq!(leaf_release[63], 1);
            write_lock_success(&mut stream, &leaf_release);
        });

        let client = Client::connect(address).unwrap();
        let db = client.select_database(2);

        let mut reentrant = db.reentrant_lock("reentrant-state", 5, 6);
        reentrant.acquire().unwrap();
        reentrant.release().unwrap();

        let mut semaphore = db.semaphore("semaphore-state", 3, 5, 6);
        semaphore.acquire().unwrap();
        semaphore.release().unwrap();

        let mut rw = db.read_write_lock("rw-state", 5, 6);
        assert!(matches!(
            rw.release_read().unwrap_err(),
            SlockError::LockData(_)
        ));
        rw.acquire_write().unwrap();
        rw.release_write().unwrap();
        rw.acquire_read().unwrap();
        rw.release_read().unwrap();

        let mut priority = db.priority_lock("priority-state", 7, 5, 6);
        priority.acquire().unwrap();
        priority.release().unwrap();

        let mut max_flow = db.max_concurrent_flow("max-flow-state", 5, 5, 6);
        max_flow.acquire().unwrap();
        max_flow.release().unwrap();

        let mut token_flow = db.token_bucket_flow("token-flow-state", 3, 5, 1.25);
        token_flow.acquire().unwrap();

        let mut tree = db.tree_lock("tree-state", 5, 6);
        tree.acquire().unwrap();
        tree.release().unwrap();
        let mut leaf = tree.new_leaf_lock();
        leaf.acquire().unwrap();
        leaf.release().unwrap();
    }

    #[test]
    fn group_event_updates_version_from_wait_and_wakeup_results() {
        let expected_initial_id = group_lock_id(11, 2);
        let wait_result_id = group_lock_id(11, 3);
        let wakeup_result_id = group_lock_id(0, 4);
        let address = start_server(move |mut stream| {
            let init = read_frame(&mut stream);
            stream.write_all(&init_response(&init)).unwrap();

            let wait = read_frame(&mut stream);
            assert_eq!(wait[2], COMMAND_TYPE_LOCK);
            assert_eq!(wait[19], 0);
            assert_eq!(request_lock_id(&wait), expected_initial_id);
            assert_eq!(
                request_timeout_bits(&wait),
                PackedTime::with_flags(6, TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED).bits()
            );
            let data = [LOCK_DATA_COMMAND_TYPE_SET, 0, b'g', b'r', b'o', b'u', b'p'];
            let wait_response = lock_response_with_lock_id(
                &wait,
                COMMAND_RESULT_SUCCED,
                Some(&data),
                wait_result_id,
            );
            stream.write_all(&wait_response).unwrap();
            stream
                .write_all(&(data.len() as u32).to_le_bytes())
                .unwrap();
            stream.write_all(&data).unwrap();

            let wakeup = read_frame(&mut stream);
            assert_eq!(wakeup[2], COMMAND_TYPE_UNLOCK);
            assert_eq!(
                wakeup[19],
                UNLOCK_FLAG_UNLOCK_FIRST_LOCK_WHEN_UNLOCKED
                    | UNLOCK_FLAG_SUCCED_TO_LOCK_WAIT
                    | UNLOCK_FLAG_CONTAINS_DATA
            );
            assert_eq!(request_lock_id(&wakeup), [0u8; 16]);
            assert!(read_extra_if_present(&mut stream, &wakeup).is_some());
            let wakeup_response =
                lock_response_with_lock_id(&wakeup, COMMAND_RESULT_SUCCED, None, wakeup_result_id);
            stream.write_all(&wakeup_response).unwrap();
        });

        let client = Client::connect(address).unwrap();
        let db = client.select_database(3);
        let mut group = db.group_event("group-state", 11, 2, 4, 5);
        group.wait(6).unwrap();
        assert_eq!(group.version_id(), 3);
        assert_eq!(group.current_data().unwrap().as_string().unwrap(), "group");
        group.wakeup(Some(LockData::set("next"))).unwrap();
        assert_eq!(group.version_id(), 4);
    }
}

#[cfg(feature = "aio")]
mod aio_state {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use ruslock::aio::Client;
    use ruslock::protocol::constants::*;
    use ruslock::{Key16, LockData, PackedTime, SlockError};

    fn init_response(request: &[u8; 64]) -> [u8; 64] {
        let mut response = [0u8; 64];
        response[0] = MAGIC;
        response[1] = VERSION;
        response[2] = COMMAND_TYPE_INIT;
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

    fn lock_response_with_lock_id(
        request: &[u8; 64],
        result: u8,
        data: Option<&[u8]>,
        lock_id: [u8; 16],
    ) -> [u8; 64] {
        let mut response = lock_response(request, result, data);
        response[22..38].copy_from_slice(&lock_id);
        response
    }

    async fn start_server<F, Fut>(handler: F) -> String
    where
        F: FnOnce(TcpStream) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handler(stream).await;
        });
        address
    }

    async fn read_frame(stream: &mut TcpStream) -> [u8; 64] {
        let mut frame = [0u8; 64];
        stream.read_exact(&mut frame).await.unwrap();
        frame
    }

    async fn read_extra_if_present(stream: &mut TcpStream, request: &[u8; 64]) -> Option<Vec<u8>> {
        if (request[19] & (LOCK_FLAG_CONTAINS_DATA | UNLOCK_FLAG_CONTAINS_DATA)) == 0 {
            return None;
        }
        let mut len_bytes = [0u8; 4];
        stream.read_exact(&mut len_bytes).await.unwrap();
        let len = u32::from_le_bytes(len_bytes) as usize;
        let mut payload = vec![0u8; len];
        stream.read_exact(&mut payload).await.unwrap();
        Some(payload)
    }

    async fn write_lock_success(stream: &mut TcpStream, request: &[u8; 64]) {
        stream
            .write_all(&lock_response(request, COMMAND_RESULT_SUCCED, None))
            .await
            .unwrap();
    }

    async fn write_lock_success_with_data(stream: &mut TcpStream, request: &[u8; 64], data: &[u8]) {
        stream
            .write_all(&lock_response(request, COMMAND_RESULT_SUCCED, Some(data)))
            .await
            .unwrap();
        stream
            .write_all(&(data.len() as u32).to_le_bytes())
            .await
            .unwrap();
        stream.write_all(data).await.unwrap();
    }

    fn request_lock_id(request: &[u8; 64]) -> [u8; 16] {
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&request[21..37]);
        bytes
    }

    fn request_key(request: &[u8; 64]) -> [u8; 16] {
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&request[37..53]);
        bytes
    }

    fn request_timeout_bits(request: &[u8; 64]) -> u32 {
        u32::from_le_bytes(request[53..57].try_into().unwrap())
    }

    fn request_expired_bits(request: &[u8; 64]) -> u32 {
        u32::from_le_bytes(request[57..61].try_into().unwrap())
    }

    fn request_count(request: &[u8; 64]) -> u16 {
        u16::from_le_bytes(request[61..63].try_into().unwrap())
    }

    fn group_lock_id(client_id: u64, version_id: u64) -> [u8; 16] {
        let mut bytes = [0u8; 16];
        bytes[0..8].copy_from_slice(&version_id.to_le_bytes());
        bytes[8..16].copy_from_slice(&client_id.to_le_bytes());
        bytes
    }

    #[tokio::test]
    async fn event_default_unset_uses_update_release_and_wait_flags() {
        let key = Key16::new("async-event-state").into_bytes();
        let address = start_server(move |mut stream| async move {
            let init = read_frame(&mut stream).await;
            stream.write_all(&init_response(&init)).await.unwrap();

            let set = read_frame(&mut stream).await;
            assert_eq!(set[2], COMMAND_TYPE_LOCK);
            assert_eq!(
                set[19],
                LOCK_FLAG_UPDATE_WHEN_LOCKED | LOCK_FLAG_CONTAINS_DATA
            );
            assert_eq!(set[20], 1);
            assert_eq!(request_lock_id(&set), key);
            assert_eq!(request_key(&set), key);
            assert_eq!(request_count(&set), 1);
            assert_eq!(request_timeout_bits(&set), PackedTime::new(3).bits());
            assert_eq!(request_expired_bits(&set), PackedTime::new(7).bits());
            assert!(read_extra_if_present(&mut stream, &set).await.is_some());
            write_lock_success(&mut stream, &set).await;

            let clear = read_frame(&mut stream).await;
            assert_eq!(clear[2], COMMAND_TYPE_UNLOCK);
            assert_eq!(clear[19], 0);
            assert_eq!(request_lock_id(&clear), key);
            assert_eq!(request_count(&clear), 1);
            write_lock_success(&mut stream, &clear).await;

            let wait = read_frame(&mut stream).await;
            assert_eq!(wait[2], COMMAND_TYPE_LOCK);
            assert_eq!(wait[19], 0);
            assert_eq!(request_key(&wait), key);
            assert_eq!(request_count(&wait), 1);
            assert_eq!(
                request_timeout_bits(&wait),
                PackedTime::with_flags(9, TIMEOUT_FLAG_LOCK_WAIT_WHEN_UNLOCK).bits()
            );
            assert_eq!(request_expired_bits(&wait), PackedTime::new(0).bits());
            let data = [LOCK_DATA_COMMAND_TYPE_SET, 0, b'r', b'e', b'a', b'd', b'y'];
            write_lock_success_with_data(&mut stream, &wait, &data).await;
        })
        .await;

        let client = Client::connect(address).await.unwrap();
        let db = client.select_database(1);
        let mut event = db.event("async-event-state", 3, 7, false);
        event
            .set_with_data(Some(LockData::set("payload")))
            .await
            .unwrap();
        event.clear().await.unwrap();
        event.wait(9).await.unwrap();
        assert_eq!(event.current_data().unwrap().as_string().unwrap(), "ready");
    }

    #[tokio::test]
    async fn shared_primitives_send_expected_counts_and_release_state() {
        let address = start_server(move |mut stream| async move {
            let init = read_frame(&mut stream).await;
            stream.write_all(&init_response(&init)).await.unwrap();

            let reentrant_acquire = read_frame(&mut stream).await;
            assert_eq!(reentrant_acquire[2], COMMAND_TYPE_LOCK);
            assert_eq!(request_count(&reentrant_acquire), 0);
            assert_eq!(reentrant_acquire[63], 0xff);
            write_lock_success(&mut stream, &reentrant_acquire).await;

            let reentrant_release = read_frame(&mut stream).await;
            assert_eq!(reentrant_release[2], COMMAND_TYPE_UNLOCK);
            assert_eq!(request_count(&reentrant_release), 0);
            assert_eq!(reentrant_release[63], 0xff);
            write_lock_success(&mut stream, &reentrant_release).await;

            let semaphore_acquire = read_frame(&mut stream).await;
            assert_eq!(semaphore_acquire[2], COMMAND_TYPE_LOCK);
            assert_eq!(request_count(&semaphore_acquire), 2);
            write_lock_success(&mut stream, &semaphore_acquire).await;

            let semaphore_release = read_frame(&mut stream).await;
            assert_eq!(semaphore_release[2], COMMAND_TYPE_UNLOCK);
            assert_eq!(
                semaphore_release[19],
                UNLOCK_FLAG_UNLOCK_FIRST_LOCK_WHEN_UNLOCKED
            );
            assert_eq!(request_lock_id(&semaphore_release), [0u8; 16]);
            assert_eq!(request_count(&semaphore_release), 2);
            write_lock_success(&mut stream, &semaphore_release).await;

            let write_acquire = read_frame(&mut stream).await;
            assert_eq!(write_acquire[2], COMMAND_TYPE_LOCK);
            assert_eq!(request_count(&write_acquire), 0);
            let write_lock_id = request_lock_id(&write_acquire);
            write_lock_success(&mut stream, &write_acquire).await;

            let write_release = read_frame(&mut stream).await;
            assert_eq!(write_release[2], COMMAND_TYPE_UNLOCK);
            assert_eq!(request_lock_id(&write_release), write_lock_id);
            assert_eq!(request_count(&write_release), 0);
            write_lock_success(&mut stream, &write_release).await;

            let read_acquire = read_frame(&mut stream).await;
            assert_eq!(read_acquire[2], COMMAND_TYPE_LOCK);
            assert_eq!(request_count(&read_acquire), u16::MAX);
            let read_lock_id = request_lock_id(&read_acquire);
            write_lock_success(&mut stream, &read_acquire).await;

            let read_release = read_frame(&mut stream).await;
            assert_eq!(read_release[2], COMMAND_TYPE_UNLOCK);
            assert_eq!(request_lock_id(&read_release), read_lock_id);
            assert_eq!(request_count(&read_release), u16::MAX);
            write_lock_success(&mut stream, &read_release).await;

            let priority_acquire = read_frame(&mut stream).await;
            assert_eq!(priority_acquire[2], COMMAND_TYPE_LOCK);
            assert_eq!(
                request_timeout_bits(&priority_acquire),
                PackedTime::with_flags(5, TIMEOUT_FLAG_RCOUNT_IS_PRIORITY).bits()
            );
            assert_eq!(priority_acquire[63], 7);
            write_lock_success(&mut stream, &priority_acquire).await;

            let priority_release = read_frame(&mut stream).await;
            assert_eq!(priority_release[2], COMMAND_TYPE_UNLOCK);
            assert_eq!(priority_release[63], 7);
            write_lock_success(&mut stream, &priority_release).await;

            let max_flow_acquire = read_frame(&mut stream).await;
            assert_eq!(max_flow_acquire[2], COMMAND_TYPE_LOCK);
            assert_eq!(request_count(&max_flow_acquire), 4);
            write_lock_success(&mut stream, &max_flow_acquire).await;

            let max_flow_release = read_frame(&mut stream).await;
            assert_eq!(max_flow_release[2], COMMAND_TYPE_UNLOCK);
            assert_eq!(request_count(&max_flow_release), 4);
            write_lock_success(&mut stream, &max_flow_release).await;

            let token_flow_acquire = read_frame(&mut stream).await;
            assert_eq!(token_flow_acquire[2], COMMAND_TYPE_LOCK);
            assert_eq!(request_count(&token_flow_acquire), 2);
            assert_eq!(
                request_expired_bits(&token_flow_acquire),
                PackedTime::with_flags(1250, EXPRIED_FLAG_MILLISECOND_TIME).bits()
            );
            write_lock_success(&mut stream, &token_flow_acquire).await;

            let tree_acquire = read_frame(&mut stream).await;
            assert_eq!(tree_acquire[2], COMMAND_TYPE_LOCK);
            assert_eq!(tree_acquire[19], LOCK_FLAG_LOCK_TREE_LOCK);
            assert_eq!(request_count(&tree_acquire), u16::MAX);
            assert_eq!(tree_acquire[63], 1);
            assert_eq!(
                request_timeout_bits(&tree_acquire),
                PackedTime::with_flags(5, TIMEOUT_FLAG_RCOUNT_IS_PRIORITY).bits()
            );
            write_lock_success(&mut stream, &tree_acquire).await;

            let tree_release = read_frame(&mut stream).await;
            assert_eq!(tree_release[2], COMMAND_TYPE_UNLOCK);
            assert_eq!(tree_release[19], UNLOCK_FLAG_UNLOCK_TREE_LOCK);
            assert_eq!(request_count(&tree_release), u16::MAX);
            assert_eq!(tree_release[63], 1);
            write_lock_success(&mut stream, &tree_release).await;

            let leaf_acquire = read_frame(&mut stream).await;
            assert_eq!(leaf_acquire[2], COMMAND_TYPE_LOCK);
            assert_eq!(leaf_acquire[19], 0);
            assert_eq!(request_count(&leaf_acquire), u16::MAX);
            assert_eq!(leaf_acquire[63], 1);
            write_lock_success(&mut stream, &leaf_acquire).await;

            let leaf_release = read_frame(&mut stream).await;
            assert_eq!(leaf_release[2], COMMAND_TYPE_UNLOCK);
            assert_eq!(leaf_release[19], UNLOCK_FLAG_UNLOCK_TREE_LOCK);
            assert_eq!(request_count(&leaf_release), u16::MAX);
            assert_eq!(leaf_release[63], 1);
            write_lock_success(&mut stream, &leaf_release).await;
        })
        .await;

        let client = Client::connect(address).await.unwrap();
        let db = client.select_database(2);

        let mut reentrant = db.reentrant_lock("async-reentrant-state", 5, 6);
        reentrant.acquire().await.unwrap();
        reentrant.release().await.unwrap();

        let mut semaphore = db.semaphore("async-semaphore-state", 3, 5, 6);
        semaphore.acquire().await.unwrap();
        semaphore.release().await.unwrap();

        let mut rw = db.read_write_lock("async-rw-state", 5, 6);
        assert!(matches!(
            rw.release_read().await.unwrap_err(),
            SlockError::LockData(_)
        ));
        rw.acquire_write().await.unwrap();
        rw.release_write().await.unwrap();
        rw.acquire_read().await.unwrap();
        rw.release_read().await.unwrap();

        let mut priority = db.priority_lock("async-priority-state", 7, 5, 6);
        priority.acquire().await.unwrap();
        priority.release().await.unwrap();

        let mut max_flow = db.max_concurrent_flow("async-max-flow-state", 5, 5, 6);
        max_flow.acquire().await.unwrap();
        max_flow.release().await.unwrap();

        let mut token_flow = db.token_bucket_flow("async-token-flow-state", 3, 5, 1.25);
        token_flow.acquire().await.unwrap();

        let mut tree = db.tree_lock("async-tree-state", 5, 6);
        tree.acquire().await.unwrap();
        tree.release().await.unwrap();
        let mut leaf = tree.new_leaf_lock();
        leaf.acquire().await.unwrap();
        leaf.release().await.unwrap();
    }

    #[tokio::test]
    async fn group_event_updates_version_from_wait_and_wakeup_results() {
        let expected_initial_id = group_lock_id(11, 2);
        let wait_result_id = group_lock_id(11, 3);
        let wakeup_result_id = group_lock_id(0, 4);
        let address = start_server(move |mut stream| async move {
            let init = read_frame(&mut stream).await;
            stream.write_all(&init_response(&init)).await.unwrap();

            let wait = read_frame(&mut stream).await;
            assert_eq!(wait[2], COMMAND_TYPE_LOCK);
            assert_eq!(wait[19], 0);
            assert_eq!(request_lock_id(&wait), expected_initial_id);
            assert_eq!(
                request_timeout_bits(&wait),
                PackedTime::with_flags(6, TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED).bits()
            );
            let data = [LOCK_DATA_COMMAND_TYPE_SET, 0, b'g', b'r', b'o', b'u', b'p'];
            let wait_response = lock_response_with_lock_id(
                &wait,
                COMMAND_RESULT_SUCCED,
                Some(&data),
                wait_result_id,
            );
            stream.write_all(&wait_response).await.unwrap();
            stream
                .write_all(&(data.len() as u32).to_le_bytes())
                .await
                .unwrap();
            stream.write_all(&data).await.unwrap();

            let wakeup = read_frame(&mut stream).await;
            assert_eq!(wakeup[2], COMMAND_TYPE_UNLOCK);
            assert_eq!(
                wakeup[19],
                UNLOCK_FLAG_UNLOCK_FIRST_LOCK_WHEN_UNLOCKED
                    | UNLOCK_FLAG_SUCCED_TO_LOCK_WAIT
                    | UNLOCK_FLAG_CONTAINS_DATA
            );
            assert_eq!(request_lock_id(&wakeup), [0u8; 16]);
            assert!(read_extra_if_present(&mut stream, &wakeup).await.is_some());
            let wakeup_response =
                lock_response_with_lock_id(&wakeup, COMMAND_RESULT_SUCCED, None, wakeup_result_id);
            stream.write_all(&wakeup_response).await.unwrap();
        })
        .await;

        let client = Client::connect(address).await.unwrap();
        let db = client.select_database(3);
        let mut group = db.group_event("async-group-state", 11, 2, 4, 5);
        group.wait(6).await.unwrap();
        assert_eq!(group.version_id(), 3);
        assert_eq!(group.current_data().unwrap().as_string().unwrap(), "group");
        group.wakeup(Some(LockData::set("next"))).await.unwrap();
        assert_eq!(group.version_id(), 4);
    }
}

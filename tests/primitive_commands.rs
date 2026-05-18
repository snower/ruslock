#![cfg(any(feature = "blocking", feature = "aio"))]

use ruslock::protocol::constants::*;

fn init_response(request: &[u8; 64]) -> [u8; 64] {
    let mut response = [0u8; 64];
    response[0] = MAGIC;
    response[1] = VERSION;
    response[2] = COMMAND_TYPE_INIT;
    response[3..19].copy_from_slice(&request[3..19]);
    response[19] = COMMAND_RESULT_SUCCED;
    response
}

fn lock_response(request: &[u8; 64]) -> [u8; 64] {
    let mut response = [0u8; 64];
    response[0] = MAGIC;
    response[1] = VERSION;
    response[2] = request[2];
    response[3..19].copy_from_slice(&request[3..19]);
    response[19] = COMMAND_RESULT_SUCCED;
    response[21] = request[20];
    response[22..38].copy_from_slice(&request[21..37]);
    response[38..54].copy_from_slice(&request[37..53]);
    response
}

fn request_count(request: &[u8; 64]) -> u16 {
    u16::from_le_bytes([request[61], request[62]])
}

fn request_timeout_flags(request: &[u8; 64]) -> u16 {
    (u32::from_le_bytes([request[53], request[54], request[55], request[56]]) >> 16) as u16
}

fn request_expired_bits(request: &[u8; 64]) -> u32 {
    u32::from_le_bytes([request[57], request[58], request[59], request[60]])
}

#[cfg(feature = "blocking")]
mod blocking {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    use ruslock::blocking::Client;

    fn assert_lock_request<A, F>(action: A, assertion: F)
    where
        A: FnOnce(Client),
        F: FnOnce(&[u8; 64]) + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut init = [0u8; 64];
            stream.read_exact(&mut init).unwrap();
            stream.write_all(&init_response(&init)).unwrap();

            let mut request = [0u8; 64];
            stream.read_exact(&mut request).unwrap();
            assertion(&request);
            stream.write_all(&lock_response(&request)).unwrap();
        });

        let client = Client::connect(address).unwrap();
        action(client);
    }

    #[test]
    fn blocking_core_primitives_build_expected_lock_headers() {
        assert_lock_request(
            |client| {
                let mut event = client.event("primitive-event", 1, 2, false);
                event.set().unwrap();
            },
            |request| {
                assert_eq!(request[2], COMMAND_TYPE_LOCK);
                assert_eq!(
                    request[19] & LOCK_FLAG_UPDATE_WHEN_LOCKED,
                    LOCK_FLAG_UPDATE_WHEN_LOCKED
                );
                assert_eq!(request_count(request), 1);
            },
        );

        assert_lock_request(
            |client| {
                let mut group = client.group_event(
                    "primitive-group",
                    0x1122_3344_5566_7788,
                    0x99aa_bbcc_ddee_ff00,
                    1,
                    2,
                );
                group.wait(3).unwrap();
            },
            |request| {
                assert_eq!(request[2], COMMAND_TYPE_LOCK);
                assert_eq!(
                    request_timeout_flags(request) & TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED,
                    TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED
                );
                assert_eq!(&request[21..29], &0x99aa_bbcc_ddee_ff00u64.to_le_bytes());
                assert_eq!(&request[29..37], &0x1122_3344_5566_7788u64.to_le_bytes());
            },
        );

        assert_lock_request(
            |client| {
                let mut semaphore = client.semaphore("primitive-sem", 10, 1, 2);
                semaphore.acquire().unwrap();
            },
            |request| {
                assert_eq!(request[2], COMMAND_TYPE_LOCK);
                assert_eq!(request_count(request), 9);
            },
        );

        assert_lock_request(
            |client| {
                let mut reentrant = client.reentrant_lock("primitive-reentrant", 1, 2);
                reentrant.acquire().unwrap();
            },
            |request| assert_eq!(request[63], 0xff),
        );

        assert_lock_request(
            |client| {
                let mut rw = client.read_write_lock("primitive-rw-write", 1, 2);
                rw.acquire_write().unwrap();
            },
            |request| assert_eq!(request_count(request), 0),
        );

        assert_lock_request(
            |client| {
                let mut rw = client.read_write_lock("primitive-rw-read", 1, 2);
                rw.acquire_read().unwrap();
            },
            |request| assert_eq!(request_count(request), u16::MAX),
        );

        assert_lock_request(
            |client| {
                let mut priority = client.priority_lock("primitive-priority", 7, 1, 2);
                priority.acquire().unwrap();
            },
            |request| {
                assert_eq!(request[63], 7);
                assert_eq!(
                    request_timeout_flags(request) & TIMEOUT_FLAG_RCOUNT_IS_PRIORITY,
                    TIMEOUT_FLAG_RCOUNT_IS_PRIORITY
                );
            },
        );

        assert_lock_request(
            |client| {
                let mut flow = client.max_concurrent_flow("primitive-flow", 3, 1, 2);
                flow.acquire().unwrap();
            },
            |request| assert_eq!(request_count(request), 2),
        );

        assert_lock_request(
            |client| {
                let mut flow = client.token_bucket_flow("primitive-token", 4, 1, 0.25);
                flow.acquire().unwrap();
            },
            |request| {
                let expired = request_expired_bits(request);
                assert_eq!(expired & 0xffff, 250);
                assert_eq!(
                    ((expired >> 16) as u16) & EXPRIED_FLAG_MILLISECOND_TIME,
                    EXPRIED_FLAG_MILLISECOND_TIME
                );
                assert_eq!(request_count(request), 3);
            },
        );

        assert_lock_request(
            |client| {
                let mut tree = client.tree_lock("primitive-tree", 1, 2);
                tree.acquire().unwrap();
            },
            |request| {
                assert_eq!(
                    request[19] & LOCK_FLAG_LOCK_TREE_LOCK,
                    LOCK_FLAG_LOCK_TREE_LOCK
                );
                assert_eq!(request_count(request), u16::MAX);
                assert_eq!(request[63], 1);
            },
        );
    }
}

#[cfg(feature = "aio")]
mod aio {
    use super::*;
    use std::future::Future;
    use std::time::Duration;

    use ruslock::aio::Client;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    async fn assert_lock_request<A, Fut, F>(action: A, assertion: F)
    where
        A: FnOnce(Client) -> Fut,
        Fut: Future<Output = ()>,
        F: FnOnce(&[u8; 64]) + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut init = [0u8; 64];
            stream.read_exact(&mut init).await.unwrap();
            stream.write_all(&init_response(&init)).await.unwrap();

            let mut request = [0u8; 64];
            stream.read_exact(&mut request).await.unwrap();
            assertion(&request);
            stream.write_all(&lock_response(&request)).await.unwrap();
            tokio::time::sleep(Duration::from_millis(5)).await;
        });

        let client = Client::connect(address).await.unwrap();
        action(client).await;
    }

    #[tokio::test]
    async fn async_core_primitives_build_expected_lock_headers() {
        assert_lock_request(
            |client| async move {
                let mut event = client.event("primitive-event", 1, 2, false);
                event.set().await.unwrap();
            },
            |request| {
                assert_eq!(request[2], COMMAND_TYPE_LOCK);
                assert_eq!(
                    request[19] & LOCK_FLAG_UPDATE_WHEN_LOCKED,
                    LOCK_FLAG_UPDATE_WHEN_LOCKED
                );
                assert_eq!(request_count(request), 1);
            },
        )
        .await;

        assert_lock_request(
            |client| async move {
                let mut group = client.group_event(
                    "primitive-group",
                    0x1122_3344_5566_7788,
                    0x99aa_bbcc_ddee_ff00,
                    1,
                    2,
                );
                group.wait(3).await.unwrap();
            },
            |request| {
                assert_eq!(request[2], COMMAND_TYPE_LOCK);
                assert_eq!(
                    request_timeout_flags(request) & TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED,
                    TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED
                );
                assert_eq!(&request[21..29], &0x99aa_bbcc_ddee_ff00u64.to_le_bytes());
                assert_eq!(&request[29..37], &0x1122_3344_5566_7788u64.to_le_bytes());
            },
        )
        .await;

        assert_lock_request(
            |client| async move {
                let mut semaphore = client.semaphore("primitive-sem", 10, 1, 2);
                semaphore.acquire().await.unwrap();
            },
            |request| {
                assert_eq!(request[2], COMMAND_TYPE_LOCK);
                assert_eq!(request_count(request), 9);
            },
        )
        .await;

        assert_lock_request(
            |client| async move {
                let mut reentrant = client.reentrant_lock("primitive-reentrant", 1, 2);
                reentrant.acquire().await.unwrap();
            },
            |request| assert_eq!(request[63], 0xff),
        )
        .await;

        assert_lock_request(
            |client| async move {
                let mut rw = client.read_write_lock("primitive-rw-write", 1, 2);
                rw.acquire_write().await.unwrap();
            },
            |request| assert_eq!(request_count(request), 0),
        )
        .await;

        assert_lock_request(
            |client| async move {
                let mut rw = client.read_write_lock("primitive-rw-read", 1, 2);
                rw.acquire_read().await.unwrap();
            },
            |request| assert_eq!(request_count(request), u16::MAX),
        )
        .await;

        assert_lock_request(
            |client| async move {
                let mut priority = client.priority_lock("primitive-priority", 7, 1, 2);
                priority.acquire().await.unwrap();
            },
            |request| {
                assert_eq!(request[63], 7);
                assert_eq!(
                    request_timeout_flags(request) & TIMEOUT_FLAG_RCOUNT_IS_PRIORITY,
                    TIMEOUT_FLAG_RCOUNT_IS_PRIORITY
                );
            },
        )
        .await;

        assert_lock_request(
            |client| async move {
                let mut flow = client.max_concurrent_flow("primitive-flow", 3, 1, 2);
                flow.acquire().await.unwrap();
            },
            |request| assert_eq!(request_count(request), 2),
        )
        .await;

        assert_lock_request(
            |client| async move {
                let mut flow = client.token_bucket_flow("primitive-token", 4, 1, 0.25);
                flow.acquire().await.unwrap();
            },
            |request| {
                let expired = request_expired_bits(request);
                assert_eq!(expired & 0xffff, 250);
                assert_eq!(
                    ((expired >> 16) as u16) & EXPRIED_FLAG_MILLISECOND_TIME,
                    EXPRIED_FLAG_MILLISECOND_TIME
                );
                assert_eq!(request_count(request), 3);
            },
        )
        .await;

        assert_lock_request(
            |client| async move {
                let mut tree = client.tree_lock("primitive-tree", 1, 2);
                tree.acquire().await.unwrap();
            },
            |request| {
                assert_eq!(
                    request[19] & LOCK_FLAG_LOCK_TREE_LOCK,
                    LOCK_FLAG_LOCK_TREE_LOCK
                );
                assert_eq!(request_count(request), u16::MAX);
                assert_eq!(request[63], 1);
            },
        )
        .await;
    }
}

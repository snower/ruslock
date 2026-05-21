use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;

use crate::callback::client::{Client, RequestHandle};
use crate::callback::primitives::{
    Event, GroupEvent, Lock, MaxConcurrentFlow, PriorityLock, ReadWriteLock, ReentrantLock,
    Semaphore, TokenBucketFlow, TreeLock,
};
use crate::error::Result;
use crate::protocol::command::Command;
use crate::protocol::result::CommandResult;
use crate::time::PackedTime;

#[derive(Clone, Debug)]
pub struct Database {
    client: Client,
    db_id: u8,
    default_timeout_flags: Arc<AtomicU16>,
    default_expired_flags: Arc<AtomicU16>,
}

impl Database {
    pub(crate) fn new(client: Client, db_id: u8) -> Self {
        Self {
            client,
            db_id,
            default_timeout_flags: Arc::new(AtomicU16::new(0)),
            default_expired_flags: Arc::new(AtomicU16::new(0)),
        }
    }

    pub fn db_id(&self) -> u8 {
        self.db_id
    }

    pub fn set_default_timeout_flags(&self, flags: u16) {
        self.default_timeout_flags.store(flags, Ordering::SeqCst);
    }

    pub fn set_default_expired_flags(&self, flags: u16) {
        self.default_expired_flags.store(flags, Ordering::SeqCst);
    }

    pub fn timeout(&self, value: u16) -> PackedTime {
        PackedTime::with_flags(value, self.default_timeout_flags.load(Ordering::SeqCst))
    }

    pub fn expired(&self, value: u16) -> PackedTime {
        PackedTime::with_flags(value, self.default_expired_flags.load(Ordering::SeqCst))
    }

    pub(crate) fn send_command_callback<F>(
        &self,
        command: Command,
        callback: F,
    ) -> Result<RequestHandle>
    where
        F: FnOnce(Result<CommandResult>) + Send + 'static,
    {
        self.client.send_command_callback(command, callback)
    }

    pub fn lock<K: AsRef<[u8]>>(&self, key: K, timeout: u16, expired: u16) -> Lock {
        Lock::new(self.clone(), key, timeout, expired)
    }

    pub fn event<K: AsRef<[u8]>>(
        &self,
        key: K,
        timeout: u16,
        expired: u16,
        default_set: bool,
    ) -> Event {
        Event::new(self.clone(), key, timeout, expired, default_set)
    }

    pub fn group_event<K: AsRef<[u8]>>(
        &self,
        key: K,
        client_id: u64,
        version_id: u64,
        timeout: u16,
        expired: u16,
    ) -> GroupEvent {
        GroupEvent::new(self.clone(), key, client_id, version_id, timeout, expired)
    }

    pub fn semaphore<K: AsRef<[u8]>>(
        &self,
        key: K,
        count: u16,
        timeout: u16,
        expired: u16,
    ) -> Semaphore {
        Semaphore::new(self.clone(), key, count, timeout, expired)
    }

    pub fn reentrant_lock<K: AsRef<[u8]>>(
        &self,
        key: K,
        timeout: u16,
        expired: u16,
    ) -> ReentrantLock {
        ReentrantLock::new(self.clone(), key, timeout, expired)
    }

    pub fn read_write_lock<K: AsRef<[u8]>>(
        &self,
        key: K,
        timeout: u16,
        expired: u16,
    ) -> ReadWriteLock {
        ReadWriteLock::new(self.clone(), key, timeout, expired)
    }

    pub fn priority_lock<K: AsRef<[u8]>>(
        &self,
        key: K,
        priority: u8,
        timeout: u16,
        expired: u16,
    ) -> PriorityLock {
        PriorityLock::new(self.clone(), key, priority, timeout, expired)
    }

    pub fn max_concurrent_flow<K: AsRef<[u8]>>(
        &self,
        key: K,
        max: u16,
        timeout: u16,
        expired: u16,
    ) -> MaxConcurrentFlow {
        MaxConcurrentFlow::new(self.clone(), key, max, timeout, expired)
    }

    pub fn token_bucket_flow<K: AsRef<[u8]>>(
        &self,
        key: K,
        count: u16,
        timeout: u16,
        period: f64,
    ) -> TokenBucketFlow {
        TokenBucketFlow::new(self.clone(), key, count, timeout, period)
    }

    pub fn tree_lock<K: AsRef<[u8]>>(&self, key: K, timeout: u16, expired: u16) -> TreeLock {
        TreeLock::new(self.clone(), key, timeout, expired)
    }

    pub(crate) fn client(&self) -> Client {
        self.client.clone()
    }
}

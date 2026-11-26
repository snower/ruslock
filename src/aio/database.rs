use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;

use crate::aio::client::Client;
use crate::aio::primitives::{
    Event, GroupEvent, Lock, MaxConcurrentFlow, PriorityLock, ReadWriteLock, ReentrantLock,
    Semaphore, TokenBucketFlow, TreeLock,
};
use crate::aio::replset::ReplsetClient;
use crate::error::Result;
use crate::protocol::command::Command;
use crate::protocol::id::Id16;
use crate::protocol::result::CommandResult;
use crate::time::PackedTime;

#[derive(Clone, Debug)]
pub(crate) enum ClientBackend {
    Single(Client),
    Replset(ReplsetClient),
}

impl ClientBackend {
    pub(crate) async fn send_command(&self, command: Command) -> Result<CommandResult> {
        match self {
            Self::Single(client) => client.send_command(command).await,
            Self::Replset(client) => client.send_command(command).await,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Database {
    client: ClientBackend,
    db_id: u8,
    default_timeout_flags: Arc<AtomicU16>,
    default_expired_flags: Arc<AtomicU16>,
}

impl Database {
    pub(crate) fn new(client: ClientBackend, db_id: u8) -> Self {
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

    /// Merges the database default timeout flags into a caller packed value
    /// without truncating the high 16 bits.
    pub fn timeout_packed(&self, timeout: PackedTime) -> PackedTime {
        timeout.merge_flags(self.default_timeout_flags.load(Ordering::SeqCst))
    }

    /// Merges the database default expired flags into a caller packed value
    /// without truncating the high 16 bits.
    pub fn expired_packed(&self, expired: PackedTime) -> PackedTime {
        expired.merge_flags(self.default_expired_flags.load(Ordering::SeqCst))
    }

    /// Creates a lock from caller-provided packed timeout/expired values.
    pub fn lock_packed<K: AsRef<[u8]>>(
        &self,
        key: K,
        timeout: PackedTime,
        expired: PackedTime,
    ) -> Lock {
        Lock::with_packed_times(
            self.clone(),
            key,
            Id16::new(),
            self.timeout_packed(timeout),
            self.expired_packed(expired),
            0,
            0,
        )
    }

    /// Creates an event from caller-provided packed timeout/expired values.
    pub fn event_packed<K: AsRef<[u8]>>(
        &self,
        key: K,
        timeout: PackedTime,
        expired: PackedTime,
        default_set: bool,
    ) -> Event {
        Event::with_packed_times(
            self.clone(),
            key,
            self.timeout_packed(timeout),
            self.expired_packed(expired),
            default_set,
        )
    }

    pub(crate) async fn send_command(&self, command: Command) -> Result<CommandResult> {
        self.client.send_command(command).await
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

    /// Creates a child tree lock with an explicit parent key.
    pub fn tree_lock_with_parent<K: AsRef<[u8]>, P: AsRef<[u8]>>(
        &self,
        parent: P,
        key: K,
        timeout: u16,
        expired: u16,
    ) -> TreeLock {
        TreeLock::with_packed_times(
            self.clone(),
            key,
            Some(crate::key::Key16::new(parent)),
            self.timeout(timeout),
            self.expired(expired),
        )
    }

    /// Creates a semaphore from caller-provided packed timeout/expired values.
    pub fn semaphore_packed<K: AsRef<[u8]>>(
        &self,
        key: K,
        count: u16,
        timeout: PackedTime,
        expired: PackedTime,
    ) -> Semaphore {
        Semaphore::with_packed_times(
            self.clone(),
            key,
            count,
            self.timeout_packed(timeout),
            self.expired_packed(expired),
        )
    }

    /// Creates a reentrant lock from caller-provided packed values.
    pub fn reentrant_lock_packed<K: AsRef<[u8]>>(
        &self,
        key: K,
        timeout: PackedTime,
        expired: PackedTime,
    ) -> ReentrantLock {
        ReentrantLock::with_packed_times(
            self.clone(),
            key,
            Id16::new(),
            self.timeout_packed(timeout),
            self.expired_packed(expired),
        )
    }

    /// Creates a read/write lock from caller-provided packed values.
    pub fn read_write_lock_packed<K: AsRef<[u8]>>(
        &self,
        key: K,
        timeout: PackedTime,
        expired: PackedTime,
    ) -> ReadWriteLock {
        ReadWriteLock::with_packed_times(
            self.clone(),
            key,
            self.timeout_packed(timeout),
            self.expired_packed(expired),
        )
    }

    /// Creates a priority lock from caller-provided packed values and the raw
    /// Java `count` value (decremented by the primitive).
    pub fn priority_lock_packed<K: AsRef<[u8]>>(
        &self,
        key: K,
        priority: u8,
        count: u16,
        timeout: PackedTime,
        expired: PackedTime,
    ) -> PriorityLock {
        PriorityLock::with_packed_times(
            self.clone(),
            key,
            Id16::new(),
            priority,
            count,
            self.timeout_packed(timeout),
            self.expired_packed(expired),
        )
    }

    /// Creates a max-concurrency flow from caller-provided packed values.
    pub fn max_concurrent_flow_packed<K: AsRef<[u8]>>(
        &self,
        key: K,
        max: u16,
        priority: u8,
        timeout: PackedTime,
        expired: PackedTime,
    ) -> MaxConcurrentFlow {
        MaxConcurrentFlow::with_packed_times(
            self.clone(),
            key,
            Id16::new(),
            max,
            priority,
            self.timeout_packed(timeout),
            self.expired_packed(expired),
        )
    }

    /// Creates a token bucket flow. As in the Java facade, only the timeout
    /// database flags are merged; the expired value is derived from `period`.
    pub fn token_bucket_flow_packed<K: AsRef<[u8]>>(
        &self,
        key: K,
        count: u16,
        timeout: PackedTime,
        period: f64,
        priority: u8,
    ) -> TokenBucketFlow {
        TokenBucketFlow::with_packed_times(
            self.clone(),
            key,
            count,
            self.timeout_packed(timeout),
            period,
            priority,
        )
    }

    /// Creates a group event from caller-provided packed values.
    pub fn group_event_packed<K: AsRef<[u8]>>(
        &self,
        key: K,
        client_id: u64,
        version_id: u64,
        timeout: PackedTime,
        expired: PackedTime,
    ) -> GroupEvent {
        GroupEvent::with_packed_times(
            self.clone(),
            key,
            client_id,
            version_id,
            self.timeout_packed(timeout),
            self.expired_packed(expired),
        )
    }

    /// Creates a root tree lock from caller-provided packed values.
    pub fn tree_lock_packed<K: AsRef<[u8]>>(
        &self,
        key: K,
        timeout: PackedTime,
        expired: PackedTime,
    ) -> TreeLock {
        TreeLock::with_packed_times(
            self.clone(),
            key,
            None,
            self.timeout_packed(timeout),
            self.expired_packed(expired),
        )
    }

    /// Creates a child tree lock with an explicit parent key and packed values.
    pub fn tree_lock_with_parent_packed<K: AsRef<[u8]>, P: AsRef<[u8]>>(
        &self,
        parent: P,
        key: K,
        timeout: PackedTime,
        expired: PackedTime,
    ) -> TreeLock {
        TreeLock::with_packed_times(
            self.clone(),
            key,
            Some(crate::key::Key16::new(parent)),
            self.timeout_packed(timeout),
            self.expired_packed(expired),
        )
    }
}

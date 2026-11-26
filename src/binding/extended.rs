//! Typed binding objects for every non-`Lock`/non-`Event` Rust primitive.
//!
//! Each object is cheap to clone and shares only the state that the Java
//! facade keeps on the object (group-event version, read-lock FIFO, lazy lock
//! identity, tree leaf identity). Every semantic operation performs one call
//! into the existing `aio` primitive; shared state mutexes are never held
//! across an `.await`, so a `wait()` on a group event cannot block a `wakeup()`
//! on the same handle and a read lock release cannot block a concurrent
//! acquire.

use std::sync::{Arc, Mutex, MutexGuard};

use crate::aio::{
    Database, GroupEvent, Lock, MaxConcurrentFlow, PriorityLock, ReadWriteLock, ReentrantLock,
    TreeLeafLock, TreeLock,
};
use crate::data::{LockData, LockResultData};
use crate::error::SlockError;
use crate::key::Key16;
use crate::protocol::constants::{EXPRIED_FLAG_MILLISECOND_TIME, TIMEOUT_FLAG_RCOUNT_IS_PRIORITY};
use crate::protocol::id::Id16;
use crate::protocol::result::LockCommandResult;
use crate::time::PackedTime;

use super::primitives::BindingCompletion;
use super::{read_timing, shared_timing, write_timing, TimingState};

fn state<T>(state: &Mutex<T>) -> MutexGuard<'_, T> {
    state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn merge_current_data(slot: &mut Option<LockResultData>, value: Option<&LockResultData>) {
    if let Some(value) = value {
        *slot = Some(value.clone());
    }
}

// ---------------------------------------------------------------------------
// Semaphore
// ---------------------------------------------------------------------------

/// Typed handle to `aio::Semaphore`.
///
/// The stored count is the *encoded command count* (Java
/// `AbstractExecution.count`), i.e. already `raw - 1`. Every acquire/release
/// sends it verbatim on a fresh `Lock`, exactly like the Java facade.
#[derive(Clone, Debug)]
pub struct BindingSemaphore {
    database: Database,
    key: Key16,
    count: Arc<Mutex<u16>>,
    timing: Arc<Mutex<TimingState>>,
}

impl BindingSemaphore {
    /// Creates a semaphore. `count` is the Java raw count; the stored count is
    /// `count - 1` (saturating), matching the Java constructor.
    pub fn new<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        count: u16,
        timeout: PackedTime,
        expired: PackedTime,
    ) -> Self {
        let timeout = database.timeout_packed(timeout);
        let expired = database.expired_packed(expired);
        Self {
            database,
            key: Key16::new(key),
            count: Arc::new(Mutex::new(count.saturating_sub(1))),
            timing: shared_timing(timeout, expired),
        }
    }

    pub fn db_id(&self) -> u8 {
        self.database.db_id()
    }

    pub fn lock_key(&self) -> Key16 {
        self.key
    }

    /// Returns the stored encoded command count (Java `getCount`).
    pub fn count(&self) -> u16 {
        *state(&self.count)
    }

    /// Sets the encoded command count used by every subsequent fresh acquire
    /// and release lock, matching Java `AbstractExecution.setCount(short)`.
    ///
    /// The value is stored verbatim: it is already the command count, so it is
    /// not decremented again (unlike the raw-count constructor).
    pub fn set_count(&self, count: u16) {
        *state(&self.count) = count;
    }

    pub fn timeout(&self) -> PackedTime {
        read_timing(&self.timing).timeout
    }

    pub fn expired(&self) -> PackedTime {
        read_timing(&self.timing).expired
    }

    /// Replaces the packed timeout used by the next submission. Database
    /// defaults are not re-merged and identity/state are preserved.
    pub fn set_timeout(&self, timeout: PackedTime) {
        let mut guard = self
            .timing
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.timeout = timeout;
    }

    /// Replaces the packed expired used by the next submission.
    pub fn set_expired(&self, expired: PackedTime) {
        let mut guard = self
            .timing
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.expired = expired;
    }

    /// Replaces both packed timing values atomically.
    pub fn set_timing(&self, timeout: PackedTime, expired: PackedTime) {
        write_timing(&self.timing, timeout, expired);
    }

    pub fn acquire(&self) -> BindingCompletion<LockCommandResult> {
        let timing = read_timing(&self.timing);
        let mut lock = Lock::with_packed_times(
            self.database.clone(),
            self.key.as_bytes(),
            Id16::new(),
            timing.timeout,
            timing.expired,
            *state(&self.count),
            0,
        );
        Box::pin(async move { lock.acquire().await })
    }

    pub fn release(&self) -> BindingCompletion<()> {
        let timing = read_timing(&self.timing);
        let mut lock = Lock::with_packed_times(
            self.database.clone(),
            self.key.as_bytes(),
            Id16::zero(),
            timing.timeout,
            timing.expired,
            *state(&self.count),
            0,
        );
        Box::pin(async move { lock.release_head(None).await })
    }
}

// ---------------------------------------------------------------------------
// ReentrantLock
// ---------------------------------------------------------------------------

/// Typed handle to `aio::ReentrantLock`.
#[derive(Clone, Debug)]
pub struct BindingReentrantLock {
    database: Database,
    key: Key16,
    lock_id: Id16,
    timing: Arc<Mutex<TimingState>>,
}

impl BindingReentrantLock {
    pub fn new<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        timeout: PackedTime,
        expired: PackedTime,
    ) -> Self {
        let timeout = database.timeout_packed(timeout);
        let expired = database.expired_packed(expired);
        Self {
            database,
            key: Key16::new(key),
            lock_id: Id16::new(),
            timing: shared_timing(timeout, expired),
        }
    }

    pub fn db_id(&self) -> u8 {
        self.database.db_id()
    }

    pub fn lock_key(&self) -> Key16 {
        self.key
    }

    pub fn lock_id(&self) -> Id16 {
        self.lock_id
    }

    pub fn timeout(&self) -> PackedTime {
        read_timing(&self.timing).timeout
    }

    pub fn expired(&self) -> PackedTime {
        read_timing(&self.timing).expired
    }

    /// Replaces the packed timeout used by the next submission. Database
    /// defaults are not re-merged and identity/state are preserved.
    pub fn set_timeout(&self, timeout: PackedTime) {
        let mut guard = self
            .timing
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.timeout = timeout;
    }

    /// Replaces the packed expired used by the next submission.
    pub fn set_expired(&self, expired: PackedTime) {
        let mut guard = self
            .timing
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.expired = expired;
    }

    /// Replaces both packed timing values atomically.
    pub fn set_timing(&self, timeout: PackedTime, expired: PackedTime) {
        write_timing(&self.timing, timeout, expired);
    }

    pub fn current_data(&self) -> Option<LockResultData> {
        None
    }

    fn snapshot(&self) -> ReentrantLock {
        ReentrantLock::with_packed_times(
            self.database.clone(),
            self.key.as_bytes(),
            self.lock_id,
            read_timing(&self.timing).timeout,
            read_timing(&self.timing).expired,
        )
    }

    pub fn acquire(&self) -> BindingCompletion<LockCommandResult> {
        let mut lock = self.snapshot();
        Box::pin(async move { lock.acquire().await })
    }

    pub fn release(&self) -> BindingCompletion<LockCommandResult> {
        let mut lock = self.snapshot();
        Box::pin(async move { lock.release().await })
    }
}

// ---------------------------------------------------------------------------
// ReadWriteLock
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
struct ReadLockState {
    lock_id: Id16,
    timing: TimingState,
}

#[derive(Debug, Default)]
struct ReadWriteState {
    read_locks: Vec<ReadLockState>,
    write_timing: Option<TimingState>,
}

/// Typed handle to `aio::ReadWriteLock`.
///
/// Read locks are released in acquisition (FIFO) order like the Java facade.
/// The read-lock identity queue is the only shared state and is never held
/// across an `.await`.
#[derive(Clone, Debug)]
pub struct BindingReadWriteLock {
    database: Database,
    key: Key16,
    timing: Arc<Mutex<TimingState>>,
    write_lock_id: Id16,
    state: Arc<Mutex<ReadWriteState>>,
}

impl BindingReadWriteLock {
    pub fn new<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        timeout: PackedTime,
        expired: PackedTime,
    ) -> Self {
        let timeout = database.timeout_packed(timeout);
        let expired = database.expired_packed(expired);
        Self {
            database,
            key: Key16::new(key),
            timing: shared_timing(timeout, expired),
            write_lock_id: Id16::new(),
            state: Arc::new(Mutex::new(ReadWriteState::default())),
        }
    }

    pub fn db_id(&self) -> u8 {
        self.database.db_id()
    }

    pub fn lock_key(&self) -> Key16 {
        self.key
    }

    pub fn timeout(&self) -> PackedTime {
        read_timing(&self.timing).timeout
    }

    pub fn expired(&self) -> PackedTime {
        read_timing(&self.timing).expired
    }

    /// Replaces the packed timeout used by the next submission. Database
    /// defaults are not re-merged and identity/state are preserved.
    pub fn set_timeout(&self, timeout: PackedTime) {
        let mut guard = self
            .timing
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.timeout = timeout;
    }

    /// Replaces the packed expired used by the next submission.
    pub fn set_expired(&self, expired: PackedTime) {
        let mut guard = self
            .timing
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.expired = expired;
    }

    /// Replaces both packed timing values atomically.
    pub fn set_timing(&self, timeout: PackedTime, expired: PackedTime) {
        write_timing(&self.timing, timeout, expired);
    }

    pub fn write_lock_id(&self) -> Id16 {
        self.write_lock_id
    }

    pub fn read_lock_count(&self) -> usize {
        state(&self.state).read_locks.len()
    }

    fn snapshot_with_timing(&self, timing: TimingState) -> ReadWriteLock {
        ReadWriteLock::with_lock_id_packed(
            self.database.clone(),
            self.key.as_bytes(),
            self.write_lock_id,
            timing.timeout,
            timing.expired,
        )
    }

    /// The write Lock is materialized once: its identity and timing are frozen
    /// on first read/write materialization and reused across release/reacquire.
    fn snapshot_write(&self) -> ReadWriteLock {
        let timing = {
            let mut guard = state(&self.state);
            match guard.write_timing {
                Some(timing) => timing,
                None => {
                    let timing = read_timing(&self.timing);
                    guard.write_timing = Some(timing);
                    timing
                }
            }
        };
        self.snapshot_with_timing(timing)
    }

    pub fn acquire_write(&self) -> BindingCompletion<LockCommandResult> {
        let mut lock = self.snapshot_write();
        Box::pin(async move { lock.acquire_write().await })
    }

    pub fn release_write(&self) -> BindingCompletion<LockCommandResult> {
        let mut lock = self.snapshot_write();
        Box::pin(async move { lock.release_write().await })
    }

    pub fn acquire_read(&self) -> BindingCompletion<LockCommandResult> {
        let timing = read_timing(&self.timing);
        let mut lock = self.snapshot_with_timing(timing);
        let shared = Arc::clone(&self.state);
        let lock_id = Id16::new();
        Box::pin(async move {
            let result = lock.acquire_read_with_id(lock_id).await?;
            state(&shared).read_locks.push(ReadLockState { lock_id, timing });
            Ok(result)
        })
    }

    pub fn release_read(&self) -> BindingCompletion<()> {
        let read = {
            let mut guard = state(&self.state);
            if guard.read_locks.is_empty() {
                None
            } else {
                Some(guard.read_locks.remove(0))
            }
        };
        let Some(read) = read else {
            return Box::pin(async { Ok(()) });
        };
        // Java releases the cached read Lock, so it keeps the timing captured
        // when that read was acquired.
        let mut lock = self.snapshot_with_timing(read.timing);
        Box::pin(async move { lock.release_read_with_id(read.lock_id).await.map(|_| ()) })
    }

    pub fn acquire(&self) -> BindingCompletion<LockCommandResult> {
        self.acquire_write()
    }

    pub fn release(&self) -> BindingCompletion<LockCommandResult> {
        self.release_write()
    }
}

// ---------------------------------------------------------------------------
// GroupEvent
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct GroupEventState {
    version_id: u64,
    current_data: Option<LockResultData>,
}

/// Typed handle to `aio::GroupEvent`.
///
/// The version is shared across clones and merged back only after an operation
/// completes. `wait`/`wakeup` do not hold the state mutex across the network
/// await, so a `wakeup` can proceed while a wait is in flight.
#[derive(Clone, Debug)]
pub struct BindingGroupEvent {
    database: Database,
    key: Key16,
    client_id: u64,
    timing: Arc<Mutex<TimingState>>,
    state: Arc<Mutex<GroupEventState>>,
}

impl BindingGroupEvent {
    pub fn new<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        client_id: u64,
        version_id: u64,
        timeout: PackedTime,
        expired: PackedTime,
    ) -> Self {
        let timeout = database.timeout_packed(timeout);
        let expired = database.expired_packed(expired);
        Self {
            database,
            key: Key16::new(key),
            client_id,
            timing: shared_timing(timeout, expired),
            state: Arc::new(Mutex::new(GroupEventState {
                version_id,
                current_data: None,
            })),
        }
    }

    pub fn db_id(&self) -> u8 {
        self.database.db_id()
    }

    pub fn lock_key(&self) -> Key16 {
        self.key
    }

    pub fn client_id(&self) -> u64 {
        self.client_id
    }

    pub fn version_id(&self) -> u64 {
        state(&self.state).version_id
    }

    pub fn timeout(&self) -> PackedTime {
        read_timing(&self.timing).timeout
    }

    pub fn expired(&self) -> PackedTime {
        read_timing(&self.timing).expired
    }

    /// Replaces the packed timeout used by the next submission. Database
    /// defaults are not re-merged and identity/state are preserved.
    pub fn set_timeout(&self, timeout: PackedTime) {
        let mut guard = self
            .timing
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.timeout = timeout;
    }

    /// Replaces the packed expired used by the next submission.
    pub fn set_expired(&self, expired: PackedTime) {
        let mut guard = self
            .timing
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.expired = expired;
    }

    /// Replaces both packed timing values atomically.
    pub fn set_timing(&self, timeout: PackedTime, expired: PackedTime) {
        write_timing(&self.timing, timeout, expired);
    }

    pub fn current_data(&self) -> Option<LockResultData> {
        state(&self.state).current_data.clone()
    }

    fn snapshot(&self) -> GroupEvent {
        let version_id = state(&self.state).version_id;
        GroupEvent::with_packed_times(
            self.database.clone(),
            self.key.as_bytes(),
            self.client_id,
            version_id,
            read_timing(&self.timing).timeout,
            read_timing(&self.timing).expired,
        )
    }

    pub fn is_set(&self) -> BindingCompletion<bool> {
        let mut event = self.snapshot();
        Box::pin(async move { event.is_set().await })
    }

    pub fn set(&self) -> BindingCompletion<()> {
        let mut event = self.snapshot();
        Box::pin(async move { event.set().await })
    }

    /// Sets the event with a payload. The facade's `current_data` is not
    /// updated, matching Java `GroupEvent.set(LockData)`.
    pub fn set_with_data(&self, data: LockData) -> BindingCompletion<()> {
        let mut event = self.snapshot();
        Box::pin(async move { event.set_with_data(data).await })
    }

    pub fn clear(&self) -> BindingCompletion<()> {
        let mut event = self.snapshot();
        Box::pin(async move { event.clear().await })
    }

    /// Clears the event with a payload (`update` + contains-data flag).
    pub fn clear_with_data(&self, data: LockData) -> BindingCompletion<()> {
        let mut event = self.snapshot();
        Box::pin(async move { event.clear_with_data(data).await })
    }

    pub fn wakeup(&self) -> BindingCompletion<()> {
        self.wakeup_with_data(None)
    }

    pub fn wakeup_with_data(&self, data: Option<LockData>) -> BindingCompletion<()> {
        let mut event = self.snapshot();
        let shared = Arc::clone(&self.state);
        Box::pin(async move {
            event.wakeup(data).await?;
            let mut guard = state(&shared);
            guard.version_id = guard.version_id.max(event.version_id());
            Ok(())
        })
    }

    /// Waits using the caller's raw `u16` timeout. Database timeout flags are
    /// not merged, matching the Java facade's `wait(int)` overload.
    pub fn wait(&self, timeout: u16) -> BindingCompletion<()> {
        self.wait_packed(PackedTime::new(timeout))
    }

    pub fn wait_packed(&self, timeout: PackedTime) -> BindingCompletion<()> {
        let mut event = self.snapshot();
        let shared = Arc::clone(&self.state);
        Box::pin(async move {
            event.wait_packed(timeout).await?;
            let mut guard = state(&shared);
            guard.version_id = guard.version_id.max(event.version_id());
            merge_current_data(&mut guard.current_data, event.current_data());
            Ok(())
        })
    }

    pub fn wait_and_timeout_retry_clear(&self, timeout: u16) -> BindingCompletion<()> {
        self.wait_and_timeout_retry_clear_packed(PackedTime::new(timeout))
    }

    pub fn wait_and_timeout_retry_clear_packed(
        &self,
        timeout: PackedTime,
    ) -> BindingCompletion<()> {
        let mut event = self.snapshot();
        let shared = Arc::clone(&self.state);
        Box::pin(async move {
            event.wait_and_timeout_retry_clear_packed(timeout).await?;
            let mut guard = state(&shared);
            guard.version_id = guard.version_id.max(event.version_id());
            merge_current_data(&mut guard.current_data, event.current_data());
            Ok(())
        })
    }
}

// ---------------------------------------------------------------------------
// PriorityLock
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct LazyLockState {
    priority: u8,
    cached: Option<(Id16, u8)>,
}

/// Typed handle to `aio::PriorityLock`.
///
/// The underlying lock is created lazily like the Java facade: `set_priority`
/// only affects the lock if it is called before the first operation.
#[derive(Clone, Debug)]
pub struct BindingPriorityLock {
    database: Database,
    key: Key16,
    count: u16,
    timing: Arc<Mutex<TimingState>>,
    state: Arc<Mutex<LazyLockState>>,
}

impl BindingPriorityLock {
    pub fn new<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        priority: u8,
        count: u16,
        timeout: PackedTime,
        expired: PackedTime,
    ) -> Self {
        let timeout = database.timeout_packed(timeout);
        let expired = database.expired_packed(expired);
        Self {
            database,
            key: Key16::new(key),
            count,
            timing: shared_timing(timeout, expired),
            state: Arc::new(Mutex::new(LazyLockState {
                priority,
                cached: None,
            })),
        }
    }

    pub fn db_id(&self) -> u8 {
        self.database.db_id()
    }

    pub fn lock_key(&self) -> Key16 {
        self.key
    }

    pub fn priority(&self) -> u8 {
        state(&self.state).priority
    }

    pub fn set_priority(&self, priority: u8) {
        state(&self.state).priority = priority;
    }

    pub fn lock_id(&self) -> Id16 {
        self.ensure_lock().0
    }

    pub fn timeout(&self) -> PackedTime {
        read_timing(&self.timing).timeout
    }

    pub fn expired(&self) -> PackedTime {
        read_timing(&self.timing).expired
    }

    /// Replaces the packed timeout used by the next submission. Database
    /// defaults are not re-merged and identity/state are preserved.
    pub fn set_timeout(&self, timeout: PackedTime) {
        let mut guard = self
            .timing
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.timeout = timeout;
    }

    /// Replaces the packed expired used by the next submission.
    pub fn set_expired(&self, expired: PackedTime) {
        let mut guard = self
            .timing
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.expired = expired;
    }

    /// Replaces both packed timing values atomically.
    pub fn set_timing(&self, timeout: PackedTime, expired: PackedTime) {
        write_timing(&self.timing, timeout, expired);
    }

    fn ensure_lock(&self) -> (Id16, u8) {
        let mut guard = state(&self.state);
        if let Some(cached) = guard.cached {
            cached
        } else {
            let cached = (Id16::new(), guard.priority);
            guard.cached = Some(cached);
            cached
        }
    }

    fn snapshot(&self) -> PriorityLock {
        let (lock_id, priority) = self.ensure_lock();
        PriorityLock::with_packed_times(
            self.database.clone(),
            self.key.as_bytes(),
            lock_id,
            priority,
            self.count,
            read_timing(&self.timing).timeout,
            read_timing(&self.timing).expired,
        )
    }

    pub fn acquire(&self) -> BindingCompletion<LockCommandResult> {
        let mut lock = self.snapshot();
        Box::pin(async move { lock.acquire().await })
    }

    pub fn release(&self) -> BindingCompletion<LockCommandResult> {
        let mut lock = self.snapshot();
        Box::pin(async move { lock.release().await })
    }
}

// ---------------------------------------------------------------------------
// MaxConcurrentFlow
// ---------------------------------------------------------------------------

/// Typed handle to `aio::MaxConcurrentFlow`.
#[derive(Clone, Debug)]
pub struct BindingMaxConcurrentFlow {
    database: Database,
    key: Key16,
    max: u16,
    timing: Arc<Mutex<TimingState>>,
    state: Arc<Mutex<LazyLockState>>,
}

impl BindingMaxConcurrentFlow {
    pub fn new<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        max: u16,
        priority: u8,
        timeout: PackedTime,
        expired: PackedTime,
    ) -> Self {
        let timeout = database.timeout_packed(timeout);
        let expired = database.expired_packed(expired);
        Self {
            database,
            key: Key16::new(key),
            max,
            timing: shared_timing(timeout, expired),
            state: Arc::new(Mutex::new(LazyLockState {
                priority,
                cached: None,
            })),
        }
    }

    pub fn db_id(&self) -> u8 {
        self.database.db_id()
    }

    pub fn lock_key(&self) -> Key16 {
        self.key
    }

    pub fn max(&self) -> u16 {
        self.max
    }

    pub fn priority(&self) -> u8 {
        state(&self.state).priority
    }

    pub fn set_priority(&self, priority: u8) {
        state(&self.state).priority = priority;
    }

    pub fn lock_id(&self) -> Id16 {
        self.ensure_lock().0
    }

    pub fn timeout(&self) -> PackedTime {
        read_timing(&self.timing).timeout
    }

    pub fn expired(&self) -> PackedTime {
        read_timing(&self.timing).expired
    }

    /// Replaces the packed timeout used by the next submission. Database
    /// defaults are not re-merged and identity/state are preserved.
    pub fn set_timeout(&self, timeout: PackedTime) {
        let mut guard = self
            .timing
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.timeout = timeout;
    }

    /// Replaces the packed expired used by the next submission.
    pub fn set_expired(&self, expired: PackedTime) {
        let mut guard = self
            .timing
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.expired = expired;
    }

    /// Replaces both packed timing values atomically.
    pub fn set_timing(&self, timeout: PackedTime, expired: PackedTime) {
        write_timing(&self.timing, timeout, expired);
    }

    fn ensure_lock(&self) -> (Id16, u8) {
        let mut guard = state(&self.state);
        if let Some(cached) = guard.cached {
            cached
        } else {
            let cached = (Id16::new(), guard.priority);
            guard.cached = Some(cached);
            cached
        }
    }

    fn snapshot(&self) -> MaxConcurrentFlow {
        let (lock_id, priority) = self.ensure_lock();
        MaxConcurrentFlow::with_packed_times(
            self.database.clone(),
            self.key.as_bytes(),
            lock_id,
            self.max,
            priority,
            read_timing(&self.timing).timeout,
            read_timing(&self.timing).expired,
        )
    }

    pub fn acquire(&self) -> BindingCompletion<LockCommandResult> {
        let mut flow = self.snapshot();
        Box::pin(async move { flow.acquire().await })
    }

    pub fn release(&self) -> BindingCompletion<LockCommandResult> {
        let mut flow = self.snapshot();
        Box::pin(async move { flow.release().await })
    }
}

// ---------------------------------------------------------------------------
// TokenBucketFlow
// ---------------------------------------------------------------------------

/// Typed handle to `aio::TokenBucketFlow`.
///
/// Each acquire creates a fresh lock, as the Java facade does. The stored count
/// is the encoded command count (`raw - 1`), sent verbatim on every fresh
/// acquire lock; only the priority is additional mutable state.
#[derive(Clone, Debug)]
pub struct BindingTokenBucketFlow {
    database: Database,
    key: Key16,
    count: Arc<Mutex<u16>>,
    timing: Arc<Mutex<TimingState>>,
    period: f64,
    priority: Arc<Mutex<u8>>,
}

impl BindingTokenBucketFlow {
    pub fn new<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        count: u16,
        timeout: PackedTime,
        period: f64,
        priority: u8,
    ) -> Self {
        let timeout = database.timeout_packed(timeout);
        Self {
            database,
            key: Key16::new(key),
            count: Arc::new(Mutex::new(count.saturating_sub(1))),
            timing: shared_timing(timeout, PackedTime::default()),
            period,
            priority: Arc::new(Mutex::new(priority)),
        }
    }

    pub fn db_id(&self) -> u8 {
        self.database.db_id()
    }

    pub fn lock_key(&self) -> Key16 {
        self.key
    }

    /// Returns the stored encoded command count (Java `getCount`).
    pub fn count(&self) -> u16 {
        *state(&self.count)
    }

    /// Sets the encoded command count used by every subsequent fresh acquire
    /// lock, matching Java `AbstractExecution.setCount(short)`. The value is
    /// stored verbatim (not decremented again).
    pub fn set_count(&self, count: u16) {
        *state(&self.count) = count;
    }

    pub fn period(&self) -> f64 {
        self.period
    }

    pub fn timeout(&self) -> PackedTime {
        read_timing(&self.timing).timeout
    }

    /// Stored for API parity; token-bucket acquires consume only `timeout`.
    pub fn expired(&self) -> PackedTime {
        read_timing(&self.timing).expired
    }

    /// Replaces the packed timeout used by the next submission. Database
    /// defaults are not re-merged and the token bucket count/period are kept.
    pub fn set_timeout(&self, timeout: PackedTime) {
        let mut guard = self
            .timing
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.timeout = timeout;
    }

    /// Replaces the stored packed expired value (unused by `acquire`).
    pub fn set_expired(&self, expired: PackedTime) {
        let mut guard = self
            .timing
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.expired = expired;
    }

    /// Replaces both packed timing values atomically.
    pub fn set_timing(&self, timeout: PackedTime, expired: PackedTime) {
        write_timing(&self.timing, timeout, expired);
    }

    pub fn priority(&self) -> u8 {
        *state(&self.priority)
    }

    pub fn set_priority(&self, priority: u8) {
        *state(&self.priority) = priority;
    }

    pub fn acquire(&self) -> BindingCompletion<LockCommandResult> {
        let priority = *state(&self.priority);
        let timeout = read_timing(&self.timing).timeout;
        let count = *state(&self.count);
        let period = self.period;
        let database = self.database.clone();
        let key = self.key;
        Box::pin(async move {
            let priority_timeout = if priority > 0 {
                timeout.merge_flags(TIMEOUT_FLAG_RCOUNT_IS_PRIORITY)
            } else {
                timeout
            };

            if period < 3.0 {
                let expired = PackedTime::with_flags(
                    ((period * 1000.0).ceil() as u16).max(1),
                    EXPRIED_FLAG_MILLISECOND_TIME,
                );
                let mut lock = Lock::with_packed_times(
                    database.clone(),
                    key.as_bytes(),
                    Id16::new(),
                    priority_timeout,
                    expired,
                    count,
                    0,
                );
                return lock.acquire().await;
            }

            let period_secs = period.ceil() as u64;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let remaining = if period_secs == 0 {
                0
            } else {
                period_secs - (now % period_secs)
            };
            let expired = PackedTime::new(remaining.min(u16::MAX as u64) as u16);
            let mut lock = Lock::with_packed_times(
                database.clone(),
                key.as_bytes(),
                Id16::new(),
                PackedTime::default(),
                expired,
                count,
                0,
            );
            match lock.acquire().await {
                Ok(result) => Ok(result),
                Err(SlockError::LockTimeout(_)) => {
                    let expired = PackedTime::new(period_secs.min(u16::MAX as u64) as u16);
                    let mut retry = Lock::with_packed_times(
                        database.clone(),
                        key.as_bytes(),
                        Id16::new(),
                        priority_timeout,
                        expired,
                        count,
                        0,
                    );
                    retry.acquire().await
                }
                Err(err) => Err(err),
            }
        })
    }
}

// ---------------------------------------------------------------------------
// TreeLock / TreeLeafLock
// ---------------------------------------------------------------------------

/// Typed handle to `aio::TreeLock`.
///
/// `acquire` mirrors the Java facade: it first runs the check-lock acquire,
/// then lazily takes and caches one leaf lock. `release` only releases a
/// previously cached leaf, so repeated acquires reuse the same leaf identity.
#[derive(Clone, Debug)]
pub struct BindingTreeLock {
    database: Database,
    key: Key16,
    parent_key: Option<Key16>,
    timing: Arc<Mutex<TimingState>>,
    leaf_id: Arc<Mutex<Option<Id16>>>,
}

impl BindingTreeLock {
    pub fn new<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        timeout: PackedTime,
        expired: PackedTime,
    ) -> Self {
        Self::with_parent(database, None, key, timeout, expired)
    }

    pub fn with_parent<K: AsRef<[u8]>>(
        database: Database,
        parent: Option<Key16>,
        key: K,
        timeout: PackedTime,
        expired: PackedTime,
    ) -> Self {
        let timeout = database.timeout_packed(timeout);
        let expired = database.expired_packed(expired);
        Self {
            database,
            key: Key16::new(key),
            parent_key: parent,
            timing: shared_timing(timeout, expired),
            leaf_id: Arc::new(Mutex::new(None)),
        }
    }

    pub fn db_id(&self) -> u8 {
        self.database.db_id()
    }

    pub fn lock_key(&self) -> Key16 {
        self.key
    }

    pub fn parent_key(&self) -> Option<Key16> {
        self.parent_key
    }

    pub fn is_root(&self) -> bool {
        self.parent_key.is_none()
    }

    pub fn timeout(&self) -> PackedTime {
        read_timing(&self.timing).timeout
    }

    pub fn expired(&self) -> PackedTime {
        read_timing(&self.timing).expired
    }

    /// Replaces the packed timeout used by the next submission. Database
    /// defaults are not re-merged and identity/state are preserved.
    pub fn set_timeout(&self, timeout: PackedTime) {
        let mut guard = self
            .timing
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.timeout = timeout;
    }

    /// Replaces the packed expired used by the next submission.
    pub fn set_expired(&self, expired: PackedTime) {
        let mut guard = self
            .timing
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.expired = expired;
    }

    /// Replaces both packed timing values atomically.
    pub fn set_timing(&self, timeout: PackedTime, expired: PackedTime) {
        write_timing(&self.timing, timeout, expired);
    }

    pub fn new_child(&self) -> Self {
        let timing = read_timing(&self.timing);
        Self {
            database: self.database.clone(),
            key: Key16::new(Id16::new().into_bytes()),
            parent_key: Some(self.key),
            timing: shared_timing(timing.timeout, timing.expired),
            leaf_id: Arc::new(Mutex::new(None)),
        }
    }

    pub fn new_child_with_key<K: AsRef<[u8]>>(&self, key: K) -> Self {
        self.load_child(key)
    }

    pub fn load_child<K: AsRef<[u8]>>(&self, key: K) -> Self {
        let timing = read_timing(&self.timing);
        Self {
            database: self.database.clone(),
            key: Key16::new(key),
            parent_key: Some(self.key),
            timing: shared_timing(timing.timeout, timing.expired),
            leaf_id: Arc::new(Mutex::new(None)),
        }
    }

    pub fn new_leaf_lock(&self) -> BindingTreeLeafLock {
        self.load_leaf_lock(Id16::new())
    }

    pub fn load_leaf_lock(&self, lock_id: Id16) -> BindingTreeLeafLock {
        // Java's `TreeLeafLock` owns a separate `Lock` built with the tree's
        // timing at construction time. Detach the timing snapshot so a later
        // `set_timing` on this tree cannot rewrite an existing leaf's bits.
        let timing = read_timing(&self.timing);
        BindingTreeLeafLock {
            tree: BindingTreeLock {
                database: self.database.clone(),
                key: self.key,
                parent_key: self.parent_key,
                timing: shared_timing(timing.timeout, timing.expired),
                leaf_id: Arc::new(Mutex::new(None)),
            },
            lock_id,
        }
    }

    fn snapshot(&self) -> TreeLock {
        TreeLock::with_packed_times(
            self.database.clone(),
            self.key.as_bytes(),
            self.parent_key,
            read_timing(&self.timing).timeout,
            read_timing(&self.timing).expired,
        )
    }

    pub fn acquire(&self) -> BindingCompletion<LockCommandResult> {
        let mut tree = self.snapshot();
        let leaf_id = Arc::clone(&self.leaf_id);
        Box::pin(async move {
            let check = tree.acquire_check().await?;
            let cached = *state(&leaf_id);
            if cached.is_some() {
                return Ok(check);
            }
            let mut leaf = tree.load_leaf_lock(Id16::new());
            let result = leaf.acquire().await?;
            *state(&leaf_id) = Some(leaf.lock_id());
            Ok(result)
        })
    }

    pub fn release(&self) -> BindingCompletion<()> {
        let cached = { state(&self.leaf_id).take() };
        let Some(lock_id) = cached else {
            return Box::pin(async { Ok(()) });
        };
        let tree = self.snapshot();
        Box::pin(async move {
            let mut leaf = tree.load_leaf_lock(lock_id);
            leaf.release().await.map(|_| ())
        })
    }

    /// Matches Java `TreeLock.wait`: raw timeout, zero expired, count 0.
    pub fn wait(&self, timeout: u16) -> BindingCompletion<LockCommandResult> {
        self.wait_packed(PackedTime::new(timeout))
    }

    pub fn wait_packed(&self, timeout: PackedTime) -> BindingCompletion<LockCommandResult> {
        let mut tree = self.snapshot();
        Box::pin(async move { tree.wait_packed(timeout).await })
    }
}

/// Typed handle to `aio::TreeLeafLock`.
#[derive(Clone, Debug)]
pub struct BindingTreeLeafLock {
    tree: BindingTreeLock,
    lock_id: Id16,
}

impl BindingTreeLeafLock {
    pub fn lock_key(&self) -> Key16 {
        self.tree.lock_key()
    }

    pub fn lock_id(&self) -> Id16 {
        self.lock_id
    }

    pub fn timeout(&self) -> PackedTime {
        self.tree.timeout()
    }

    pub fn expired(&self) -> PackedTime {
        self.tree.expired()
    }

    pub fn set_timeout(&self, timeout: PackedTime) {
        self.tree.set_timeout(timeout);
    }

    pub fn set_expired(&self, expired: PackedTime) {
        self.tree.set_expired(expired);
    }

    pub fn set_timing(&self, timeout: PackedTime, expired: PackedTime) {
        self.tree.set_timing(timeout, expired);
    }

    pub fn acquire(&self) -> BindingCompletion<LockCommandResult> {
        let mut leaf: TreeLeafLock = self.tree.snapshot().load_leaf_lock(self.lock_id);
        Box::pin(async move { leaf.acquire().await })
    }

    pub fn release(&self) -> BindingCompletion<LockCommandResult> {
        let mut leaf: TreeLeafLock = self.tree.snapshot().load_leaf_lock(self.lock_id);
        Box::pin(async move { leaf.release().await })
    }
}

impl BindingTreeLock {
    pub fn current_leaf_id(&self) -> Option<Id16> {
        *state(&self.leaf_id)
    }
}

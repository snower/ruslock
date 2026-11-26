//! Concurrency-safe primitive objects for language bindings.
//!
//! Each operation is submitted through a shared, cheaply cloned handle and
//! returns an owned completion future. The shared state mutex only covers short
//! snapshots and result application; it is never held across an `.await`.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::aio::{Database, Event, Lock};
use crate::data::{LockData, LockResultData};
use crate::error::Result;
use crate::key::Key16;
use crate::protocol::id::Id16;
use crate::protocol::result::LockCommandResult;
use crate::time::PackedTime;

use super::{read_timing, shared_timing, write_timing, TimingState};

/// An owned, `Send + 'static` Rust completion.
///
/// A binding (JNI, C, ...) can store this future, poll it on its executor, or
/// await it from a synchronous facade without borrowing the primitive object.
pub type BindingCompletion<T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'static>>;

fn lock_state<T>(state: &Mutex<T>) -> MutexGuard<'_, T> {
    state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Clone, Debug, Default)]
struct LockState {
    count: u16,
    r_count: u8,
    current_data: Option<LockResultData>,
}

#[derive(Clone, Debug)]
struct LockConfig {
    database: Database,
    lock_key: Key16,
    lock_id: Id16,
}

/// Typed handle to the Rust `aio::Lock` primitive.
///
/// Cloning is cheap and shares mutable primitive state, matching the Java
/// object's ability to be used from multiple threads.
#[derive(Clone, Debug)]
pub struct BindingLock {
    config: LockConfig,
    timing: Arc<Mutex<TimingState>>,
    state: Arc<Mutex<LockState>>,
}

impl BindingLock {
    /// Creates a lock for `key` using the caller's full packed timeout/expired
    /// bit patterns. Database default flags are merged, not truncated.
    pub fn new<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        timeout: PackedTime,
        expired: PackedTime,
    ) -> Self {
        Self::with_resolved(database, key, Id16::new(), timeout, expired)
    }

    /// Creates a lock with an explicit lock ID, preserving packed flags.
    pub fn with_lock_id<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        lock_id: Id16,
        timeout: PackedTime,
        expired: PackedTime,
    ) -> Self {
        Self::with_resolved(database, key, lock_id, timeout, expired)
    }

    fn with_resolved<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        lock_id: Id16,
        timeout: PackedTime,
        expired: PackedTime,
    ) -> Self {
        let timeout = database.timeout_packed(timeout);
        let expired = database.expired_packed(expired);
        Self {
            config: LockConfig {
                database,
                lock_key: Key16::new(key),
                lock_id,
            },
            timing: shared_timing(timeout, expired),
            state: Arc::new(Mutex::new(LockState::default())),
        }
    }

    pub fn db_id(&self) -> u8 {
        self.config.database.db_id()
    }

    pub fn lock_key(&self) -> Key16 {
        self.config.lock_key
    }

    pub fn lock_id(&self) -> Id16 {
        self.config.lock_id
    }

    pub fn timeout(&self) -> PackedTime {
        read_timing(&self.timing).timeout
    }

    pub fn expired(&self) -> PackedTime {
        read_timing(&self.timing).expired
    }

    /// Replaces the packed timeout used by the next submission. Flags are not
    /// re-merged, and the lock identity/count are preserved.
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

    pub fn count(&self) -> u16 {
        lock_state(&self.state).count
    }

    pub fn r_count(&self) -> u8 {
        lock_state(&self.state).r_count
    }

    pub fn current_data(&self) -> Option<LockResultData> {
        lock_state(&self.state).current_data.clone()
    }

    /// Updates the acquire count used by subsequent submissions.
    pub fn set_count(&self, count: u16) {
        lock_state(&self.state).count = count;
    }

    /// Updates the acquire priority count used by subsequent submissions.
    pub fn set_r_count(&self, r_count: u8) {
        lock_state(&self.state).r_count = r_count;
    }

    fn snapshot(&self) -> (Lock, Arc<Mutex<LockState>>) {
        let (count, r_count) = {
            let state = lock_state(&self.state);
            (state.count, state.r_count)
        };
        let timing = read_timing(&self.timing);
        let lock = Lock::with_packed_times(
            self.config.database.clone(),
            self.config.lock_key.as_bytes(),
            self.config.lock_id,
            timing.timeout,
            timing.expired,
            count,
            r_count,
        );
        (lock, Arc::clone(&self.state))
    }

    pub fn acquire(&self) -> BindingCompletion<LockCommandResult> {
        let (mut lock, state) = self.snapshot();
        Box::pin(async move {
            let result = lock.acquire().await;
            finish(&state, &lock, result)
        })
    }

    pub fn acquire_with_data(&self, data: LockData) -> BindingCompletion<LockCommandResult> {
        let (mut lock, state) = self.snapshot();
        Box::pin(async move {
            let result = lock.acquire_with_data(data).await;
            finish(&state, &lock, result)
        })
    }

    pub fn acquire_with_flags(
        &self,
        flag: u8,
        data: Option<LockData>,
    ) -> BindingCompletion<LockCommandResult> {
        let (mut lock, state) = self.snapshot();
        Box::pin(async move {
            let result = lock.acquire_with_flags(flag, data).await;
            finish(&state, &lock, result)
        })
    }

    pub fn release(&self) -> BindingCompletion<LockCommandResult> {
        let (mut lock, state) = self.snapshot();
        Box::pin(async move {
            let result = lock.release().await;
            finish(&state, &lock, result)
        })
    }

    pub fn release_with_data(&self, data: LockData) -> BindingCompletion<LockCommandResult> {
        let (mut lock, state) = self.snapshot();
        Box::pin(async move {
            let result = lock.release_with_data(data).await;
            finish(&state, &lock, result)
        })
    }

    pub fn release_with_flags(
        &self,
        flag: u8,
        data: Option<LockData>,
    ) -> BindingCompletion<LockCommandResult> {
        let (mut lock, state) = self.snapshot();
        Box::pin(async move {
            let result = lock.release_with_flags(flag, data).await;
            finish(&state, &lock, result)
        })
    }

    pub fn show(&self) -> BindingCompletion<Option<LockCommandResult>> {
        let (mut lock, state) = self.snapshot();
        Box::pin(async move {
            let result = lock.show().await;
            finish(&state, &lock, result)
        })
    }

    pub fn update(&self, data: Option<LockData>) -> BindingCompletion<()> {
        let (mut lock, state) = self.snapshot();
        Box::pin(async move {
            let result = lock.update(data).await;
            finish(&state, &lock, result)
        })
    }

    pub fn release_head(&self, data: Option<LockData>) -> BindingCompletion<()> {
        let (mut lock, state) = self.snapshot();
        Box::pin(async move {
            let result = lock.release_head(data).await;
            finish(&state, &lock, result)
        })
    }

    pub fn release_head_to_lock_wait(
        &self,
        data: Option<LockData>,
    ) -> BindingCompletion<LockCommandResult> {
        let (mut lock, state) = self.snapshot();
        Box::pin(async move {
            let result = lock.release_head_to_lock_wait(data).await;
            finish(&state, &lock, result)
        })
    }
}

/// Mirrors the aio primitive's current data into the shared binding state.
///
/// `aio::Lock` updates `current_data` even for error results, so this runs for
/// every operation before the result is handed back.
fn finish<T>(state: &Mutex<LockState>, lock: &Lock, result: Result<T>) -> Result<T> {
    let current_data = lock.current_data().cloned();
    lock_state(state).current_data = current_data;
    result
}

#[derive(Clone, Debug, Default)]
struct EventState {
    current_data: Option<LockResultData>,
}

#[derive(Clone, Debug)]
struct EventConfig {
    database: Database,
    lock_key: Key16,
    default_set: bool,
}

/// Typed handle to the Rust `aio::Event` primitive.
///
/// `wait()` and `set()` may run concurrently on clones of the same handle: each
/// submission snapshots its own `aio::Event` and only the wait result is merged
/// back under a short mutex.
#[derive(Clone, Debug)]
pub struct BindingEvent {
    config: EventConfig,
    timing: Arc<Mutex<TimingState>>,
    state: Arc<Mutex<EventState>>,
}

impl BindingEvent {
    /// Creates an event using the caller's full packed timeout/expired bit
    /// patterns. Database default flags are merged, not truncated.
    pub fn new<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        timeout: PackedTime,
        expired: PackedTime,
        default_set: bool,
    ) -> Self {
        let timeout = database.timeout_packed(timeout);
        let expired = database.expired_packed(expired);
        Self {
            config: EventConfig {
                database,
                lock_key: Key16::new(key),
                default_set,
            },
            timing: shared_timing(timeout, expired),
            state: Arc::new(Mutex::new(EventState::default())),
        }
    }

    pub fn db_id(&self) -> u8 {
        self.config.database.db_id()
    }

    pub fn lock_key(&self) -> Key16 {
        self.config.lock_key
    }

    pub fn timeout(&self) -> PackedTime {
        read_timing(&self.timing).timeout
    }

    pub fn expired(&self) -> PackedTime {
        read_timing(&self.timing).expired
    }

    /// Replaces the packed timeout used by the next submission. Flags are not
    /// re-merged; the event key and `defaultSet` polarity are preserved.
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

    pub fn default_set(&self) -> bool {
        self.config.default_set
    }

    pub fn current_data(&self) -> Option<LockResultData> {
        lock_state(&self.state).current_data.clone()
    }

    fn snapshot(&self) -> Event {
        let timing = read_timing(&self.timing);
        Event::with_packed_times(
            self.config.database.clone(),
            self.config.lock_key.as_bytes(),
            timing.timeout,
            timing.expired,
            self.config.default_set,
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

    pub fn set_with_data(&self, data: Option<LockData>) -> BindingCompletion<()> {
        let mut event = self.snapshot();
        Box::pin(async move { event.set_with_data(data).await })
    }

    pub fn clear(&self) -> BindingCompletion<()> {
        self.clear_with_data(None)
    }

    /// Clears the event with a payload. `defaultSet` polarity selects `LOCK`
    /// update vs. `UNLOCK`, matching Java `Event.clear(LockData)`.
    pub fn clear_with_data(&self, data: Option<LockData>) -> BindingCompletion<()> {
        let mut event = self.snapshot();
        Box::pin(async move { event.clear_with_data(data).await })
    }

    /// Waits for the event using the caller's full packed timeout bit pattern.
    pub fn wait(&self, timeout: PackedTime) -> BindingCompletion<()> {
        let mut event = self.snapshot();
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let result = event.wait_packed(timeout).await;
            let current_data = event.current_data().cloned();
            lock_state(&state).current_data = current_data;
            result
        })
    }

    /// Waits and, on timeout, performs Java's compensating clear for a
    /// default-set event (a non-default event only releases after success).
    pub fn wait_and_timeout_retry_clear(&self, timeout: u16) -> BindingCompletion<()> {
        let mut event = self.snapshot();
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let result = event.wait_and_timeout_retry_clear(timeout).await;
            let current_data = event.current_data().cloned();
            lock_state(&state).current_data = current_data;
            result
        })
    }

    /// Packed-time variant of [`BindingEvent::wait_and_timeout_retry_clear`].
    pub fn wait_and_timeout_retry_clear_packed(
        &self,
        timeout: PackedTime,
    ) -> BindingCompletion<()> {
        let mut event = self.snapshot();
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let result = event.wait_and_timeout_retry_clear_packed(timeout).await;
            let current_data = event.current_data().cloned();
            lock_state(&state).current_data = current_data;
            result
        })
    }
}

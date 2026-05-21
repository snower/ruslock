use std::sync::{Arc, Mutex};

use crate::callback::client::RequestHandle;
use crate::callback::database::Database;
use crate::data::{LockData, LockResultData};
use crate::error::{Result, SlockError};
use crate::key::Key16;
use crate::protocol::command::{Command, LockCommand};
use crate::protocol::constants::*;
use crate::protocol::id::Id16;
use crate::protocol::result::{CommandResult, LockCommandResult};
use crate::time::PackedTime;

#[derive(Clone, Debug)]
pub struct Lock {
    database: Database,
    state: Arc<Mutex<LockState>>,
}

#[derive(Clone, Debug)]
struct LockState {
    lock_key: Key16,
    lock_id: Id16,
    timeout: PackedTime,
    expired: PackedTime,
    count: u16,
    r_count: u8,
    current_data: Option<LockResultData>,
}

impl Lock {
    pub(crate) fn new<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        timeout: u16,
        expired: u16,
    ) -> Self {
        let timeout = database.timeout(timeout);
        let expired = database.expired(expired);
        Self {
            database,
            state: Arc::new(Mutex::new(LockState {
                lock_key: Key16::new(key),
                lock_id: Id16::new(),
                timeout,
                expired,
                count: 0,
                r_count: 0,
                current_data: None,
            })),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn with_lock_id<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        lock_id: Id16,
        timeout: u16,
        expired: u16,
        count: u16,
        r_count: u8,
    ) -> Self {
        let timeout = database.timeout(timeout);
        let expired = database.expired(expired);
        Self {
            database,
            state: Arc::new(Mutex::new(LockState {
                lock_key: Key16::new(key),
                lock_id,
                timeout,
                expired,
                count,
                r_count,
                current_data: None,
            })),
        }
    }

    pub fn set_count(&self, count: u16) {
        self.state.lock().expect("lock state mutex poisoned").count = count;
    }

    pub fn set_r_count(&self, r_count: u8) {
        self.state
            .lock()
            .expect("lock state mutex poisoned")
            .r_count = r_count;
    }

    fn set_timeout(&self, timeout: PackedTime) {
        self.state
            .lock()
            .expect("lock state mutex poisoned")
            .timeout = timeout;
    }

    fn set_current_data(&self, data: Option<LockResultData>) {
        self.state
            .lock()
            .expect("lock state mutex poisoned")
            .current_data = data;
    }

    pub fn acquire<F>(&self, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<LockCommandResult>) + Send + 'static,
    {
        self.acquire_with_flags(0, None, callback)
    }

    /// Writes an acquire command with LockData and returns a cancellable handle.
    pub fn acquire_with_data<F>(&self, data: LockData, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<LockCommandResult>) + Send + 'static,
    {
        self.acquire_with_flags(0, Some(data), callback)
    }

    pub fn acquire_with_flags<F>(
        &self,
        flag: u8,
        data: Option<LockData>,
        callback: F,
    ) -> Result<RequestHandle>
    where
        F: FnOnce(Result<LockCommandResult>) + Send + 'static,
    {
        let flag = if data.is_some() {
            flag | LOCK_FLAG_CONTAINS_DATA
        } else {
            flag
        };
        self.send_lock_mapped(
            COMMAND_TYPE_LOCK,
            flag,
            self.lock_id(),
            data,
            |value| value,
            callback,
        )
    }

    pub fn release<F>(&self, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<LockCommandResult>) + Send + 'static,
    {
        self.release_with_flags(0, None, callback)
    }

    pub fn release_with_data<F>(&self, data: LockData, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<LockCommandResult>) + Send + 'static,
    {
        self.release_with_flags(0, Some(data), callback)
    }

    pub fn release_with_flags<F>(
        &self,
        flag: u8,
        data: Option<LockData>,
        callback: F,
    ) -> Result<RequestHandle>
    where
        F: FnOnce(Result<LockCommandResult>) + Send + 'static,
    {
        let flag = if data.is_some() {
            flag | UNLOCK_FLAG_CONTAINS_DATA
        } else {
            flag
        };
        self.send_lock_mapped(
            COMMAND_TYPE_UNLOCK,
            flag,
            self.lock_id(),
            data,
            |value| value,
            callback,
        )
    }

    pub fn show<F>(&self, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<Option<LockCommandResult>>) + Send + 'static,
    {
        self.send_lock_mapped(
            COMMAND_TYPE_LOCK,
            LOCK_FLAG_SHOW_WHEN_LOCKED,
            self.lock_id(),
            None,
            |value| match value {
                Ok(result) => Ok(Some(result)),
                Err(SlockError::LockNotOwn(result)) => Ok(Some(*result)),
                Err(err) => Err(err),
            },
            callback,
        )
    }

    pub fn update<F>(&self, data: Option<LockData>, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<()>) + Send + 'static,
    {
        let flag = if data.is_some() {
            LOCK_FLAG_UPDATE_WHEN_LOCKED | LOCK_FLAG_CONTAINS_DATA
        } else {
            LOCK_FLAG_UPDATE_WHEN_LOCKED
        };
        self.send_lock_mapped(
            COMMAND_TYPE_LOCK,
            flag,
            self.lock_id(),
            data,
            |value| match value {
                Ok(_) | Err(SlockError::LockLocked(_)) => Ok(()),
                Err(err) => Err(err),
            },
            callback,
        )
    }

    pub fn release_head<F>(&self, data: Option<LockData>, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<()>) + Send + 'static,
    {
        let flag = UNLOCK_FLAG_UNLOCK_FIRST_LOCK_WHEN_UNLOCKED
            | if data.is_some() {
                UNLOCK_FLAG_CONTAINS_DATA
            } else {
                0
            };
        self.send_lock_mapped(
            COMMAND_TYPE_UNLOCK,
            flag,
            Id16::zero(),
            data,
            |value| value.map(|_| ()),
            callback,
        )
    }

    pub fn release_head_to_lock_wait<F>(
        &self,
        data: Option<LockData>,
        callback: F,
    ) -> Result<RequestHandle>
    where
        F: FnOnce(Result<LockCommandResult>) + Send + 'static,
    {
        self.release_with_flags(
            UNLOCK_FLAG_UNLOCK_FIRST_LOCK_WHEN_UNLOCKED | UNLOCK_FLAG_SUCCED_TO_LOCK_WAIT,
            data,
            callback,
        )
    }

    pub fn current_data(&self) -> Option<LockResultData> {
        self.state
            .lock()
            .expect("lock state mutex poisoned")
            .current_data
            .clone()
    }

    pub fn lock_key(&self) -> Key16 {
        self.state
            .lock()
            .expect("lock state mutex poisoned")
            .lock_key
    }

    pub fn lock_id(&self) -> Id16 {
        self.state
            .lock()
            .expect("lock state mutex poisoned")
            .lock_id
    }

    pub fn db_id(&self) -> u8 {
        self.database.db_id()
    }

    pub fn timeout(&self) -> PackedTime {
        self.state
            .lock()
            .expect("lock state mutex poisoned")
            .timeout
    }

    pub fn expired(&self) -> PackedTime {
        self.state
            .lock()
            .expect("lock state mutex poisoned")
            .expired
    }

    fn command(
        &self,
        command_type: u8,
        flag: u8,
        lock_id: Id16,
        data: Option<LockData>,
    ) -> LockCommand {
        let state = self
            .state
            .lock()
            .expect("lock state mutex poisoned")
            .clone();
        LockCommand::new(
            command_type,
            flag,
            self.database.db_id(),
            Id16::new(),
            state.lock_key,
            lock_id,
            state.timeout,
            state.expired,
            state.count,
            state.r_count,
            data,
        )
    }

    fn send_lock_mapped<T, M, F>(
        &self,
        command_type: u8,
        flag: u8,
        lock_id: Id16,
        data: Option<LockData>,
        mapper: M,
        callback: F,
    ) -> Result<RequestHandle>
    where
        T: Send + 'static,
        M: FnOnce(Result<LockCommandResult>) -> Result<T> + Send + 'static,
        F: FnOnce(Result<T>) + Send + 'static,
    {
        let state = self.state.clone();
        let command = self.command(command_type, flag, lock_id, data);
        self.database
            .send_command_callback(Command::Lock(command), move |result| {
                callback(mapper(command_result_to_lock_result(&state, result)));
            })
    }

    #[allow(clippy::too_many_arguments)]
    fn send_lock_on_active<T, M, F>(
        &self,
        active_request: Arc<Mutex<Option<Id16>>>,
        command_type: u8,
        flag: u8,
        lock_id: Id16,
        data: Option<LockData>,
        mapper: M,
        callback: F,
    ) -> Result<Id16>
    where
        T: Send + 'static,
        M: FnOnce(Result<LockCommandResult>) -> Result<T> + Send + 'static,
        F: FnOnce(Result<T>) + Send + 'static,
    {
        let state = self.state.clone();
        let command = self.command(command_type, flag, lock_id, data);
        self.database.client().send_command_on_handle(
            Command::Lock(command),
            active_request,
            move |result| {
                callback(mapper(command_result_to_lock_result(&state, result)));
            },
        )
    }
}

fn command_result_to_lock_result(
    state: &Arc<Mutex<LockState>>,
    result: Result<CommandResult>,
) -> Result<LockCommandResult> {
    match result {
        Ok(CommandResult::Lock(result)) => apply_lock_result(state, result),
        Ok(_) => Err(SlockError::Protocol("expected lock result".to_string())),
        Err(err) => Err(err),
    }
}

fn apply_lock_result(
    state: &Arc<Mutex<LockState>>,
    result: LockCommandResult,
) -> Result<LockCommandResult> {
    state
        .lock()
        .expect("lock state mutex poisoned")
        .current_data = result.data.clone();
    match result.result {
        COMMAND_RESULT_SUCCED => Ok(result),
        COMMAND_RESULT_LOCKED_ERROR => Err(SlockError::LockLocked(Box::new(result))),
        COMMAND_RESULT_UNLOCK_ERROR => Err(SlockError::LockUnlocked(Box::new(result))),
        COMMAND_RESULT_UNOWN_ERROR => Err(SlockError::LockNotOwn(Box::new(result))),
        COMMAND_RESULT_TIMEOUT => Err(SlockError::LockTimeout(Box::new(result))),
        COMMAND_RESULT_EXPRIED => Err(SlockError::LockExpired(Box::new(result))),
        COMMAND_RESULT_STATE_ERROR => Err(SlockError::StateError(Box::new(result))),
        result_code => Err(SlockError::Server {
            result: result_code,
        }),
    }
}

#[derive(Clone, Debug)]
pub struct Event {
    lock: Lock,
    default_set: bool,
}

impl Event {
    pub(crate) fn new<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        timeout: u16,
        expired: u16,
        default_set: bool,
    ) -> Self {
        Self {
            lock: Lock::new(database, key, timeout, expired),
            default_set,
        }
    }

    pub fn is_set<F>(&self, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<bool>) + Send + 'static,
    {
        let check_lock = if self.default_set {
            Lock::new(
                self.lock.database.clone(),
                self.lock.lock_key().as_bytes(),
                0,
                0,
            )
        } else {
            let lock = Lock::new(
                self.lock.database.clone(),
                self.lock.lock_key().as_bytes(),
                0,
                0,
            );
            lock.set_timeout(PackedTime::with_flags(
                0,
                TIMEOUT_FLAG_LOCK_WAIT_WHEN_UNLOCK,
            ));
            lock.set_count(1);
            lock
        };
        check_lock.acquire(move |result| {
            callback(match result {
                Ok(_) => Ok(true),
                Err(SlockError::LockTimeout(_)) | Err(SlockError::LockNotOwn(_)) => Ok(false),
                Err(err) => Err(err),
            });
        })
    }

    pub fn set<F>(&self, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<()>) + Send + 'static,
    {
        self.set_with_data(None, callback)
    }

    pub fn set_with_data<F>(&self, data: Option<LockData>, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<()>) + Send + 'static,
    {
        if self.default_set {
            let event_lock = self.event_lock(0);
            event_lock.release_with_flags(0, data, move |result| {
                callback(match result {
                    Ok(_) | Err(SlockError::LockUnlocked(_)) => Ok(()),
                    Err(err) => Err(err),
                });
            })
        } else {
            let event_lock = self.event_lock(1);
            event_lock.update(data, callback)
        }
    }

    pub fn clear<F>(&self, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<()>) + Send + 'static,
    {
        if self.default_set {
            let event_lock = self.event_lock(0);
            event_lock.update(None, callback)
        } else {
            let event_lock = self.event_lock(1);
            event_lock.release(move |result| {
                callback(match result {
                    Ok(_) | Err(SlockError::LockUnlocked(_)) => Ok(()),
                    Err(err) => Err(err),
                });
            })
        }
    }

    pub fn wait<F>(&self, timeout: u16, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<()>) + Send + 'static,
    {
        let wait_lock = Lock::new(
            self.lock.database.clone(),
            self.lock.lock_key().as_bytes(),
            timeout,
            0,
        );
        if !self.default_set {
            wait_lock.set_timeout(PackedTime::with_flags(
                timeout,
                TIMEOUT_FLAG_LOCK_WAIT_WHEN_UNLOCK,
            ));
            wait_lock.set_count(1);
        }
        let lock = self.lock.clone();
        let wait_lock_for_callback = wait_lock.clone();
        wait_lock.acquire(move |result| {
            lock.set_current_data(wait_lock_for_callback.current_data());
            callback(match result {
                Ok(_) => Ok(()),
                Err(SlockError::LockTimeout(_)) | Err(SlockError::CommandTimeout) => {
                    Err(SlockError::EventWaitTimeout)
                }
                Err(err) => Err(err),
            });
        })
    }

    pub fn current_data(&self) -> Option<LockResultData> {
        self.lock.current_data()
    }

    fn event_lock(&self, count: u16) -> Lock {
        Lock::with_lock_id(
            self.lock.database.clone(),
            self.lock.lock_key().as_bytes(),
            Id16::from_bytes(self.lock.lock_key().into_bytes()),
            self.lock.timeout().value(),
            self.lock.expired().value(),
            count,
            0,
        )
    }
}

#[derive(Clone, Debug)]
pub struct ReentrantLock {
    lock: Lock,
}

impl ReentrantLock {
    pub(crate) fn new<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        timeout: u16,
        expired: u16,
    ) -> Self {
        let lock = Lock::new(database, key, timeout, expired);
        lock.set_r_count(0xff);
        Self { lock }
    }

    pub fn acquire<F>(&self, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<LockCommandResult>) + Send + 'static,
    {
        self.lock.acquire(callback)
    }

    pub fn release<F>(&self, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<LockCommandResult>) + Send + 'static,
    {
        self.lock.release(callback)
    }
}

#[derive(Clone, Debug)]
pub struct Semaphore {
    database: Database,
    key: Vec<u8>,
    count: u16,
    timeout: u16,
    expired: u16,
}

impl Semaphore {
    pub(crate) fn new<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        count: u16,
        timeout: u16,
        expired: u16,
    ) -> Self {
        Self {
            database,
            key: key.as_ref().to_vec(),
            count: count.saturating_sub(1),
            timeout,
            expired,
        }
    }

    pub fn acquire<F>(&self, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<LockCommandResult>) + Send + 'static,
    {
        let lock = Lock::new(self.database.clone(), &self.key, self.timeout, self.expired);
        lock.set_count(self.count);
        lock.acquire(callback)
    }

    pub fn release<F>(&self, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<()>) + Send + 'static,
    {
        let lock = Lock::with_lock_id(
            self.database.clone(),
            &self.key,
            Id16::zero(),
            self.timeout,
            self.expired,
            self.count,
            0,
        );
        lock.release_head(None, callback)
    }
}

#[derive(Clone, Debug)]
pub struct ReadWriteLock {
    database: Database,
    key: Vec<u8>,
    timeout: u16,
    expired: u16,
    write_lock: Lock,
    read_locks: Arc<Mutex<Vec<Lock>>>,
}

impl ReadWriteLock {
    pub(crate) fn new<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        timeout: u16,
        expired: u16,
    ) -> Self {
        let key_vec = key.as_ref().to_vec();
        let write_lock = Lock::new(database.clone(), &key_vec, timeout, expired);
        write_lock.set_count(0);
        Self {
            database,
            key: key_vec,
            timeout,
            expired,
            write_lock,
            read_locks: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn acquire_write<F>(&self, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<LockCommandResult>) + Send + 'static,
    {
        self.write_lock.acquire(callback)
    }

    pub fn release_write<F>(&self, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<LockCommandResult>) + Send + 'static,
    {
        self.write_lock.release(callback)
    }

    pub fn acquire_read<F>(&self, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<LockCommandResult>) + Send + 'static,
    {
        let lock = Lock::new(self.database.clone(), &self.key, self.timeout, self.expired);
        lock.set_count(u16::MAX);
        let read_locks = self.read_locks.clone();
        let lock_for_push = lock.clone();
        lock.acquire(move |result| {
            if result.is_ok() {
                read_locks
                    .lock()
                    .expect("read locks mutex poisoned")
                    .push(lock_for_push);
            }
            callback(result);
        })
    }

    pub fn release_read<F>(&self, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<LockCommandResult>) + Send + 'static,
    {
        let lock = self
            .read_locks
            .lock()
            .expect("read locks mutex poisoned")
            .pop()
            .ok_or_else(|| SlockError::LockData("no read lock to release".to_string()))?;
        lock.release(callback)
    }
}

#[derive(Clone, Debug)]
pub struct PriorityLock {
    lock: Lock,
    priority: u8,
}

impl PriorityLock {
    pub(crate) fn new<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        priority: u8,
        timeout: u16,
        expired: u16,
    ) -> Self {
        let lock = Lock::new(database, key, timeout, expired);
        lock.set_timeout(lock.timeout().merge_flags(TIMEOUT_FLAG_RCOUNT_IS_PRIORITY));
        lock.set_r_count(priority);
        Self { lock, priority }
    }

    pub fn priority(&self) -> u8 {
        self.priority
    }

    pub fn acquire<F>(&self, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<LockCommandResult>) + Send + 'static,
    {
        self.lock.acquire(callback)
    }

    pub fn release<F>(&self, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<LockCommandResult>) + Send + 'static,
    {
        self.lock.release(callback)
    }
}

#[derive(Clone, Debug)]
pub struct MaxConcurrentFlow {
    lock: Lock,
}

impl MaxConcurrentFlow {
    pub(crate) fn new<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        max: u16,
        timeout: u16,
        expired: u16,
    ) -> Self {
        let lock = Lock::new(database, key, timeout, expired);
        lock.set_count(max.saturating_sub(1));
        Self { lock }
    }

    pub fn acquire<F>(&self, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<LockCommandResult>) + Send + 'static,
    {
        self.lock.acquire(callback)
    }

    pub fn release<F>(&self, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<LockCommandResult>) + Send + 'static,
    {
        self.lock.release(callback)
    }
}

#[derive(Clone, Debug)]
pub struct TokenBucketFlow {
    lock: Lock,
}

impl TokenBucketFlow {
    pub(crate) fn new<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        count: u16,
        timeout: u16,
        period: f64,
    ) -> Self {
        let expired = if period < 3.0 {
            ((period * 1000.0).ceil() as u16).max(1)
        } else {
            period.ceil() as u16
        };
        let lock = Lock::new(database, key, timeout, expired);
        lock.set_count(count.saturating_sub(1));
        if period < 3.0 {
            lock.state
                .lock()
                .expect("lock state mutex poisoned")
                .expired = lock.expired().merge_flags(EXPRIED_FLAG_MILLISECOND_TIME);
        }
        Self { lock }
    }

    pub fn acquire<F>(&self, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<LockCommandResult>) + Send + 'static,
    {
        self.lock.acquire(callback)
    }
}

#[derive(Clone, Debug)]
pub struct GroupEvent {
    lock: Lock,
    client_id: u64,
    version_id: Arc<Mutex<u64>>,
}

impl GroupEvent {
    pub(crate) fn new<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        client_id: u64,
        version_id: u64,
        timeout: u16,
        expired: u16,
    ) -> Self {
        let lock = Lock::with_lock_id(
            database,
            key,
            encode_group_event_lock_id(client_id, version_id),
            timeout,
            expired,
            0,
            0,
        );
        lock.set_timeout(
            lock.timeout()
                .merge_flags(TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED),
        );
        Self {
            lock,
            client_id,
            version_id: Arc::new(Mutex::new(version_id)),
        }
    }

    pub fn version_id(&self) -> u64 {
        *self
            .version_id
            .lock()
            .expect("group version mutex poisoned")
    }

    pub fn is_set<F>(&self, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<bool>) + Send + 'static,
    {
        let check_lock = Lock::with_lock_id(
            self.lock.database.clone(),
            self.lock.lock_key().as_bytes(),
            Id16::new(),
            0,
            0,
            0,
            0,
        );
        check_lock.acquire(move |result| {
            callback(match result {
                Ok(_) => Ok(true),
                Err(SlockError::LockTimeout(_)) => Ok(false),
                Err(err) => Err(err),
            });
        })
    }

    pub fn set<F>(&self, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<()>) + Send + 'static,
    {
        let event_lock = Lock::with_lock_id(
            self.lock.database.clone(),
            self.lock.lock_key().as_bytes(),
            Id16::zero(),
            self.lock.timeout().value(),
            self.lock.expired().value(),
            0,
            0,
        );
        event_lock.release_head(None, move |result| {
            callback(match result {
                Ok(_) | Err(SlockError::LockUnlocked(_)) => Ok(()),
                Err(err) => Err(err),
            });
        })
    }

    pub fn clear<F>(&self, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<()>) + Send + 'static,
    {
        let event_lock = Lock::with_lock_id(
            self.lock.database.clone(),
            self.lock.lock_key().as_bytes(),
            encode_group_event_lock_id(0, self.version_id()),
            self.lock.timeout().value(),
            self.lock.expired().value(),
            0,
            0,
        );
        event_lock.set_timeout(
            event_lock
                .timeout()
                .merge_flags(TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED),
        );
        event_lock.update(None, callback)
    }

    pub fn wakeup<F>(&self, data: Option<LockData>, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<()>) + Send + 'static,
    {
        let event_lock = Lock::with_lock_id(
            self.lock.database.clone(),
            self.lock.lock_key().as_bytes(),
            Id16::zero(),
            self.lock.timeout().value(),
            self.lock.expired().value(),
            0,
            0,
        );
        event_lock.set_timeout(
            event_lock
                .timeout()
                .merge_flags(TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED),
        );
        let version_id = self.version_id.clone();
        event_lock.release_head_to_lock_wait(data, move |result| {
            callback(result.map(|result| {
                if result.lock_id != Id16::zero() {
                    *version_id.lock().expect("group version mutex poisoned") = u64::from_le_bytes(
                        result.lock_id.as_bytes()[0..8]
                            .try_into()
                            .expect("slice is 8 bytes"),
                    );
                }
            }));
        })
    }

    pub fn wait<F>(&self, timeout: u16, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<()>) + Send + 'static,
    {
        let current_version = self.version_id();
        let wait_lock = Lock::with_lock_id(
            self.lock.database.clone(),
            self.lock.lock_key().as_bytes(),
            encode_group_event_lock_id(self.client_id, current_version),
            timeout,
            0,
            0,
            0,
        );
        wait_lock.set_timeout(
            wait_lock
                .timeout()
                .merge_flags(TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED),
        );
        let lock = self.lock.clone();
        let wait_lock_for_callback = wait_lock.clone();
        let version_id = self.version_id.clone();
        let client_id = self.client_id;
        wait_lock.acquire(move |result| {
            lock.set_current_data(wait_lock_for_callback.current_data());
            callback(match result {
                Ok(result) => {
                    if result.lock_id != encode_group_event_lock_id(client_id, current_version) {
                        *version_id.lock().expect("group version mutex poisoned") =
                            u64::from_le_bytes(
                                result.lock_id.as_bytes()[0..8]
                                    .try_into()
                                    .expect("slice is 8 bytes"),
                            );
                    }
                    Ok(())
                }
                Err(SlockError::LockTimeout(_)) | Err(SlockError::CommandTimeout) => {
                    Err(SlockError::EventWaitTimeout)
                }
                Err(err) => Err(err),
            });
        })
    }

    pub fn current_data(&self) -> Option<LockResultData> {
        self.lock.current_data()
    }
}

#[derive(Clone, Debug)]
pub struct TreeLock {
    lock: Lock,
    parent_key: Option<Key16>,
}

impl TreeLock {
    pub(crate) fn new<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        timeout: u16,
        expired: u16,
    ) -> Self {
        let lock = Lock::new(database, key, timeout, expired);
        lock.set_count(u16::MAX);
        lock.set_r_count(1);
        lock.set_timeout(lock.timeout().merge_flags(TIMEOUT_FLAG_RCOUNT_IS_PRIORITY));
        Self {
            lock,
            parent_key: None,
        }
    }

    pub fn new_child(&self) -> Self {
        self.load_child(Key16::from_bytes(Id16::new().into_bytes()).as_bytes())
    }

    pub fn new_child_with_key<K: AsRef<[u8]>>(&self, key: K) -> Self {
        self.load_child(key)
    }

    pub fn load_child<K: AsRef<[u8]>>(&self, key: K) -> Self {
        let mut child = Self::new(
            self.lock.database.clone(),
            key,
            self.lock.timeout().value(),
            self.lock.expired().value(),
        );
        child.parent_key = Some(self.lock.lock_key());
        child
    }

    pub fn new_leaf_lock(&self) -> TreeLeafLock {
        self.load_leaf_lock(Id16::new())
    }

    pub fn load_leaf_lock(&self, lock_id: Id16) -> TreeLeafLock {
        let lock = Lock::with_lock_id(
            self.lock.database.clone(),
            self.lock.lock_key().as_bytes(),
            lock_id,
            self.lock.timeout().value(),
            self.lock.expired().value(),
            u16::MAX,
            1,
        );
        lock.set_timeout(lock.timeout().merge_flags(TIMEOUT_FLAG_RCOUNT_IS_PRIORITY));
        TreeLeafLock {
            tree_lock: self.clone(),
            lock,
        }
    }

    pub fn acquire<F>(&self, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<LockCommandResult>) + Send + 'static,
    {
        self.lock
            .acquire_with_flags(LOCK_FLAG_LOCK_TREE_LOCK, None, callback)
    }

    pub fn release<F>(&self, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<LockCommandResult>) + Send + 'static,
    {
        self.lock
            .release_with_flags(UNLOCK_FLAG_UNLOCK_TREE_LOCK, None, callback)
    }

    pub fn wait<F>(&self, timeout: u16, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<LockCommandResult>) + Send + 'static,
    {
        let check_lock = Lock::with_lock_id(
            self.lock.database.clone(),
            self.lock.lock_key().as_bytes(),
            Id16::new(),
            timeout,
            0,
            0,
            0,
        );
        check_lock.acquire(callback)
    }

    pub fn lock_key(&self) -> Key16 {
        self.lock.lock_key()
    }

    pub fn parent_key(&self) -> Option<Key16> {
        self.parent_key
    }

    pub fn is_root(&self) -> bool {
        self.parent_key.is_none()
    }
}

#[derive(Clone, Debug)]
pub struct TreeLeafLock {
    tree_lock: TreeLock,
    lock: Lock,
}

impl TreeLeafLock {
    pub fn acquire<F>(&self, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<LockCommandResult>) + Send + 'static,
    {
        let Some(parent_key) = self.tree_lock.parent_key else {
            return self.lock.acquire(callback);
        };

        let child_check = Lock::with_lock_id(
            self.tree_lock.lock.database.clone(),
            self.tree_lock.lock.lock_key().as_bytes(),
            Id16::from_bytes(parent_key.into_bytes()),
            0,
            self.tree_lock.lock.expired().value(),
            u16::MAX,
            1,
        );
        child_check.set_timeout(
            child_check
                .timeout()
                .merge_flags(TIMEOUT_FLAG_RCOUNT_IS_PRIORITY),
        );

        let parent_check = Lock::with_lock_id(
            self.tree_lock.lock.database.clone(),
            parent_key.as_bytes(),
            Id16::from_bytes(self.tree_lock.lock.lock_key().into_bytes()),
            0,
            self.tree_lock.lock.expired().value(),
            u16::MAX,
            1,
        );
        parent_check.set_timeout(
            parent_check
                .timeout()
                .merge_flags(TIMEOUT_FLAG_RCOUNT_IS_PRIORITY),
        );

        let leaf_lock = self.lock.clone();
        let user_callback: SharedLockCallback = Arc::new(Mutex::new(Some(Box::new(callback))));
        let active_request = Arc::new(Mutex::new(None));
        let client = self.tree_lock.lock.database.client();
        let request_id = child_check.send_lock_on_active(
            active_request.clone(),
            COMMAND_TYPE_LOCK,
            LOCK_FLAG_LOCK_TREE_LOCK,
            child_check.lock_id(),
            None,
            accept_locked_result,
            {
                let parent_check = parent_check.clone();
                let leaf_lock = leaf_lock.clone();
                let active_request = active_request.clone();
                let user_callback = user_callback.clone();
                move |child_result| {
                    if let Err(err) = child_result {
                        finish_lock_callback(&user_callback, Err(err));
                        return;
                    }
                    let active = active_request.clone();
                    let leaf_active = active_request.clone();
                    let leaf_callback = user_callback.clone();
                    if let Err(err) = parent_check.send_lock_on_active(
                        active,
                        COMMAND_TYPE_LOCK,
                        0,
                        parent_check.lock_id(),
                        None,
                        accept_locked_result,
                        move |parent_result| {
                            if let Err(err) = parent_result {
                                finish_lock_callback(&leaf_callback, Err(err));
                                return;
                            }
                            let final_callback = leaf_callback.clone();
                            if let Err(err) = leaf_lock.send_lock_on_active(
                                leaf_active,
                                COMMAND_TYPE_LOCK,
                                0,
                                leaf_lock.lock_id(),
                                None,
                                |value| value,
                                move |result| finish_lock_callback(&final_callback, result),
                            ) {
                                finish_lock_callback(&leaf_callback, Err(err));
                            }
                        },
                    ) {
                        finish_lock_callback(&user_callback, Err(err));
                    }
                }
            },
        )?;

        Ok(RequestHandle {
            request_id,
            active_request,
            client: Arc::downgrade(&client.inner),
        })
    }

    pub fn release<F>(&self, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<LockCommandResult>) + Send + 'static,
    {
        self.lock
            .release_with_flags(UNLOCK_FLAG_UNLOCK_TREE_LOCK, None, callback)
    }

    pub fn lock_key(&self) -> Key16 {
        self.lock.lock_key()
    }

    pub fn lock_id(&self) -> Id16 {
        self.lock.lock_id()
    }
}

type SharedLockCallback =
    Arc<Mutex<Option<Box<dyn FnOnce(Result<LockCommandResult>) + Send + 'static>>>>;

fn finish_lock_callback(callback: &SharedLockCallback, result: Result<LockCommandResult>) {
    if let Some(callback) = callback
        .lock()
        .expect("tree callback mutex poisoned")
        .take()
    {
        callback(result);
    }
}

fn accept_locked_result(value: Result<LockCommandResult>) -> Result<LockCommandResult> {
    match value {
        Ok(result) => Ok(result),
        Err(SlockError::LockLocked(result)) => Ok(*result),
        Err(err) => Err(err),
    }
}

fn encode_group_event_lock_id(client_id: u64, version_id: u64) -> Id16 {
    let mut bytes = [0u8; 16];
    bytes[0..8].copy_from_slice(&version_id.to_le_bytes());
    bytes[8..16].copy_from_slice(&client_id.to_le_bytes());
    Id16::from_bytes(bytes)
}

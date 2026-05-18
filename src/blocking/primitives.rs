use crate::blocking::database::Database;
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
            lock_key: Key16::new(key),
            lock_id: Id16::new(),
            timeout,
            expired,
            count: 0,
            r_count: 0,
            current_data: None,
        }
    }

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
            lock_key: Key16::new(key),
            lock_id,
            timeout,
            expired,
            count,
            r_count,
            current_data: None,
        }
    }

    pub fn set_count(&mut self, count: u16) {
        self.count = count;
    }

    pub fn set_r_count(&mut self, r_count: u8) {
        self.r_count = r_count;
    }

    pub fn acquire(&mut self) -> Result<LockCommandResult> {
        self.acquire_with_flags(0, None)
    }

    pub fn acquire_with_data(&mut self, data: LockData) -> Result<LockCommandResult> {
        self.acquire_with_flags(0, Some(data))
    }

    pub fn acquire_with_flags(
        &mut self,
        flag: u8,
        data: Option<LockData>,
    ) -> Result<LockCommandResult> {
        let flag = if data.is_some() {
            flag | LOCK_FLAG_CONTAINS_DATA
        } else {
            flag
        };
        let result = self.send_lock(COMMAND_TYPE_LOCK, flag, self.lock_id, data)?;
        self.apply_result(result)
    }

    pub fn release(&mut self) -> Result<LockCommandResult> {
        self.release_with_flags(0, None)
    }

    pub fn release_with_data(&mut self, data: LockData) -> Result<LockCommandResult> {
        self.release_with_flags(0, Some(data))
    }

    pub fn release_with_flags(
        &mut self,
        flag: u8,
        data: Option<LockData>,
    ) -> Result<LockCommandResult> {
        let flag = if data.is_some() {
            flag | UNLOCK_FLAG_CONTAINS_DATA
        } else {
            flag
        };
        let result = self.send_lock(COMMAND_TYPE_UNLOCK, flag, self.lock_id, data)?;
        self.apply_result(result)
    }

    pub fn show(&mut self) -> Result<Option<LockCommandResult>> {
        match self.acquire_with_flags(LOCK_FLAG_SHOW_WHEN_LOCKED, None) {
            Ok(result) => Ok(Some(result)),
            Err(SlockError::LockNotOwn(result)) => Ok(Some(*result)),
            Err(err) => Err(err),
        }
    }

    pub fn update(&mut self, data: Option<LockData>) -> Result<()> {
        match self.acquire_with_flags(LOCK_FLAG_UPDATE_WHEN_LOCKED, data) {
            Ok(_) | Err(SlockError::LockLocked(_)) => Ok(()),
            Err(err) => Err(err),
        }
    }

    pub fn release_head(&mut self, data: Option<LockData>) -> Result<()> {
        let result = self.send_lock(
            COMMAND_TYPE_UNLOCK,
            UNLOCK_FLAG_UNLOCK_FIRST_LOCK_WHEN_UNLOCKED
                | if data.is_some() {
                    UNLOCK_FLAG_CONTAINS_DATA
                } else {
                    0
                },
            Id16::zero(),
            data,
        )?;
        self.apply_result(result)?;
        Ok(())
    }

    pub fn release_head_to_lock_wait(
        &mut self,
        data: Option<LockData>,
    ) -> Result<LockCommandResult> {
        self.release_with_flags(
            UNLOCK_FLAG_UNLOCK_FIRST_LOCK_WHEN_UNLOCKED | UNLOCK_FLAG_SUCCED_TO_LOCK_WAIT,
            data,
        )
    }

    pub fn current_data(&self) -> Option<&LockResultData> {
        self.current_data.as_ref()
    }

    pub fn lock_key(&self) -> Key16 {
        self.lock_key
    }

    pub fn lock_id(&self) -> Id16 {
        self.lock_id
    }

    pub fn db_id(&self) -> u8 {
        self.database.db_id()
    }

    pub fn timeout(&self) -> PackedTime {
        self.timeout
    }

    pub fn expired(&self) -> PackedTime {
        self.expired
    }

    fn send_lock(
        &self,
        command_type: u8,
        flag: u8,
        lock_id: Id16,
        data: Option<LockData>,
    ) -> Result<LockCommandResult> {
        let command = LockCommand::new(
            command_type,
            flag,
            self.database.db_id(),
            Id16::new(),
            self.lock_key,
            lock_id,
            self.timeout,
            self.expired,
            self.count,
            self.r_count,
            data,
        );
        match self
            .database
            .client()
            .send_command(Command::Lock(command))?
        {
            CommandResult::Lock(result) => Ok(result),
            _ => Err(SlockError::Protocol("expected lock result".to_string())),
        }
    }

    fn apply_result(&mut self, result: LockCommandResult) -> Result<LockCommandResult> {
        self.current_data = result.data.clone();
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

    pub fn is_set(&mut self) -> Result<bool> {
        let mut check_lock = if self.default_set {
            Lock::new(
                self.lock.database.clone(),
                self.lock.lock_key.as_bytes(),
                0,
                0,
            )
        } else {
            let mut lock = Lock::new(
                self.lock.database.clone(),
                self.lock.lock_key.as_bytes(),
                0,
                0,
            );
            lock.timeout = PackedTime::with_flags(0, TIMEOUT_FLAG_LOCK_WAIT_WHEN_UNLOCK);
            lock.set_count(1);
            lock
        };
        match check_lock.acquire() {
            Ok(_) => Ok(true),
            Err(SlockError::LockTimeout(_)) | Err(SlockError::LockNotOwn(_)) => Ok(false),
            Err(err) => Err(err),
        }
    }

    pub fn set(&mut self) -> Result<()> {
        self.set_with_data(None)
    }

    pub fn set_with_data(&mut self, data: Option<LockData>) -> Result<()> {
        if self.default_set {
            let mut event_lock = self.event_lock(0);
            match event_lock.release_with_flags(0, data) {
                Ok(_) | Err(SlockError::LockUnlocked(_)) => Ok(()),
                Err(err) => Err(err),
            }
        } else {
            let mut event_lock = self.event_lock(1);
            event_lock.update(data)
        }
    }

    pub fn clear(&mut self) -> Result<()> {
        if self.default_set {
            let mut event_lock = self.event_lock(0);
            event_lock.update(None)
        } else {
            let mut event_lock = self.event_lock(1);
            match event_lock.release() {
                Ok(_) | Err(SlockError::LockUnlocked(_)) => Ok(()),
                Err(err) => Err(err),
            }
        }
    }

    pub fn wait(&mut self, timeout: u16) -> Result<()> {
        let mut wait_lock = Lock::new(
            self.lock.database.clone(),
            self.lock.lock_key.as_bytes(),
            timeout,
            0,
        );
        if !self.default_set {
            wait_lock.timeout = PackedTime::with_flags(timeout, TIMEOUT_FLAG_LOCK_WAIT_WHEN_UNLOCK);
            wait_lock.set_count(1);
        }
        let result = wait_lock.acquire().map(|_| ()).map_err(|err| match err {
            SlockError::LockTimeout(_) | SlockError::CommandTimeout => SlockError::EventWaitTimeout,
            err => err,
        });
        self.lock.current_data = wait_lock.current_data.clone();
        result
    }

    pub fn current_data(&self) -> Option<&LockResultData> {
        self.lock.current_data()
    }

    fn event_lock(&self, count: u16) -> Lock {
        Lock::with_lock_id(
            self.lock.database.clone(),
            self.lock.lock_key.as_bytes(),
            Id16::from_bytes(self.lock.lock_key.into_bytes()),
            self.lock.timeout.value(),
            self.lock.expired.value(),
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
        let mut lock = Lock::new(database, key, timeout, expired);
        lock.set_r_count(0xff);
        Self { lock }
    }

    pub fn acquire(&mut self) -> Result<LockCommandResult> {
        self.lock.acquire()
    }

    pub fn release(&mut self) -> Result<LockCommandResult> {
        self.lock.release()
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

    pub fn acquire(&mut self) -> Result<LockCommandResult> {
        let mut lock = Lock::new(self.database.clone(), &self.key, self.timeout, self.expired);
        lock.set_count(self.count);
        lock.acquire()
    }

    pub fn release(&mut self) -> Result<()> {
        let mut lock = Lock::with_lock_id(
            self.database.clone(),
            &self.key,
            Id16::zero(),
            self.timeout,
            self.expired,
            self.count,
            0,
        );
        lock.release_head(None)
    }
}

#[derive(Clone, Debug)]
pub struct ReadWriteLock {
    database: Database,
    key: Vec<u8>,
    timeout: u16,
    expired: u16,
    write_lock: Lock,
    read_locks: Vec<Lock>,
}

impl ReadWriteLock {
    pub(crate) fn new<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        timeout: u16,
        expired: u16,
    ) -> Self {
        let key_vec = key.as_ref().to_vec();
        let mut write_lock = Lock::new(database.clone(), &key_vec, timeout, expired);
        write_lock.set_count(0);
        Self {
            database,
            key: key_vec,
            timeout,
            expired,
            write_lock,
            read_locks: Vec::new(),
        }
    }

    pub fn acquire_write(&mut self) -> Result<LockCommandResult> {
        self.write_lock.acquire()
    }

    pub fn release_write(&mut self) -> Result<LockCommandResult> {
        self.write_lock.release()
    }

    pub fn acquire_read(&mut self) -> Result<LockCommandResult> {
        let mut lock = Lock::new(self.database.clone(), &self.key, self.timeout, self.expired);
        lock.set_count(u16::MAX);
        let result = lock.acquire()?;
        self.read_locks.push(lock);
        Ok(result)
    }

    pub fn release_read(&mut self) -> Result<LockCommandResult> {
        let mut lock = self
            .read_locks
            .pop()
            .ok_or_else(|| SlockError::LockData("no read lock to release".to_string()))?;
        lock.release()
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
        let mut lock = Lock::new(database, key, timeout, expired);
        lock.timeout = lock.timeout.merge_flags(TIMEOUT_FLAG_RCOUNT_IS_PRIORITY);
        lock.set_r_count(priority);
        Self { lock, priority }
    }

    pub fn priority(&self) -> u8 {
        self.priority
    }

    pub fn acquire(&mut self) -> Result<LockCommandResult> {
        self.lock.acquire()
    }

    pub fn release(&mut self) -> Result<LockCommandResult> {
        self.lock.release()
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
        let mut lock = Lock::new(database, key, timeout, expired);
        lock.set_count(max.saturating_sub(1));
        Self { lock }
    }

    pub fn acquire(&mut self) -> Result<LockCommandResult> {
        self.lock.acquire()
    }

    pub fn release(&mut self) -> Result<LockCommandResult> {
        self.lock.release()
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
        let mut lock = Lock::new(database, key, timeout, expired);
        lock.set_count(count.saturating_sub(1));
        if period < 3.0 {
            lock.expired = lock.expired.merge_flags(EXPRIED_FLAG_MILLISECOND_TIME);
        }
        Self { lock }
    }

    pub fn acquire(&mut self) -> Result<LockCommandResult> {
        self.lock.acquire()
    }
}

#[derive(Clone, Debug)]
pub struct GroupEvent {
    lock: Lock,
    client_id: u64,
    version_id: u64,
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
        let mut lock = Lock::with_lock_id(
            database,
            key,
            encode_group_event_lock_id(client_id, version_id),
            timeout,
            expired,
            0,
            0,
        );
        lock.timeout = lock
            .timeout
            .merge_flags(TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED);
        Self {
            lock,
            client_id,
            version_id,
        }
    }

    pub fn version_id(&self) -> u64 {
        self.version_id
    }

    pub fn is_set(&mut self) -> Result<bool> {
        let mut check_lock = Lock::with_lock_id(
            self.lock.database.clone(),
            self.lock.lock_key.as_bytes(),
            Id16::new(),
            0,
            0,
            0,
            0,
        );
        match check_lock.acquire() {
            Ok(_) => Ok(true),
            Err(SlockError::LockTimeout(_)) => Ok(false),
            Err(err) => Err(err),
        }
    }

    pub fn set(&mut self) -> Result<()> {
        let mut event_lock = Lock::with_lock_id(
            self.lock.database.clone(),
            self.lock.lock_key.as_bytes(),
            Id16::zero(),
            self.lock.timeout.value(),
            self.lock.expired.value(),
            0,
            0,
        );
        match event_lock.release_head(None) {
            Ok(_) | Err(SlockError::LockUnlocked(_)) => Ok(()),
            Err(err) => Err(err),
        }
    }

    pub fn clear(&mut self) -> Result<()> {
        let mut event_lock = Lock::with_lock_id(
            self.lock.database.clone(),
            self.lock.lock_key.as_bytes(),
            encode_group_event_lock_id(0, self.version_id),
            self.lock.timeout.value(),
            self.lock.expired.value(),
            0,
            0,
        );
        event_lock.timeout = event_lock
            .timeout
            .merge_flags(TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED);
        event_lock.update(None)
    }

    pub fn wakeup(&mut self, data: Option<LockData>) -> Result<()> {
        let mut event_lock = Lock::with_lock_id(
            self.lock.database.clone(),
            self.lock.lock_key.as_bytes(),
            Id16::zero(),
            self.lock.timeout.value(),
            self.lock.expired.value(),
            0,
            0,
        );
        event_lock.timeout = event_lock
            .timeout
            .merge_flags(TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED);
        let result = event_lock.release_head_to_lock_wait(data)?;
        if result.lock_id != Id16::zero() {
            self.version_id = u64::from_le_bytes(
                result.lock_id.as_bytes()[0..8]
                    .try_into()
                    .expect("slice is 8 bytes"),
            );
        }
        self.lock.current_data = event_lock.current_data.clone();
        Ok(())
    }

    pub fn wait(&mut self, timeout: u16) -> Result<()> {
        let mut wait_lock = Lock::with_lock_id(
            self.lock.database.clone(),
            self.lock.lock_key.as_bytes(),
            encode_group_event_lock_id(self.client_id, self.version_id),
            timeout,
            0,
            0,
            0,
        );
        wait_lock.timeout = wait_lock
            .timeout
            .merge_flags(TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED);
        let result = wait_lock.acquire().map_err(|err| match err {
            SlockError::LockTimeout(_) | SlockError::CommandTimeout => SlockError::EventWaitTimeout,
            err => err,
        })?;
        if result.lock_id != encode_group_event_lock_id(self.client_id, self.version_id) {
            self.version_id = u64::from_le_bytes(
                result.lock_id.as_bytes()[0..8]
                    .try_into()
                    .expect("slice is 8 bytes"),
            );
        }
        self.lock.current_data = wait_lock.current_data.clone();
        Ok(())
    }

    pub fn current_data(&self) -> Option<&LockResultData> {
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
        let mut lock = Lock::new(database, key, timeout, expired);
        lock.set_count(u16::MAX);
        lock.set_r_count(1);
        lock.timeout = lock.timeout.merge_flags(TIMEOUT_FLAG_RCOUNT_IS_PRIORITY);
        Self {
            lock,
            parent_key: None,
        }
    }

    pub fn new_child<K: AsRef<[u8]>>(&self, key: K) -> Self {
        let mut child = Self::new(
            self.lock.database.clone(),
            key,
            self.lock.timeout.value(),
            self.lock.expired.value(),
        );
        child.parent_key = Some(self.lock.lock_key);
        child
    }

    pub fn acquire(&mut self) -> Result<LockCommandResult> {
        self.lock.acquire_with_flags(LOCK_FLAG_LOCK_TREE_LOCK, None)
    }

    pub fn release(&mut self) -> Result<LockCommandResult> {
        self.lock
            .release_with_flags(UNLOCK_FLAG_UNLOCK_TREE_LOCK, None)
    }

    pub fn wait(&mut self, timeout: u16) -> Result<LockCommandResult> {
        let mut check_lock = Lock::with_lock_id(
            self.lock.database.clone(),
            self.lock.lock_key.as_bytes(),
            Id16::new(),
            timeout,
            0,
            0,
            0,
        );
        check_lock.acquire()
    }

    pub fn lock_key(&self) -> Key16 {
        self.lock.lock_key
    }
}

fn encode_group_event_lock_id(client_id: u64, version_id: u64) -> Id16 {
    let mut bytes = [0u8; 16];
    bytes[0..8].copy_from_slice(&version_id.to_le_bytes());
    bytes[8..16].copy_from_slice(&client_id.to_le_bytes());
    Id16::from_bytes(bytes)
}

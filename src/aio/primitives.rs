use crate::aio::database::Database;
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

    #[allow(dead_code)] // retained for crate-internal u16 callers and future parity bridges
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
        Self::with_packed_times(database, key, lock_id, timeout, expired, count, r_count)
    }

    /// Builds a lock from already-resolved packed times.
    ///
    /// Callers must merge `Database` default flags before calling this; unlike
    /// [`Lock::with_lock_id`], the full 32-bit bit pattern is preserved.
    pub(crate) fn with_packed_times<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        lock_id: Id16,
        timeout: PackedTime,
        expired: PackedTime,
        count: u16,
        r_count: u8,
    ) -> Self {
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

    pub async fn acquire(&mut self) -> Result<LockCommandResult> {
        self.acquire_with_flags(0, None).await
    }

    pub async fn acquire_guard(&mut self) -> Result<LockGuard> {
        self.acquire().await?;
        Ok(LockGuard { lock: self.clone() })
    }

    pub async fn acquire_with_data(&mut self, data: LockData) -> Result<LockCommandResult> {
        self.acquire_with_flags(0, Some(data)).await
    }

    pub async fn acquire_with_flags(
        &mut self,
        flag: u8,
        data: Option<LockData>,
    ) -> Result<LockCommandResult> {
        let flag = if data.is_some() {
            flag | LOCK_FLAG_CONTAINS_DATA
        } else {
            flag
        };
        let result = self
            .send_lock(COMMAND_TYPE_LOCK, flag, self.lock_id, data)
            .await?;
        self.apply_result(result)
    }

    pub async fn release(&mut self) -> Result<LockCommandResult> {
        self.release_with_flags(0, None).await
    }

    pub async fn release_with_data(&mut self, data: LockData) -> Result<LockCommandResult> {
        self.release_with_flags(0, Some(data)).await
    }

    pub async fn release_with_flags(
        &mut self,
        flag: u8,
        data: Option<LockData>,
    ) -> Result<LockCommandResult> {
        let flag = if data.is_some() {
            flag | UNLOCK_FLAG_CONTAINS_DATA
        } else {
            flag
        };
        let result = self
            .send_lock(COMMAND_TYPE_UNLOCK, flag, self.lock_id, data)
            .await?;
        self.apply_result(result)
    }

    pub async fn show(&mut self) -> Result<Option<LockCommandResult>> {
        match self
            .acquire_with_flags(LOCK_FLAG_SHOW_WHEN_LOCKED, None)
            .await
        {
            Ok(result) => Ok(Some(result)),
            Err(SlockError::LockNotOwn(result)) => Ok(Some(*result)),
            Err(err) => Err(err),
        }
    }

    pub async fn update(&mut self, data: Option<LockData>) -> Result<()> {
        match self
            .acquire_with_flags(LOCK_FLAG_UPDATE_WHEN_LOCKED, data)
            .await
        {
            Ok(_) | Err(SlockError::LockLocked(_)) => Ok(()),
            Err(err) => Err(err),
        }
    }

    pub async fn release_head(&mut self, data: Option<LockData>) -> Result<()> {
        let result = self
            .send_lock(
                COMMAND_TYPE_UNLOCK,
                UNLOCK_FLAG_UNLOCK_FIRST_LOCK_WHEN_UNLOCKED
                    | if data.is_some() {
                        UNLOCK_FLAG_CONTAINS_DATA
                    } else {
                        0
                    },
                Id16::zero(),
                data,
            )
            .await?;
        self.apply_result(result)?;
        Ok(())
    }

    pub async fn release_head_to_lock_wait(
        &mut self,
        data: Option<LockData>,
    ) -> Result<LockCommandResult> {
        self.release_with_flags(
            UNLOCK_FLAG_UNLOCK_FIRST_LOCK_WHEN_UNLOCKED | UNLOCK_FLAG_SUCCED_TO_LOCK_WAIT,
            data,
        )
        .await
    }

    pub fn current_data(&self) -> Option<&LockResultData> {
        self.current_data.as_ref()
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

    pub fn count(&self) -> u16 {
        self.count
    }

    pub fn r_count(&self) -> u8 {
        self.r_count
    }

    pub fn lock_key(&self) -> Key16 {
        self.lock_key
    }

    pub fn lock_id(&self) -> Id16 {
        self.lock_id
    }

    async fn send_lock(
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
        match self.database.send_command(Command::Lock(command)).await? {
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

#[derive(Debug)]
pub struct LockGuard {
    lock: Lock,
}

impl LockGuard {
    pub async fn release(mut self) -> Result<LockCommandResult> {
        self.lock.release().await
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

    /// Builds an event from already-resolved packed times, preserving the full
    /// 32-bit caller/database flag bit pattern.
    pub(crate) fn with_packed_times<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        timeout: PackedTime,
        expired: PackedTime,
        default_set: bool,
    ) -> Self {
        Self {
            lock: Lock::with_packed_times(database, key, Id16::new(), timeout, expired, 0, 0),
            default_set,
        }
    }

    pub async fn is_set(&mut self) -> Result<bool> {
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
        match check_lock.acquire().await {
            Ok(_) => Ok(true),
            Err(SlockError::LockTimeout(_)) | Err(SlockError::LockNotOwn(_)) => Ok(false),
            Err(err) => Err(err),
        }
    }

    pub async fn set(&mut self) -> Result<()> {
        self.set_with_data(None).await
    }

    pub async fn set_with_data(&mut self, data: Option<LockData>) -> Result<()> {
        if self.default_set {
            let mut event_lock = self.event_lock(0);
            match event_lock.release_with_flags(0, data).await {
                Ok(_) | Err(SlockError::LockUnlocked(_)) => Ok(()),
                Err(err) => Err(err),
            }
        } else {
            let mut event_lock = self.event_lock(1);
            event_lock.update(data).await
        }
    }

    pub async fn clear(&mut self) -> Result<()> {
        self.clear_with_data(None).await
    }

    /// Clears the event with a payload, matching Java `Event.clear(LockData)`.
    ///
    /// `defaultSet` polarity decides the wire command: a default-set event
    /// updates the event lock (`LOCK` + update flag), while a non-default event
    /// releases it (`UNLOCK`). Like Java, the facade's current data is not
    /// replaced by a clear result.
    pub async fn clear_with_data(&mut self, data: Option<LockData>) -> Result<()> {
        if self.default_set {
            let mut event_lock = self.event_lock(0);
            event_lock.update(data).await
        } else {
            let mut event_lock = self.event_lock(1);
            match event_lock.release_with_flags(0, data).await {
                Ok(_) | Err(SlockError::LockUnlocked(_)) => Ok(()),
                Err(err) => Err(err),
            }
        }
    }

    pub async fn wait(&mut self, timeout: u16) -> Result<()> {
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
        self.wait_with_lock(wait_lock).await
    }

    /// Waits using the caller's full packed timeout bit pattern.
    ///
    /// Database default timeout flags are merged with the caller's flags so the
    /// high 16 bits are never truncated.
    pub async fn wait_packed(&mut self, timeout: PackedTime) -> Result<()> {
        let timeout = self.lock.database.timeout_packed(timeout);
        let expired = self.lock.database.expired_packed(PackedTime::default());
        let (timeout, count) = if self.default_set {
            (timeout, 0)
        } else {
            (timeout.merge_flags(TIMEOUT_FLAG_LOCK_WAIT_WHEN_UNLOCK), 1)
        };
        let wait_lock = Lock::with_packed_times(
            self.lock.database.clone(),
            self.lock.lock_key.as_bytes(),
            Id16::new(),
            timeout,
            expired,
            count,
            0,
        );
        self.wait_with_lock(wait_lock).await
    }

    async fn wait_with_lock(&mut self, mut wait_lock: Lock) -> Result<()> {
        let result = wait_lock
            .acquire()
            .await
            .map(|_| ())
            .map_err(|err| match err {
                SlockError::LockTimeout(_) | SlockError::CommandTimeout => {
                    SlockError::EventWaitTimeout
                }
                err => err,
            });
        self.lock.current_data = wait_lock.current_data.clone();
        result
    }

    pub async fn wait_and_timeout_retry_clear(&mut self, timeout: u16) -> Result<()> {
        let mut wait_lock = Lock::new(
            self.lock.database.clone(),
            self.lock.lock_key.as_bytes(),
            timeout,
            0,
        );
        // Java passes a null lock id for the wait lock.
        wait_lock.lock_id = Id16::zero();
        if !self.default_set {
            wait_lock.timeout = PackedTime::with_flags(timeout, TIMEOUT_FLAG_LOCK_WAIT_WHEN_UNLOCK);
            wait_lock.set_count(1);
        }
        self.wait_and_timeout_retry_clear_with_lock(wait_lock)
            .await
    }

    /// Waits with a packed timeout and, on timeout, performs Java's
    /// compensating clear for a default-set event.
    pub async fn wait_and_timeout_retry_clear_packed(
        &mut self,
        timeout: PackedTime,
    ) -> Result<()> {
        // Java passes the caller's timeout verbatim and a literal zero
        // expired to the wait lock; default flags are applied at construction.
        let expired = PackedTime::default();
        let (timeout, count) = if self.default_set {
            (timeout, 0)
        } else {
            (timeout.merge_flags(TIMEOUT_FLAG_LOCK_WAIT_WHEN_UNLOCK), 1)
        };
        let wait_lock = Lock::with_packed_times(
            self.lock.database.clone(),
            self.lock.lock_key.as_bytes(),
            Id16::zero(),
            timeout,
            expired,
            count,
            0,
        );
        self.wait_and_timeout_retry_clear_with_lock(wait_lock)
            .await
    }

    async fn wait_and_timeout_retry_clear_with_lock(
        &mut self,
        mut wait_lock: Lock,
    ) -> Result<()> {
        match wait_lock.acquire_with_data(LockData::unset()).await {
            Ok(_) => {
                self.lock.current_data = wait_lock.current_data.clone();
                if !self.default_set {
                    let mut event_lock = self.event_lock(1);
                    let _ = event_lock.release().await;
                }
                Ok(())
            }
            Err(SlockError::LockTimeout(_)) | Err(SlockError::CommandTimeout) => {
                // Java only compensates for a default-set event; a
                // non-default event converts the timeout directly.
                if self.default_set {
                    let mut event_lock = self.event_lock(0);
                    if event_lock
                        .acquire_with_flags(LOCK_FLAG_UPDATE_WHEN_LOCKED, Some(LockData::unset()))
                        .await
                        .is_ok()
                    {
                        let _ = event_lock.release().await;
                        // The Java facade copies the *wait* lock's result data,
                        // not the compensating update's data.
                        self.lock.current_data = wait_lock.current_data.clone();
                        return Ok(());
                    }
                }
                Err(SlockError::EventWaitTimeout)
            }
            Err(err) => Err(err),
        }
    }

    pub fn current_data(&self) -> Option<&LockResultData> {
        self.lock.current_data()
    }

    fn event_lock(&self, count: u16) -> Lock {
        Lock::with_packed_times(
            self.lock.database.clone(),
            self.lock.lock_key.as_bytes(),
            Id16::from_bytes(self.lock.lock_key.into_bytes()),
            self.lock.timeout,
            self.lock.expired,
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
        let timeout = database.timeout(timeout);
        let expired = database.expired(expired);
        Self::with_packed_times(database, key, Id16::new(), timeout, expired)
    }

    /// Builds a reentrant lock from already-resolved packed times and an
    /// explicit lock identity. The Java facade reuses one lock ID for every
    /// acquire/release, so bindings must preserve it across operations.
    pub(crate) fn with_packed_times<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        lock_id: Id16,
        timeout: PackedTime,
        expired: PackedTime,
    ) -> Self {
        Self {
            lock: Lock::with_packed_times(database, key, lock_id, timeout, expired, 0, 0xff),
        }
    }

    pub fn db_id(&self) -> u8 {
        self.lock.db_id()
    }

    pub fn lock_key(&self) -> Key16 {
        self.lock.lock_key()
    }

    pub fn lock_id(&self) -> Id16 {
        self.lock.lock_id()
    }

    pub fn timeout(&self) -> PackedTime {
        self.lock.timeout()
    }

    pub fn expired(&self) -> PackedTime {
        self.lock.expired()
    }

    pub fn current_data(&self) -> Option<&LockResultData> {
        self.lock.current_data()
    }

    pub async fn acquire(&mut self) -> Result<LockCommandResult> {
        self.lock.acquire().await
    }

    pub async fn release(&mut self) -> Result<LockCommandResult> {
        self.lock.release().await
    }
}

#[derive(Clone, Debug)]
pub struct Semaphore {
    database: Database,
    key: Key16,
    count: u16,
    timeout: PackedTime,
    expired: PackedTime,
}

impl Semaphore {
    pub(crate) fn new<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        count: u16,
        timeout: u16,
        expired: u16,
    ) -> Self {
        let timeout = database.timeout(timeout);
        let expired = database.expired(expired);
        Self::with_packed_times(database, key, count, timeout, expired)
    }

    /// Builds a semaphore from already-resolved packed times. The raw `count`
    /// is converted the same way as the Java constructor (`count - 1`).
    pub(crate) fn with_packed_times<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        count: u16,
        timeout: PackedTime,
        expired: PackedTime,
    ) -> Self {
        Self {
            database,
            key: Key16::new(key),
            count: count.saturating_sub(1),
            timeout,
            expired,
        }
    }

    pub fn db_id(&self) -> u8 {
        self.database.db_id()
    }

    pub fn lock_key(&self) -> Key16 {
        self.key
    }

    pub fn count(&self) -> u16 {
        self.count
    }

    pub fn timeout(&self) -> PackedTime {
        self.timeout
    }

    pub fn expired(&self) -> PackedTime {
        self.expired
    }

    pub async fn acquire(&mut self) -> Result<LockCommandResult> {
        let mut lock = Lock::with_packed_times(
            self.database.clone(),
            self.key.as_bytes(),
            Id16::new(),
            self.timeout,
            self.expired,
            self.count,
            0,
        );
        lock.acquire().await
    }

    pub async fn release(&mut self) -> Result<()> {
        let mut lock = Lock::with_packed_times(
            self.database.clone(),
            self.key.as_bytes(),
            Id16::zero(),
            self.timeout,
            self.expired,
            self.count,
            0,
        );
        lock.release_head(None).await
    }
}

#[derive(Clone, Debug)]
pub struct ReadWriteLock {
    database: Database,
    key: Key16,
    timeout: PackedTime,
    expired: PackedTime,
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
        let timeout = database.timeout(timeout);
        let expired = database.expired(expired);
        Self::with_packed_times(database, key, timeout, expired)
    }

    /// Builds a read/write lock from already-resolved packed times.
    pub(crate) fn with_packed_times<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        timeout: PackedTime,
        expired: PackedTime,
    ) -> Self {
        Self::with_lock_id_packed(database, key, Id16::new(), timeout, expired)
    }

    /// Builds a read/write lock with an explicit write-lock identity so
    /// bindings can reuse the same write lock across snapshots.
    pub(crate) fn with_lock_id_packed<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        write_lock_id: Id16,
        timeout: PackedTime,
        expired: PackedTime,
    ) -> Self {
        let key = Key16::new(key);
        let write_lock = Lock::with_packed_times(
            database.clone(),
            key.as_bytes(),
            write_lock_id,
            timeout,
            expired,
            0,
            0,
        );
        Self {
            database,
            key,
            timeout,
            expired,
            write_lock,
            read_locks: Vec::new(),
        }
    }

    pub fn db_id(&self) -> u8 {
        self.database.db_id()
    }

    pub fn lock_key(&self) -> Key16 {
        self.key
    }

    pub fn timeout(&self) -> PackedTime {
        self.timeout
    }

    pub fn expired(&self) -> PackedTime {
        self.expired
    }

    pub fn write_lock_id(&self) -> Id16 {
        self.write_lock.lock_id()
    }

    pub fn read_lock_count(&self) -> usize {
        self.read_locks.len()
    }

    pub async fn acquire_write(&mut self) -> Result<LockCommandResult> {
        self.write_lock.acquire().await
    }

    pub async fn release_write(&mut self) -> Result<LockCommandResult> {
        self.write_lock.release().await
    }

    pub async fn acquire_read(&mut self) -> Result<LockCommandResult> {
        let mut lock = Lock::with_packed_times(
            self.database.clone(),
            self.key.as_bytes(),
            Id16::new(),
            self.timeout,
            self.expired,
            u16::MAX,
            0,
        );
        let result = lock.acquire().await?;
        self.read_locks.push(lock);
        Ok(result)
    }

    pub async fn release_read(&mut self) -> Result<LockCommandResult> {
        // Java releases read locks in acquisition (FIFO) order.
        if self.read_locks.is_empty() {
            return Err(SlockError::LockData(
                "no read lock to release".to_string(),
            ));
        }
        let mut lock = self.read_locks.remove(0);
        lock.release().await
    }

    /// Acquires a read lock with an explicit identity. Bindings keep the FIFO
    /// queue themselves so a snapshot does not lose ownership of earlier reads.
    pub(crate) async fn acquire_read_with_id(
        &mut self,
        lock_id: Id16,
    ) -> Result<LockCommandResult> {
        let mut lock = Lock::with_packed_times(
            self.database.clone(),
            self.key.as_bytes(),
            lock_id,
            self.timeout,
            self.expired,
            u16::MAX,
            0,
        );
        lock.acquire().await
    }

    pub(crate) async fn release_read_with_id(
        &mut self,
        lock_id: Id16,
    ) -> Result<LockCommandResult> {
        let mut lock = Lock::with_packed_times(
            self.database.clone(),
            self.key.as_bytes(),
            lock_id,
            self.timeout,
            self.expired,
            u16::MAX,
            0,
        );
        lock.release().await
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
        let timeout = database.timeout(timeout);
        let expired = database.expired(expired);
        Self::with_packed_times(database, key, Id16::new(), priority, 0, timeout, expired)
    }

    /// Builds a priority lock from already-resolved packed times. `count` is
    /// the raw Java constructor value and is decremented here.
    pub(crate) fn with_packed_times<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        lock_id: Id16,
        priority: u8,
        count: u16,
        timeout: PackedTime,
        expired: PackedTime,
    ) -> Self {
        let count = count.saturating_sub(1);
        let timeout = timeout.merge_flags(TIMEOUT_FLAG_RCOUNT_IS_PRIORITY);
        Self {
            lock: Lock::with_packed_times(
                database,
                key,
                lock_id,
                timeout,
                expired,
                count,
                priority,
            ),
            priority,
        }
    }

    pub fn priority(&self) -> u8 {
        self.priority
    }

    pub fn lock_id(&self) -> Id16 {
        self.lock.lock_id()
    }

    pub fn lock_key(&self) -> Key16 {
        self.lock.lock_key()
    }

    pub fn timeout(&self) -> PackedTime {
        self.lock.timeout()
    }

    pub fn expired(&self) -> PackedTime {
        self.lock.expired()
    }

    pub fn count(&self) -> u16 {
        self.lock.count()
    }

    pub fn set_priority(&mut self, priority: u8) {
        self.priority = priority;
        self.lock.set_r_count(priority);
    }

    pub async fn acquire(&mut self) -> Result<LockCommandResult> {
        self.lock.acquire().await
    }

    pub async fn release(&mut self) -> Result<LockCommandResult> {
        self.lock.release().await
    }
}

#[derive(Clone, Debug)]
pub struct MaxConcurrentFlow {
    lock: Lock,
    max: u16,
    priority: u8,
}

impl MaxConcurrentFlow {
    pub(crate) fn new<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        max: u16,
        timeout: u16,
        expired: u16,
    ) -> Self {
        let timeout = database.timeout(timeout);
        let expired = database.expired(expired);
        Self::with_packed_times(database, key, Id16::new(), max, 0, timeout, expired)
    }

    /// Builds a max-concurrency flow from already-resolved packed times.
    /// `max` is the raw Java count and is decremented here.
    pub(crate) fn with_packed_times<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        lock_id: Id16,
        max: u16,
        priority: u8,
        timeout: PackedTime,
        expired: PackedTime,
    ) -> Self {
        let count = max.saturating_sub(1);
        let timeout = if priority > 0 {
            timeout.merge_flags(TIMEOUT_FLAG_RCOUNT_IS_PRIORITY)
        } else {
            timeout
        };
        Self {
            lock: Lock::with_packed_times(
                database, key, lock_id, timeout, expired, count, priority,
            ),
            max,
            priority,
        }
    }

    pub fn max(&self) -> u16 {
        self.max
    }

    pub fn priority(&self) -> u8 {
        self.priority
    }

    pub fn lock_id(&self) -> Id16 {
        self.lock.lock_id()
    }

    pub fn lock_key(&self) -> Key16 {
        self.lock.lock_key()
    }

    pub fn count(&self) -> u16 {
        self.lock.count()
    }

    pub fn set_priority(&mut self, priority: u8) {
        self.priority = priority;
        self.lock.set_r_count(priority);
        if priority > 0 {
            self.lock.timeout = self
                .lock
                .timeout
                .merge_flags(TIMEOUT_FLAG_RCOUNT_IS_PRIORITY);
        }
    }

    pub async fn acquire(&mut self) -> Result<LockCommandResult> {
        self.lock.acquire().await
    }

    pub async fn release(&mut self) -> Result<LockCommandResult> {
        self.lock.release().await
    }
}

#[derive(Clone, Debug)]
pub struct TokenBucketFlow {
    database: Database,
    key: Key16,
    count: u16,
    timeout: PackedTime,
    period: f64,
    priority: u8,
}

impl TokenBucketFlow {
    pub(crate) fn new<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        count: u16,
        timeout: u16,
        period: f64,
    ) -> Self {
        // The Java token bucket constructor only merges the database timeout
        // flags; the expired value is always derived from `period`.
        let timeout = database.timeout(timeout);
        Self::with_packed_times(database, key, count, timeout, period, 0)
    }

    /// Builds a token bucket flow from already-resolved packed timeout bits.
    pub(crate) fn with_packed_times<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        count: u16,
        timeout: PackedTime,
        period: f64,
        priority: u8,
    ) -> Self {
        Self {
            database,
            key: Key16::new(key),
            count: count.saturating_sub(1),
            timeout,
            period,
            priority,
        }
    }

    pub fn count(&self) -> u16 {
        self.count
    }

    pub fn period(&self) -> f64 {
        self.period
    }

    pub fn priority(&self) -> u8 {
        self.priority
    }

    pub fn lock_key(&self) -> Key16 {
        self.key
    }

    pub fn set_priority(&mut self, priority: u8) {
        self.priority = priority;
    }

    fn priority_timeout(&self) -> PackedTime {
        if self.priority > 0 {
            self.timeout
                .merge_flags(TIMEOUT_FLAG_RCOUNT_IS_PRIORITY)
        } else {
            self.timeout
        }
    }

    pub async fn acquire(&mut self) -> Result<LockCommandResult> {
        if self.period < 3.0 {
            let expired = PackedTime::with_flags(
                ((self.period * 1000.0).ceil() as u16).max(1),
                EXPRIED_FLAG_MILLISECOND_TIME,
            );
            let mut lock = Lock::with_packed_times(
                self.database.clone(),
                self.key.as_bytes(),
                Id16::new(),
                self.priority_timeout(),
                expired,
                self.count,
                0,
            );
            return lock.acquire().await;
        }

        let period_secs = self.period.ceil() as u64;
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
            self.database.clone(),
            self.key.as_bytes(),
            Id16::new(),
            PackedTime::default(),
            expired,
            self.count,
            0,
        );
        match lock.acquire().await {
            Ok(result) => Ok(result),
            Err(SlockError::LockTimeout(_)) => {
                let expired =
                    PackedTime::new(period_secs.min(u16::MAX as u64) as u16);
                let mut retry = Lock::with_packed_times(
                    self.database.clone(),
                    self.key.as_bytes(),
                    Id16::new(),
                    self.priority_timeout(),
                    expired,
                    self.count,
                    0,
                );
                retry.acquire().await
            }
            Err(err) => Err(err),
        }
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
        let timeout = database.timeout(timeout);
        let expired = database.expired(expired);
        Self::with_packed_times(database, key, client_id, version_id, timeout, expired)
    }

    /// Builds a group event from already-resolved packed times. The base
    /// timeout/expired values do not carry the version flag; each operation
    /// adds it where the Java implementation does.
    pub(crate) fn with_packed_times<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        client_id: u64,
        version_id: u64,
        timeout: PackedTime,
        expired: PackedTime,
    ) -> Self {
        Self {
            lock: Lock::with_packed_times(
                database,
                key,
                encode_group_event_lock_id(client_id, version_id),
                timeout,
                expired,
                0,
                0,
            ),
            client_id,
            version_id,
        }
    }

    pub fn client_id(&self) -> u64 {
        self.client_id
    }

    pub fn version_id(&self) -> u64 {
        self.version_id
    }

    pub fn lock_key(&self) -> Key16 {
        self.lock.lock_key()
    }

    pub fn timeout(&self) -> PackedTime {
        self.lock.timeout()
    }

    pub fn expired(&self) -> PackedTime {
        self.lock.expired()
    }

    fn set_version_from_lock_id(&mut self, lock_id: Id16) {
        if lock_id != encode_group_event_lock_id(self.client_id, self.version_id) {
            self.version_id =
                u64::from_le_bytes(lock_id.as_bytes()[0..8].try_into().expect("slice is 8 bytes"));
        }
    }

    pub async fn is_set(&mut self) -> Result<bool> {
        // The Java facade checks with literal zero timeout/expired values; it
        // does not merge database defaults into this probe.
        let mut check_lock = Lock::with_packed_times(
            self.lock.database.clone(),
            self.lock.lock_key.as_bytes(),
            Id16::new(),
            PackedTime::default(),
            PackedTime::default(),
            0,
            0,
        );
        match check_lock.acquire().await {
            Ok(_) => Ok(true),
            Err(SlockError::LockTimeout(_)) => Ok(false),
            Err(err) => Err(err),
        }
    }

    pub async fn set(&mut self) -> Result<()> {
        self.set_inner(None).await
    }

    /// Sets the event with a payload, matching Java `GroupEvent.set(LockData)`.
    /// The facade's current lock data is intentionally left untouched.
    pub async fn set_with_data(&mut self, data: LockData) -> Result<()> {
        self.set_inner(Some(data)).await
    }

    async fn set_inner(&mut self, data: Option<LockData>) -> Result<()> {
        let mut event_lock = Lock::with_packed_times(
            self.lock.database.clone(),
            self.lock.lock_key.as_bytes(),
            Id16::zero(),
            self.lock.timeout,
            self.lock.expired,
            0,
            0,
        );
        match event_lock.release_head(data).await {
            Ok(_) | Err(SlockError::LockUnlocked(_)) => Ok(()),
            Err(err) => Err(err),
        }
    }

    pub async fn clear(&mut self) -> Result<()> {
        self.clear_inner(None).await
    }

    /// Clears the event with a payload, matching Java
    /// `GroupEvent.clear(LockData)`. Like Java, this does not update the
    /// facade's current lock data.
    pub async fn clear_with_data(&mut self, data: LockData) -> Result<()> {
        self.clear_inner(Some(data)).await
    }

    async fn clear_inner(&mut self, data: Option<LockData>) -> Result<()> {
        let mut event_lock = Lock::with_packed_times(
            self.lock.database.clone(),
            self.lock.lock_key.as_bytes(),
            encode_group_event_lock_id(0, self.version_id),
            self.lock
                .timeout
                .merge_flags(TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED),
            self.lock.expired,
            0,
            0,
        );
        event_lock.update(data).await
    }

    pub async fn wakeup(&mut self, data: Option<LockData>) -> Result<()> {
        let mut event_lock = Lock::with_packed_times(
            self.lock.database.clone(),
            self.lock.lock_key.as_bytes(),
            Id16::zero(),
            self.lock
                .timeout
                .merge_flags(TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED),
            self.lock.expired,
            0,
            0,
        );
        let result = event_lock.release_head_to_lock_wait(data).await?;
        if result.lock_id != Id16::zero() {
            self.set_version_from_lock_id(result.lock_id);
        }
        // Java's wakeup does not update the facade's currentLockData.
        Ok(())
    }

    pub async fn wait(&mut self, timeout: u16) -> Result<()> {
        self.wait_packed(PackedTime::new(timeout)).await
    }

    /// Waits using the caller's full packed timeout bits. The Java facade
    /// passes the wait argument through unchanged, so database defaults are
    /// not merged here.
    pub async fn wait_packed(&mut self, timeout: PackedTime) -> Result<()> {
        let mut wait_lock = Lock::with_packed_times(
            self.lock.database.clone(),
            self.lock.lock_key.as_bytes(),
            encode_group_event_lock_id(self.client_id, self.version_id),
            timeout.merge_flags(TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED),
            PackedTime::default(),
            0,
            0,
        );
        let result = wait_lock.acquire().await.map_err(|err| match err {
            SlockError::LockTimeout(_) | SlockError::CommandTimeout => SlockError::EventWaitTimeout,
            err => err,
        })?;
        self.set_version_from_lock_id(result.lock_id);
        self.lock.current_data = wait_lock.current_data.clone();
        Ok(())
    }

    pub async fn wait_and_timeout_retry_clear(&mut self, timeout: u16) -> Result<()> {
        self.wait_and_timeout_retry_clear_packed(PackedTime::new(timeout))
            .await
    }

    /// Java's timed wait path: on a wait timeout, try to acquire the event lock
    /// with the update flag and unset data, then release it. If that
    /// compensation also fails, surface an event wait timeout.
    pub async fn wait_and_timeout_retry_clear_packed(
        &mut self,
        timeout: PackedTime,
    ) -> Result<()> {
        let mut wait_lock = Lock::with_packed_times(
            self.lock.database.clone(),
            self.lock.lock_key.as_bytes(),
            encode_group_event_lock_id(self.client_id, self.version_id),
            timeout.merge_flags(TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED),
            PackedTime::default(),
            0,
            0,
        );
        match wait_lock.acquire_with_data(LockData::unset()).await {
            Ok(result) => {
                self.set_version_from_lock_id(result.lock_id);
                self.lock.current_data = wait_lock.current_data.clone();
                Ok(())
            }
            Err(SlockError::LockTimeout(_)) | Err(SlockError::CommandTimeout) => {
                let mut event_lock = Lock::with_packed_times(
                    self.lock.database.clone(),
                    self.lock.lock_key.as_bytes(),
                    encode_group_event_lock_id(0, self.version_id),
                    self.lock
                        .timeout
                        .merge_flags(TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED),
                    self.lock.expired,
                    0,
                    0,
                );
                if event_lock
                    .acquire_with_flags(LOCK_FLAG_UPDATE_WHEN_LOCKED, Some(LockData::unset()))
                    .await
                    .is_ok()
                {
                    let _ = event_lock.release().await;
                    self.lock.current_data = event_lock.current_data.clone();
                    return Ok(());
                }
                Err(SlockError::EventWaitTimeout)
            }
            Err(err) => Err(err),
        }
    }

    pub fn current_data(&self) -> Option<&LockResultData> {
        self.lock.current_data()
    }
}

#[derive(Clone, Debug)]
pub struct TreeLock {
    lock: Lock,
    parent_key: Option<Key16>,
    timeout: PackedTime,
    expired: PackedTime,
}

impl TreeLock {
    pub(crate) fn new<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        timeout: u16,
        expired: u16,
    ) -> Self {
        let timeout = database.timeout(timeout);
        let expired = database.expired(expired);
        Self::with_packed_times(database, key, None, timeout, expired)
    }

    /// Builds a tree lock from already-resolved packed times. The stored
    /// timeout/expired values are the base values; the priority flag is only
    /// applied to leaf and aggregate locks.
    pub(crate) fn with_packed_times<K: AsRef<[u8]>>(
        database: Database,
        key: K,
        parent_key: Option<Key16>,
        timeout: PackedTime,
        expired: PackedTime,
    ) -> Self {
        let mut lock = Lock::with_packed_times(
            database,
            key,
            Id16::new(),
            timeout,
            expired,
            u16::MAX,
            1,
        );
        lock.timeout = lock
            .timeout
            .merge_flags(TIMEOUT_FLAG_RCOUNT_IS_PRIORITY);
        Self {
            lock,
            parent_key,
            timeout,
            expired,
        }
    }

    pub fn new_child(&self) -> Self {
        self.load_child(Key16::from_bytes(Id16::new().into_bytes()).as_bytes())
    }

    pub fn new_child_with_key<K: AsRef<[u8]>>(&self, key: K) -> Self {
        self.load_child(key)
    }

    pub fn load_child<K: AsRef<[u8]>>(&self, key: K) -> Self {
        Self::with_packed_times(
            self.lock.database.clone(),
            key,
            Some(self.lock.lock_key),
            self.timeout,
            self.expired,
        )
    }

    pub fn new_leaf_lock(&self) -> TreeLeafLock {
        self.load_leaf_lock(Id16::new())
    }

    pub fn load_leaf_lock(&self, lock_id: Id16) -> TreeLeafLock {
        let lock = Lock::with_packed_times(
            self.lock.database.clone(),
            self.lock.lock_key.as_bytes(),
            lock_id,
            self.timeout.merge_flags(TIMEOUT_FLAG_RCOUNT_IS_PRIORITY),
            self.expired,
            u16::MAX,
            1,
        );
        TreeLeafLock {
            tree_lock: self.clone(),
            lock,
        }
    }

    pub async fn acquire(&mut self) -> Result<LockCommandResult> {
        self.lock
            .acquire_with_flags(LOCK_FLAG_LOCK_TREE_LOCK, None)
            .await
    }

    pub async fn release(&mut self) -> Result<LockCommandResult> {
        self.lock
            .release_with_flags(UNLOCK_FLAG_UNLOCK_TREE_LOCK, None)
            .await
    }

    pub async fn wait(&mut self, timeout: u16) -> Result<LockCommandResult> {
        self.wait_packed(PackedTime::new(timeout)).await
    }

    /// The Java `TreeLock.wait` probe uses the raw caller timeout and zero
    /// expired bits; database defaults are not merged into the probe.
    pub async fn wait_packed(&mut self, timeout: PackedTime) -> Result<LockCommandResult> {
        let mut check_lock = Lock::with_packed_times(
            self.lock.database.clone(),
            self.lock.lock_key.as_bytes(),
            Id16::new(),
            timeout,
            PackedTime::default(),
            0,
            0,
        );
        check_lock.acquire().await
    }

    /// The check lock acquired by the Java `TreeLock.acquire` before it takes
    /// its cached leaf lock.
    pub async fn acquire_check(&mut self) -> Result<LockCommandResult> {
        let mut check_lock = Lock::with_packed_times(
            self.lock.database.clone(),
            self.lock.lock_key.as_bytes(),
            Id16::new(),
            self.timeout,
            PackedTime::default(),
            0,
            0,
        );
        check_lock.acquire().await
    }

    pub fn lock_key(&self) -> Key16 {
        self.lock.lock_key
    }

    pub fn parent_key(&self) -> Option<Key16> {
        self.parent_key
    }

    pub fn is_root(&self) -> bool {
        self.parent_key.is_none()
    }

    pub fn timeout(&self) -> PackedTime {
        self.timeout
    }

    pub fn expired(&self) -> PackedTime {
        self.expired
    }
}

#[derive(Clone, Debug)]
pub struct TreeLeafLock {
    tree_lock: TreeLock,
    lock: Lock,
}

impl TreeLeafLock {
    pub async fn acquire(&mut self) -> Result<LockCommandResult> {
        let mut child_check_lock = None;
        let mut parent_check_lock = None;

        if let Some(parent_key) = self.tree_lock.parent_key {
            let mut check = Lock::with_packed_times(
                self.tree_lock.lock.database.clone(),
                self.tree_lock.lock.lock_key.as_bytes(),
                Id16::from_bytes(parent_key.into_bytes()),
                PackedTime::with_flags(0, TIMEOUT_FLAG_RCOUNT_IS_PRIORITY),
                self.tree_lock.expired,
                u16::MAX,
                1,
            );
            // Java only attempts the parent check when the child check
            // succeeds with the tree-lock flag; a locked child check is
            // swallowed and skips straight to the leaf acquire.
            match check
                .acquire_with_flags(LOCK_FLAG_LOCK_TREE_LOCK, None)
                .await
            {
                Ok(_) => child_check_lock = Some(check),
                Err(SlockError::LockLocked(_)) => {}
                Err(err) => return Err(err),
            }

            if child_check_lock.is_some() {
                let mut check = Lock::with_packed_times(
                    self.tree_lock.lock.database.clone(),
                    parent_key.as_bytes(),
                    Id16::from_bytes(self.tree_lock.lock.lock_key.into_bytes()),
                    PackedTime::with_flags(0, TIMEOUT_FLAG_RCOUNT_IS_PRIORITY),
                    self.tree_lock.expired,
                    u16::MAX,
                    1,
                );
                match check.acquire().await {
                    Ok(_) | Err(SlockError::LockLocked(_)) => {}
                    Err(err) => {
                        if let Some(mut lock) = child_check_lock {
                            let _ = lock.release().await;
                        }
                        return Err(err);
                    }
                }
                parent_check_lock = Some(check);
            }
        }

        match self.lock.acquire().await {
            Ok(result) => Ok(result),
            Err(err) => {
                if let Some(mut lock) = child_check_lock {
                    let _ = lock.release().await;
                }
                if let Some(mut lock) = parent_check_lock {
                    let _ = lock.release().await;
                }
                Err(err)
            }
        }
    }

    pub async fn release(&mut self) -> Result<LockCommandResult> {
        self.lock
            .release_with_flags(UNLOCK_FLAG_UNLOCK_TREE_LOCK, None)
            .await
    }

    pub fn lock_key(&self) -> Key16 {
        self.lock.lock_key
    }

    pub fn lock_id(&self) -> Id16 {
        self.lock.lock_id
    }
}

fn encode_group_event_lock_id(client_id: u64, version_id: u64) -> Id16 {
    let mut bytes = [0u8; 16];
    bytes[0..8].copy_from_slice(&version_id.to_le_bytes());
    bytes[8..16].copy_from_slice(&client_id.to_le_bytes());
    Id16::from_bytes(bytes)
}

use std::sync::{Arc, Mutex};

use crate::blocking::client::Client;
use crate::blocking::database::Database;
use crate::blocking::primitives::{
    Event, GroupEvent, MaxConcurrentFlow, PriorityLock, ReadWriteLock, ReentrantLock, Semaphore,
    TokenBucketFlow, TreeLock,
};
use crate::data::{LockData, LockResultData};
use crate::error::{Result, SlockError};
use crate::key::Key16;
use crate::options::ClientOptions;
use crate::protocol::command::{Command, LockCommand};
use crate::protocol::constants::*;
use crate::protocol::id::Id16;
use crate::protocol::result::{CommandResult, LockCommandResult};
use crate::time::PackedTime;

pub trait IntoNodeList {
    fn into_nodes(self) -> Vec<String>;
}

impl IntoNodeList for &str {
    fn into_nodes(self) -> Vec<String> {
        self.split(',')
            .map(str::trim)
            .filter(|node| !node.is_empty())
            .map(ToOwned::to_owned)
            .collect()
    }
}

impl IntoNodeList for String {
    fn into_nodes(self) -> Vec<String> {
        self.as_str().into_nodes()
    }
}

impl<const N: usize> IntoNodeList for [&str; N] {
    fn into_nodes(self) -> Vec<String> {
        self.into_iter().map(ToOwned::to_owned).collect()
    }
}

impl IntoNodeList for Vec<String> {
    fn into_nodes(self) -> Vec<String> {
        self
    }
}

#[derive(Clone, Debug)]
pub struct ReplsetClient {
    nodes: Vec<String>,
    options: ClientOptions,
    clients: Vec<Client>,
    active_index: Arc<Mutex<Option<usize>>>,
}

impl ReplsetClient {
    pub fn new<N: IntoNodeList>(nodes: N) -> Self {
        Self::with_options(nodes, ClientOptions::default())
    }

    pub fn with_options<N: IntoNodeList>(nodes: N, options: ClientOptions) -> Self {
        let nodes = nodes.into_nodes();
        let clients = nodes
            .iter()
            .map(|node| Client::with_options(node, options.clone()))
            .collect();
        Self {
            nodes,
            clients,
            active_index: Arc::new(Mutex::new(None)),
            options,
        }
    }

    pub fn connect<N: IntoNodeList>(nodes: N) -> Result<Self> {
        let client = Self::new(nodes);
        client.open()?;
        Ok(client)
    }

    pub fn nodes(&self) -> &[String] {
        &self.nodes
    }

    pub fn open(&self) -> Result<()> {
        if self.nodes.is_empty() {
            return Err(SlockError::NotConnected);
        }
        let mut last_error = None;
        let mut first_live = None;
        let mut leader = None;
        for (index, client) in self.clients.iter().enumerate() {
            match client.open() {
                Ok(()) => {
                    first_live.get_or_insert(index);
                    if (client.init_type() & INIT_TYPE_FLAG_IS_LEADER) != 0 {
                        leader = Some(index);
                    }
                }
                Err(err) => last_error = Some(err),
            }
        }
        if let Some(index) = leader.or(first_live) {
            *self
                .active_index
                .lock()
                .expect("replset active index mutex poisoned") = Some(index);
            Ok(())
        } else {
            Err(last_error.unwrap_or(SlockError::NotConnected))
        }
    }

    pub fn close(&self) {
        for client in &self.clients {
            client.close();
        }
        *self
            .active_index
            .lock()
            .expect("replset active index mutex poisoned") = None;
    }

    pub fn ping(&self) -> Result<bool> {
        match self.client().ping() {
            Ok(result) => Ok(result),
            Err(err) if self.clients.len() > 1 => {
                self.open()?;
                self.client().ping().map_err(|_| err)
            }
            Err(err) => Err(err),
        }
    }

    pub fn select_database(&self, db_id: u8) -> Database {
        self.client().select_database(db_id)
    }

    pub fn lock<K: AsRef<[u8]>>(&self, key: K, timeout: u16, expired: u16) -> ReplsetLock {
        ReplsetLock::new(self.clone(), 0, key, timeout, expired)
    }

    pub fn event<K: AsRef<[u8]>>(
        &self,
        key: K,
        timeout: u16,
        expired: u16,
        default_set: bool,
    ) -> Event {
        self.client().event(key, timeout, expired, default_set)
    }

    pub fn group_event<K: AsRef<[u8]>>(
        &self,
        key: K,
        client_id: u64,
        version_id: u64,
        timeout: u16,
        expired: u16,
    ) -> GroupEvent {
        self.client()
            .group_event(key, client_id, version_id, timeout, expired)
    }

    pub fn semaphore<K: AsRef<[u8]>>(
        &self,
        key: K,
        count: u16,
        timeout: u16,
        expired: u16,
    ) -> Semaphore {
        self.client().semaphore(key, count, timeout, expired)
    }

    pub fn reentrant_lock<K: AsRef<[u8]>>(
        &self,
        key: K,
        timeout: u16,
        expired: u16,
    ) -> ReentrantLock {
        self.client().reentrant_lock(key, timeout, expired)
    }

    pub fn read_write_lock<K: AsRef<[u8]>>(
        &self,
        key: K,
        timeout: u16,
        expired: u16,
    ) -> ReadWriteLock {
        self.client().read_write_lock(key, timeout, expired)
    }

    pub fn priority_lock<K: AsRef<[u8]>>(
        &self,
        key: K,
        priority: u8,
        timeout: u16,
        expired: u16,
    ) -> PriorityLock {
        self.client().priority_lock(key, priority, timeout, expired)
    }

    pub fn max_concurrent_flow<K: AsRef<[u8]>>(
        &self,
        key: K,
        max: u16,
        timeout: u16,
        expired: u16,
    ) -> MaxConcurrentFlow {
        self.client()
            .max_concurrent_flow(key, max, timeout, expired)
    }

    pub fn token_bucket_flow<K: AsRef<[u8]>>(
        &self,
        key: K,
        count: u16,
        timeout: u16,
        period: f64,
    ) -> TokenBucketFlow {
        self.client().token_bucket_flow(key, count, timeout, period)
    }

    pub fn tree_lock<K: AsRef<[u8]>>(&self, key: K, timeout: u16, expired: u16) -> TreeLock {
        self.client().tree_lock(key, timeout, expired)
    }

    pub fn options(&self) -> &ClientOptions {
        &self.options
    }

    fn client(&self) -> Client {
        let active_index = *self
            .active_index
            .lock()
            .expect("replset active index mutex poisoned");
        self.clients
            .get(active_index.unwrap_or(0))
            .cloned()
            .unwrap_or_else(|| Client::with_options("", self.options.clone()))
    }

    fn set_active_index(&self, index: usize) {
        *self
            .active_index
            .lock()
            .expect("replset active index mutex poisoned") = Some(index);
    }

    pub(crate) fn send_command(&self, command: Command) -> Result<CommandResult> {
        if self.clients.is_empty() {
            return Err(SlockError::NotConnected);
        }
        let start = self
            .active_index
            .lock()
            .expect("replset active index mutex poisoned")
            .unwrap_or(0);
        let mut last_error = None;
        let mut last_state_error = None;
        for offset in 0..self.clients.len() {
            let index = (start + offset) % self.clients.len();
            let client = &self.clients[index];
            if let Err(err) = client.open() {
                last_error = Some(err);
                continue;
            }
            match client.send_command(command.clone()) {
                Ok(result) if lock_state_error(&result) && self.clients.len() > 1 => {
                    last_state_error = Some(result);
                }
                Ok(result) => {
                    self.set_active_index(index);
                    return Ok(result);
                }
                Err(err) if retryable_transport_error(&err) && self.clients.len() > 1 => {
                    client.close();
                    last_error = Some(err);
                }
                Err(err) => return Err(err),
            }
        }
        if let Some(result) = last_state_error {
            Ok(result)
        } else {
            Err(last_error.unwrap_or(SlockError::NotConnected))
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReplsetLock {
    client: ReplsetClient,
    db_id: u8,
    lock_key: Key16,
    lock_id: Id16,
    timeout: PackedTime,
    expired: PackedTime,
    count: u16,
    r_count: u8,
    current_data: Option<LockResultData>,
}

impl ReplsetLock {
    pub(crate) fn new<K: AsRef<[u8]>>(
        client: ReplsetClient,
        db_id: u8,
        key: K,
        timeout: u16,
        expired: u16,
    ) -> Self {
        Self {
            client,
            db_id,
            lock_key: Key16::new(key),
            lock_id: Id16::new(),
            timeout: PackedTime::new(timeout),
            expired: PackedTime::new(expired),
            count: 0,
            r_count: 0,
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
            self.db_id,
            Id16::new(),
            self.lock_key,
            lock_id,
            self.timeout,
            self.expired,
            self.count,
            self.r_count,
            data,
        );
        match self.client.send_command(Command::Lock(command))? {
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

fn lock_state_error(result: &CommandResult) -> bool {
    matches!(
        result,
        CommandResult::Lock(result) if result.result == COMMAND_RESULT_STATE_ERROR
    )
}

fn retryable_transport_error(err: &SlockError) -> bool {
    matches!(
        err,
        SlockError::Io(_) | SlockError::NotConnected | SlockError::ClientClosed
    )
}

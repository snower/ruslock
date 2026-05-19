use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::aio::api::{BoxFuture, ClientApi};
use crate::aio::client::Client;
use crate::aio::database::{ClientBackend, Database};
use crate::aio::primitives::{
    Event, GroupEvent, Lock, MaxConcurrentFlow, PriorityLock, ReadWriteLock, ReentrantLock,
    Semaphore, TokenBucketFlow, TreeLock,
};
use crate::error::{Result, SlockError};
use crate::options::ClientOptions;
use crate::protocol::command::Command;
use crate::protocol::constants::*;
use crate::protocol::id::Id16;
use crate::protocol::result::CommandResult;

const REPLSET_RETRY_INTERVAL: Duration = Duration::from_millis(25);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RetryType {
    Origin,
    Redirected,
    Pending,
    Woken,
}

#[derive(Clone, Debug)]
struct PendingCommand {
    request_id: Id16,
    command: Command,
    retry_type: RetryType,
    deadline: Instant,
}

impl PendingCommand {
    fn new(command: Command, grace: Duration) -> Result<Self> {
        let encoded = command.encode()?;
        Ok(Self {
            request_id: encoded.request_id,
            command,
            retry_type: RetryType::Origin,
            deadline: Instant::now() + encoded.timeout + grace,
        })
    }
}

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
    pending_commands: Arc<Mutex<VecDeque<PendingCommand>>>,
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
            pending_commands: Arc::new(Mutex::new(VecDeque::new())),
            options,
        }
    }

    pub async fn connect<N: IntoNodeList>(nodes: N) -> Result<Self> {
        let client = Self::new(nodes);
        client.open().await?;
        Ok(client)
    }

    pub fn nodes(&self) -> &[String] {
        &self.nodes
    }

    pub async fn open(&self) -> Result<()> {
        if self.nodes.is_empty() {
            return Err(SlockError::NotConnected);
        }
        let mut last_error = None;
        let mut first_live = None;
        let mut leader = None;
        for (index, client) in self.clients.iter().enumerate() {
            match client.open().await {
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

    pub async fn close(&self) {
        for client in &self.clients {
            client.close().await;
        }
        *self
            .active_index
            .lock()
            .expect("replset active index mutex poisoned") = None;
    }

    pub async fn ping(&self) -> Result<bool> {
        match self.client().ping().await {
            Ok(result) => Ok(result),
            Err(err) if self.clients.len() > 1 => {
                self.open().await?;
                self.client().ping().await.map_err(|_| err)
            }
            Err(err) => Err(err),
        }
    }

    pub fn select_database(&self, db_id: u8) -> Database {
        Database::new(ClientBackend::Replset(self.clone()), db_id)
    }

    pub fn lock<K: AsRef<[u8]>>(&self, key: K, timeout: u16, expired: u16) -> Lock {
        self.select_database(0).lock(key, timeout, expired)
    }

    pub fn event<K: AsRef<[u8]>>(
        &self,
        key: K,
        timeout: u16,
        expired: u16,
        default_set: bool,
    ) -> Event {
        self.select_database(0)
            .event(key, timeout, expired, default_set)
    }

    pub fn group_event<K: AsRef<[u8]>>(
        &self,
        key: K,
        client_id: u64,
        version_id: u64,
        timeout: u16,
        expired: u16,
    ) -> GroupEvent {
        self.select_database(0)
            .group_event(key, client_id, version_id, timeout, expired)
    }

    pub fn semaphore<K: AsRef<[u8]>>(
        &self,
        key: K,
        count: u16,
        timeout: u16,
        expired: u16,
    ) -> Semaphore {
        self.select_database(0)
            .semaphore(key, count, timeout, expired)
    }

    pub fn reentrant_lock<K: AsRef<[u8]>>(
        &self,
        key: K,
        timeout: u16,
        expired: u16,
    ) -> ReentrantLock {
        self.select_database(0)
            .reentrant_lock(key, timeout, expired)
    }

    pub fn read_write_lock<K: AsRef<[u8]>>(
        &self,
        key: K,
        timeout: u16,
        expired: u16,
    ) -> ReadWriteLock {
        self.select_database(0)
            .read_write_lock(key, timeout, expired)
    }

    pub fn priority_lock<K: AsRef<[u8]>>(
        &self,
        key: K,
        priority: u8,
        timeout: u16,
        expired: u16,
    ) -> PriorityLock {
        self.select_database(0)
            .priority_lock(key, priority, timeout, expired)
    }

    pub fn max_concurrent_flow<K: AsRef<[u8]>>(
        &self,
        key: K,
        max: u16,
        timeout: u16,
        expired: u16,
    ) -> MaxConcurrentFlow {
        self.select_database(0)
            .max_concurrent_flow(key, max, timeout, expired)
    }

    pub fn token_bucket_flow<K: AsRef<[u8]>>(
        &self,
        key: K,
        count: u16,
        timeout: u16,
        period: f64,
    ) -> TokenBucketFlow {
        self.select_database(0)
            .token_bucket_flow(key, count, timeout, period)
    }

    pub fn tree_lock<K: AsRef<[u8]>>(&self, key: K, timeout: u16, expired: u16) -> TreeLock {
        self.select_database(0).tree_lock(key, timeout, expired)
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

    pub(crate) async fn send_command(&self, command: Command) -> Result<CommandResult> {
        if self.clients.is_empty() {
            return Err(SlockError::NotConnected);
        }
        let mut pending = PendingCommand::new(command, self.options.command_timeout_grace)?;
        self.push_pending_command(pending.clone());
        let result = self.send_pending_command(&mut pending).await;
        self.remove_pending_command(pending.request_id);
        result
    }

    async fn send_pending_command(&self, pending: &mut PendingCommand) -> Result<CommandResult> {
        let mut last_error = None;
        loop {
            let start = self
                .active_index
                .lock()
                .expect("replset active index mutex poisoned")
                .unwrap_or(0);
            let mut last_state_error = None;
            for offset in 0..self.clients.len() {
                let index = (start + offset) % self.clients.len();
                let client = &self.clients[index];
                if let Err(err) = client.open().await {
                    last_error = Some(err);
                    continue;
                }
                match client.send_command(pending.command.clone()).await {
                    Ok(result) if lock_state_error(&result) && self.clients.len() > 1 => {
                        pending.retry_type = RetryType::Redirected;
                        self.update_pending_retry_type(pending.request_id, pending.retry_type);
                        last_state_error = Some(result);
                    }
                    Ok(result) => {
                        self.set_active_index(index);
                        return Ok(result);
                    }
                    Err(err) if retryable_transport_error(&err) && self.clients.len() > 1 => {
                        client.close().await;
                        pending.retry_type = RetryType::Redirected;
                        self.update_pending_retry_type(pending.request_id, pending.retry_type);
                        last_error = Some(err);
                    }
                    Err(err) => return Err(err),
                }
            }
            if let Some(result) = last_state_error {
                return Ok(result);
            }
            if Instant::now() >= pending.deadline {
                return Err(last_error.unwrap_or(SlockError::NotConnected));
            }
            pending.retry_type = RetryType::Pending;
            self.update_pending_retry_type(pending.request_id, pending.retry_type);
            tokio::time::sleep(
                pending
                    .deadline
                    .saturating_duration_since(Instant::now())
                    .min(REPLSET_RETRY_INTERVAL),
            )
            .await;
            pending.retry_type = RetryType::Woken;
            self.update_pending_retry_type(pending.request_id, pending.retry_type);
        }
    }

    fn push_pending_command(&self, pending: PendingCommand) {
        self.pending_commands
            .lock()
            .expect("replset pending mutex poisoned")
            .push_back(pending);
    }

    fn update_pending_retry_type(&self, request_id: Id16, retry_type: RetryType) {
        if let Some(pending) = self
            .pending_commands
            .lock()
            .expect("replset pending mutex poisoned")
            .iter_mut()
            .find(|pending| pending.request_id == request_id)
        {
            pending.retry_type = retry_type;
        }
    }

    fn remove_pending_command(&self, request_id: Id16) {
        let mut pending_commands = self
            .pending_commands
            .lock()
            .expect("replset pending mutex poisoned");
        pending_commands.retain(|pending| pending.request_id != request_id);
    }
}

impl ClientApi for ReplsetClient {
    fn open(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(Self::open(self))
    }

    fn close(&self) -> BoxFuture<'_, ()> {
        Box::pin(Self::close(self))
    }

    fn ping(&self) -> BoxFuture<'_, Result<bool>> {
        Box::pin(Self::ping(self))
    }

    fn select_database(&self, db_id: u8) -> Database {
        Self::select_database(self, db_id)
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

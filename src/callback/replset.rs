use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::callback::client::{
    Client, ClientObserver, CommandCallback, RequestHandle, RequestOwner,
};
use crate::callback::database::{ClientBackend, Database};
use crate::callback::handle::{ReplsetNodeClient, RequestTransport};
use crate::callback::primitives::{
    Event, GroupEvent, Lock, MaxConcurrentFlow, PriorityLock, ReadWriteLock, ReentrantLock,
    Semaphore, TokenBucketFlow, TreeLock,
};
use crate::error::{Result, SlockError};
use crate::options::ClientOptions;
use crate::protocol::command::{Command, LockCommand, PingCommand};
use crate::protocol::constants::{COMMAND_RESULT_STATE_ERROR, INIT_TYPE_FLAG_IS_LEADER};
use crate::protocol::id::Id16;
use crate::protocol::result::{CommandResult, PingCommandResult};

type SharedCommandCallback = Arc<Mutex<Option<CommandCallback>>>;

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
    pub(crate) inner: Arc<ReplsetInner>,
}

#[derive(Debug)]
struct ReplsetNode {
    address: String,
    client: Client,
}

#[derive(Debug)]
pub(crate) struct ReplsetInner {
    options: ClientOptions,
    nodes: Vec<ReplsetNode>,
    state: Mutex<ReplsetState>,
    closed: AtomicBool,
}

#[derive(Debug, Default)]
struct ReplsetState {
    lived: Vec<usize>,
    leader: Option<usize>,
    operations: HashMap<Id16, ReplsetOperation>,
}

struct ReplsetOperation {
    command: Command,
    active_request: Arc<Mutex<Option<Id16>>>,
    active_transport: Arc<Mutex<Option<RequestTransport>>>,
    callback: SharedCommandCallback,
    deadline: Instant,
    attempts: usize,
}

impl std::fmt::Debug for ReplsetOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplsetOperation")
            .field("command", &self.command)
            .field("deadline", &self.deadline)
            .field("attempts", &self.attempts)
            .finish_non_exhaustive()
    }
}

impl ReplsetClient {
    pub fn new<N: IntoNodeList>(nodes: N) -> Result<Self> {
        Self::with_options(nodes, ClientOptions::default())
    }

    pub fn with_options<N: IntoNodeList>(nodes: N, options: ClientOptions) -> Result<Self> {
        let nodes = nodes.into_nodes();
        if nodes.is_empty() {
            return Err(SlockError::NotConnected);
        }
        let replset_nodes = nodes
            .into_iter()
            .map(|address| ReplsetNode {
                address,
                client: Client::with_options(options.clone()),
            })
            .collect::<Vec<_>>();
        let inner = Arc::new(ReplsetInner {
            options,
            nodes: replset_nodes,
            state: Mutex::new(ReplsetState::default()),
            closed: AtomicBool::new(false),
        });
        let weak = Arc::downgrade(&inner);
        for (node_index, node) in inner.nodes.iter().enumerate() {
            node.client.set_observer(ClientObserver {
                node_index,
                replset: weak.clone(),
            });
        }
        Ok(Self { inner })
    }

    pub fn node_clients(&self) -> Vec<ReplsetNodeClient> {
        self.inner.node_clients()
    }

    pub fn lived_nodes(&self) -> Vec<usize> {
        self.inner
            .state
            .lock()
            .expect("callback replset state mutex poisoned")
            .lived
            .clone()
    }

    pub fn leader(&self) -> Option<usize> {
        self.inner
            .state
            .lock()
            .expect("callback replset state mutex poisoned")
            .leader
    }

    pub fn next_deadline(&self) -> Option<Instant> {
        self.inner.next_deadline()
    }

    pub fn handle_timeout(&self, now: Instant) -> Result<usize> {
        self.inner.handle_timeout(now)
    }

    pub fn close(&self) -> Result<()> {
        self.inner.closed.store(true, Ordering::SeqCst);
        for node in &self.inner.nodes {
            node.client.close()?;
        }
        self.inner
            .state
            .lock()
            .expect("callback replset state mutex poisoned")
            .operations
            .clear();
        Ok(())
    }

    pub fn select_database(&self, db_id: u8) -> Database {
        Database::new(ClientBackend::Replset(self.clone()), db_id)
    }

    pub fn ping<F>(&self, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<PingCommandResult>) + Send + 'static,
    {
        self.send_command_callback(
            Command::Ping(PingCommand::new(Id16::new())),
            move |result| {
                callback(match result {
                    Ok(CommandResult::Ping(result)) => Ok(result),
                    Ok(_) => Err(SlockError::Protocol("expected ping result".to_string())),
                    Err(err) => Err(err),
                });
            },
        )
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

    pub(crate) fn send_command_callback<F>(
        &self,
        command: Command,
        callback: F,
    ) -> Result<RequestHandle>
    where
        F: FnOnce(Result<CommandResult>) + Send + 'static,
    {
        let active_request = Arc::new(Mutex::new(None));
        let active_transport = Arc::new(Mutex::new(None));
        let operation_id = command.request_id();
        let encoded = command.encode()?;
        let callback = Arc::new(Mutex::new(Some(Box::new(callback) as CommandCallback)));
        self.inner.insert_operation(
            operation_id,
            ReplsetOperation {
                command,
                active_request: active_request.clone(),
                active_transport: active_transport.clone(),
                callback,
                deadline: Instant::now()
                    + encoded.timeout
                    + self.inner.options.command_timeout_grace,
                attempts: 0,
            },
        );
        if let Err(err) = self.inner.clone().send_attempt(operation_id, None) {
            self.inner.remove_operation(operation_id);
            return Err(err);
        }
        Ok(RequestHandle {
            request_id: operation_id,
            active_request,
            active_transport,
            owner: RequestOwner::Replset {
                replset: Arc::downgrade(&self.inner),
                operation_id,
            },
        })
    }

    pub(crate) fn send_command_on_handle<F>(
        &self,
        command: Command,
        active_request: Arc<Mutex<Option<Id16>>>,
        active_transport: Arc<Mutex<Option<RequestTransport>>>,
        callback: F,
    ) -> Result<Id16>
    where
        F: FnOnce(Result<CommandResult>) + Send + 'static,
    {
        let operation_id = command.request_id();
        let encoded = command.encode()?;
        let callback = Arc::new(Mutex::new(Some(Box::new(callback) as CommandCallback)));
        self.inner.insert_operation(
            operation_id,
            ReplsetOperation {
                command,
                active_request,
                active_transport,
                callback,
                deadline: Instant::now()
                    + encoded.timeout
                    + self.inner.options.command_timeout_grace,
                attempts: 0,
            },
        );
        self.inner.clone().send_attempt(operation_id, None)?;
        Ok(operation_id)
    }
}

impl ReplsetInner {
    fn node_clients(&self) -> Vec<ReplsetNodeClient> {
        self.nodes
            .iter()
            .enumerate()
            .map(|(index, node)| {
                ReplsetNodeClient::new(index, node.address.clone(), node.client.clone())
            })
            .collect()
    }

    pub(crate) fn on_child_init(&self, node_index: usize, init_type: u8) {
        let mut state = self
            .state
            .lock()
            .expect("callback replset state mutex poisoned");
        if !state.lived.contains(&node_index) {
            state.lived.push(node_index);
            state.lived.sort_unstable();
        }
        if (init_type & INIT_TYPE_FLAG_IS_LEADER) != 0 {
            state.leader = Some(node_index);
        }
    }

    pub(crate) fn on_child_disconnect(&self, node_index: usize) {
        let mut state = self
            .state
            .lock()
            .expect("callback replset state mutex poisoned");
        state.lived.retain(|index| *index != node_index);
        if state.leader == Some(node_index) {
            state.leader = None;
        }
    }

    pub(crate) fn cancel_operation(&self, operation_id: Id16) -> Result<bool> {
        let operation = self.remove_operation(operation_id);
        let Some(operation) = operation else {
            return Ok(false);
        };
        cancel_active_request(&operation)?;
        Ok(true)
    }

    fn next_deadline(&self) -> Option<Instant> {
        self.state
            .lock()
            .expect("callback replset state mutex poisoned")
            .operations
            .values()
            .map(|operation| operation.deadline)
            .min()
    }

    fn handle_timeout(&self, now: Instant) -> Result<usize> {
        let expired = {
            let mut state = self
                .state
                .lock()
                .expect("callback replset state mutex poisoned");
            let expired_ids = state
                .operations
                .iter()
                .filter_map(|(operation_id, operation)| {
                    (operation.deadline <= now).then_some(*operation_id)
                })
                .collect::<Vec<_>>();
            expired_ids
                .into_iter()
                .filter_map(|operation_id| state.operations.remove(&operation_id))
                .collect::<Vec<_>>()
        };
        let count = expired.len();
        for operation in expired {
            cancel_active_request(&operation)?;
            if let Some(callback) = operation
                .callback
                .lock()
                .expect("callback replset command callback mutex poisoned")
                .take()
            {
                callback(Err(SlockError::CommandTimeout));
            }
        }
        Ok(count)
    }

    fn insert_operation(&self, operation_id: Id16, operation: ReplsetOperation) {
        self.state
            .lock()
            .expect("callback replset state mutex poisoned")
            .operations
            .insert(operation_id, operation);
    }

    fn remove_operation(&self, operation_id: Id16) -> Option<ReplsetOperation> {
        self.state
            .lock()
            .expect("callback replset state mutex poisoned")
            .operations
            .remove(&operation_id)
    }

    fn select_target(&self, exclude: Option<usize>) -> Option<usize> {
        let state = self
            .state
            .lock()
            .expect("callback replset state mutex poisoned");
        if let Some(leader) = state.leader {
            if Some(leader) != exclude && state.lived.contains(&leader) {
                return Some(leader);
            }
        }
        state
            .lived
            .iter()
            .copied()
            .find(|index| Some(*index) != exclude)
    }

    fn send_attempt(self: Arc<Self>, operation_id: Id16, exclude: Option<usize>) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(SlockError::ClientClosed);
        }
        let target_index = self
            .select_target(exclude)
            .ok_or(SlockError::NotConnected)?;
        let (command, active_request, active_transport, request_id) = {
            let mut state = self
                .state
                .lock()
                .expect("callback replset state mutex poisoned");
            let operation = state
                .operations
                .get_mut(&operation_id)
                .ok_or(SlockError::ClientClosed)?;
            let command = if operation.attempts == 0 {
                operation.command.clone()
            } else {
                refresh_request_id(&operation.command)
            };
            operation.attempts += 1;
            let request_id = command.request_id();
            (
                command,
                operation.active_request.clone(),
                operation.active_transport.clone(),
                request_id,
            )
        };
        let node = &self.nodes[target_index];
        let transport =
            RequestTransport::new(target_index, node.address.clone(), node.client.clone());
        *active_transport
            .lock()
            .expect("callback replset active transport mutex poisoned") = Some(transport);
        let weak = Arc::downgrade(&self);
        let child = node.client.clone();
        child.send_command_on_handle(command, active_request, active_transport, move |result| {
            let Some(replset) = weak.upgrade() else {
                return;
            };
            replset.handle_attempt_result(operation_id, target_index, request_id, result);
        })?;
        Ok(())
    }

    fn handle_attempt_result(
        self: Arc<Self>,
        operation_id: Id16,
        node_index: usize,
        request_id: Id16,
        result: Result<CommandResult>,
    ) {
        if retryable_result(&result) {
            self.nodes[node_index].client.mark_cancelled(request_id);
            if self
                .clone()
                .send_attempt(operation_id, Some(node_index))
                .is_ok()
            {
                return;
            }
        }
        self.finish_operation(operation_id, result);
    }

    fn finish_operation(&self, operation_id: Id16, result: Result<CommandResult>) {
        let Some(operation) = self.remove_operation(operation_id) else {
            return;
        };
        let callback = operation
            .callback
            .lock()
            .expect("callback replset command callback mutex poisoned")
            .take();
        if let Some(callback) = callback {
            callback(result);
        }
    }
}

fn retryable_result(result: &Result<CommandResult>) -> bool {
    match result {
        Ok(CommandResult::Lock(result)) => result.result == COMMAND_RESULT_STATE_ERROR,
        Err(SlockError::ClientDisconnected | SlockError::NotConnected) => true,
        _ => false,
    }
}

fn refresh_request_id(command: &Command) -> Command {
    match command {
        Command::Init(command) => Command::Init(command.clone()),
        Command::Ping(_) => Command::Ping(PingCommand::new(Id16::new())),
        Command::Lock(command) => Command::Lock(LockCommand {
            request_id: Id16::new(),
            ..command.clone()
        }),
    }
}

fn cancel_active_request(operation: &ReplsetOperation) -> Result<()> {
    let request_id = *operation
        .active_request
        .lock()
        .expect("callback replset active request mutex poisoned");
    if let Some(request_id) = request_id {
        let transport = operation
            .active_transport
            .lock()
            .expect("callback replset active transport mutex poisoned")
            .clone();
        if let Some(transport) = transport {
            let _ = transport.client().cancel_request(request_id)?;
        }
    }
    *operation
        .active_request
        .lock()
        .expect("callback replset active request mutex poisoned") = None;
    *operation
        .active_transport
        .lock()
        .expect("callback replset active transport mutex poisoned") = None;
    Ok(())
}

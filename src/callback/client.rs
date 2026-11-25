use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Instant;

use crate::callback::buffer::{ReaderBuffer, SharedBuffer, WriterBuffer};
use crate::callback::database::{ClientBackend, Database};
use crate::callback::handle::{ReplsetNodeClient, RequestTransport};
use crate::callback::primitives::{
    Event, GroupEvent, Lock, MaxConcurrentFlow, PriorityLock, ReadWriteLock, ReentrantLock,
    Semaphore, TokenBucketFlow, TreeLock,
};
use crate::callback::replset::ReplsetInner;
use crate::error::{Result, SlockError};
use crate::options::ClientOptions;
use crate::protocol::codec::{decode_response, response_has_extra_data, HEADER_LEN};
use crate::protocol::command::{Command, InitCommand, PingCommand};
use crate::protocol::constants::COMMAND_RESULT_SUCCED;
use crate::protocol::id::Id16;
use crate::protocol::result::{CommandResult, PingCommandResult};

pub(crate) type CommandCallback = Box<dyn FnOnce(Result<CommandResult>) + Send + 'static>;

#[derive(Clone)]
pub struct Client {
    pub(crate) inner: Arc<ClientInner>,
}

pub(crate) struct ClientInner {
    options: ClientOptions,
    client_id: Mutex<Option<Id16>>,
    init_type: AtomicU8,
    state: Mutex<CallbackState>,
    reader_buffer: SharedBuffer,
    writer_buffer: SharedBuffer,
    requests: Mutex<CallbackRequests>,
    observer: Mutex<Option<ClientObserver>>,
    closed: AtomicBool,
}

#[derive(Clone)]
pub(crate) struct ClientObserver {
    pub(crate) node_index: usize,
    pub(crate) replset: Weak<ReplsetInner>,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client").finish_non_exhaustive()
    }
}

#[derive(Debug)]
enum CallbackState {
    New,
    InitSent(Id16),
    Inited,
    Disconnected,
    Closed,
}

const TOMBSTONE_LIMIT: usize = 1024;

#[derive(Debug, Default)]
struct Tombstones {
    order: VecDeque<Id16>,
    ids: HashSet<Id16>,
    limit: usize,
}

impl Tombstones {
    fn new(limit: usize) -> Self {
        Self {
            order: VecDeque::new(),
            ids: HashSet::new(),
            limit,
        }
    }

    fn insert(&mut self, request_id: Id16) {
        if !self.ids.insert(request_id) {
            return;
        }
        self.order.push_back(request_id);
        while self.order.len() > self.limit {
            if let Some(expired) = self.order.pop_front() {
                self.ids.remove(&expired);
            }
        }
    }

    fn remove(&mut self, request_id: &Id16) -> bool {
        let removed = self.ids.remove(request_id);
        if removed {
            self.order.retain(|candidate| candidate != request_id);
        }
        removed
    }

    fn clear(&mut self) {
        self.order.clear();
        self.ids.clear();
    }
}

/// Shared control for one user operation. It tracks the request or replset
/// operation that currently owns the cancellation slot while the operation
/// advances through staged commands.
#[derive(Debug, Default)]
pub(crate) struct RequestControl {
    state: Mutex<RequestControlState>,
}

#[derive(Debug, Default)]
struct RequestControlState {
    current: Option<Id16>,
    cancelled: bool,
    staged: bool,
    terminal: bool,
}

pub(crate) enum CancelClaim<T> {
    Claimed(T),
    Gap,
    Miss,
}

impl RequestControl {
    pub(crate) fn staged() -> Self {
        Self {
            state: Mutex::new(RequestControlState {
                staged: true,
                ..RequestControlState::default()
            }),
        }
    }

    pub(crate) fn begin<F>(&self, request_id: Id16, register: F) -> Result<()>
    where
        F: FnOnce() -> Result<()>,
    {
        let mut state = self
            .state
            .lock()
            .expect("callback request control mutex poisoned");
        if state.cancelled || state.terminal {
            return Err(SlockError::ClientClosed);
        }
        state.current = Some(request_id);
        if let Err(err) = register() {
            if state.current == Some(request_id) {
                state.current = None;
            }
            return Err(err);
        }
        Ok(())
    }

    pub(crate) fn claim<T, F>(&self, take: F) -> CancelClaim<T>
    where
        F: FnOnce(Id16) -> Option<T>,
    {
        let mut state = self
            .state
            .lock()
            .expect("callback request control mutex poisoned");
        if state.cancelled || state.terminal {
            return CancelClaim::Miss;
        }
        let Some(request_id) = state.current else {
            if state.staged {
                state.cancelled = true;
                return CancelClaim::Gap;
            }
            return CancelClaim::Miss;
        };
        match take(request_id) {
            Some(taken) => {
                state.cancelled = true;
                state.current = None;
                CancelClaim::Claimed(taken)
            }
            None if state.staged => {
                state.cancelled = true;
                state.current = None;
                CancelClaim::Gap
            }
            None => CancelClaim::Miss,
        }
    }

    /// Claims terminal completion for a staged operation. Once claimed, later
    /// cancellation attempts lose and later stages cannot start.
    pub(crate) fn claim_terminal(&self) -> bool {
        let mut state = self
            .state
            .lock()
            .expect("callback request control mutex poisoned");
        if state.cancelled || state.terminal {
            return false;
        }
        state.terminal = true;
        state.current = None;
        true
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.state
            .lock()
            .expect("callback request control mutex poisoned")
            .cancelled
    }

    pub(crate) fn is_current(&self, request_id: Id16) -> bool {
        let state = self
            .state
            .lock()
            .expect("callback request control mutex poisoned");
        !state.cancelled && !state.terminal && state.current == Some(request_id)
    }

    pub(crate) fn clear(&self, request_id: Id16) {
        let mut state = self
            .state
            .lock()
            .expect("callback request control mutex poisoned");
        if state.current == Some(request_id) {
            state.current = None;
        }
    }
}

struct PendingCallback {
    deadline: Instant,
    active_request: Arc<Mutex<Option<Id16>>>,
    control: Arc<RequestControl>,
    complete: CommandCallback,
}

struct CallbackRequests {
    pending: HashMap<Id16, PendingCallback>,
    tombstones: Tombstones,
}

enum ResponseEntry {
    Pending(PendingCallback),
    Tombstone,
    Unknown,
}

impl CallbackRequests {
    fn new(tombstone_limit: usize) -> Self {
        Self {
            pending: HashMap::new(),
            tombstones: Tombstones::new(tombstone_limit),
        }
    }

    fn insert(&mut self, request_id: Id16, pending: PendingCallback) {
        self.pending.insert(request_id, pending);
    }

    /// Removes a pending request and records its id as a tombstone in the same
    /// critical section so a concurrent reader can never observe a gap.
    fn cancel(&mut self, request_id: Id16) -> Option<PendingCallback> {
        let pending = self.pending.remove(&request_id)?;
        self.tombstones.insert(request_id);
        Some(pending)
    }

    fn take_response(&mut self, request_id: Id16) -> ResponseEntry {
        if let Some(pending) = self.pending.remove(&request_id) {
            return ResponseEntry::Pending(pending);
        }
        if self.tombstones.remove(&request_id) {
            return ResponseEntry::Tombstone;
        }
        ResponseEntry::Unknown
    }

    fn mark_cancelled(&mut self, request_id: Id16) {
        self.tombstones.insert(request_id);
    }

    fn drain(&mut self) -> Vec<PendingCallback> {
        self.pending
            .drain()
            .map(|(_, pending)| pending)
            .collect::<Vec<_>>()
    }

    fn clear_tombstones(&mut self) {
        self.tombstones.clear();
    }
}

#[derive(Clone, Debug)]
pub struct RequestHandle {
    pub(crate) request_id: Id16,
    pub(crate) active_request: Arc<Mutex<Option<Id16>>>,
    pub(crate) active_transport: Arc<Mutex<Option<RequestTransport>>>,
    pub(crate) control: Arc<RequestControl>,
    pub(crate) owner: RequestOwner,
}

#[derive(Clone, Debug)]
pub(crate) enum RequestOwner {
    Single(Weak<ClientInner>),
    Replset { replset: Weak<ReplsetInner> },
}

impl RequestHandle {
    /// Returns the first request id created for this user operation.
    pub fn request_id(&self) -> Id16 {
        self.request_id
    }

    pub fn current_request_id(&self) -> Option<Id16> {
        *self
            .active_request
            .lock()
            .expect("callback request handle mutex poisoned")
    }

    pub fn client(&self) -> Option<Client> {
        self.transport().ok().map(|transport| transport.client())
    }

    pub fn reader_buffer(&self) -> Option<ReaderBuffer> {
        self.transport()
            .ok()
            .map(|transport| transport.reader_buffer())
    }

    pub fn writer_buffer(&self) -> Option<WriterBuffer> {
        self.transport()
            .ok()
            .map(|transport| transport.writer_buffer())
    }

    pub fn transport(&self) -> Result<RequestTransport> {
        self.active_transport
            .lock()
            .expect("callback request transport mutex poisoned")
            .clone()
            .ok_or(SlockError::NotConnected)
    }

    /// Cancels the currently pending request for this operation.
    ///
    /// Bytes already drained from `WriterBuffer` cannot be retracted. If the
    /// server later replies for the cancelled request, `handle_read` ignores
    /// that response and does not invoke the user callback.
    pub fn cancel(&self) -> Result<bool> {
        match &self.owner {
            RequestOwner::Single(client) => {
                let Some(client) = client.upgrade() else {
                    return Ok(false);
                };
                let claimed = self
                    .control
                    .claim(|request_id| client.take_cancelled(request_id));
                match claimed {
                    CancelClaim::Claimed(pending) => pending.clear_active(),
                    CancelClaim::Gap => {}
                    CancelClaim::Miss => return Ok(false),
                }
                *self
                    .active_request
                    .lock()
                    .expect("callback request handle mutex poisoned") = None;
                *self
                    .active_transport
                    .lock()
                    .expect("callback request transport mutex poisoned") = None;
                Ok(true)
            }
            RequestOwner::Replset { replset } => {
                let Some(replset) = replset.upgrade() else {
                    return Ok(false);
                };
                replset.cancel_control(&self.control)
            }
        }
    }
}

impl Client {
    /// Creates a callback client with default options.
    pub fn new() -> Self {
        Self::with_options(ClientOptions::default())
    }

    /// Creates a callback client with custom options.
    pub fn with_options(options: ClientOptions) -> Self {
        Self {
            inner: Arc::new(ClientInner {
                options,
                client_id: Mutex::new(None),
                init_type: AtomicU8::new(0),
                state: Mutex::new(CallbackState::New),
                reader_buffer: SharedBuffer::new(),
                writer_buffer: SharedBuffer::new(),
                requests: Mutex::new(CallbackRequests::new(TOMBSTONE_LIMIT)),
                observer: Mutex::new(None),
                closed: AtomicBool::new(false),
            }),
        }
    }

    pub fn reader_buffer(&self) -> ReaderBuffer {
        self.inner.reader_buffer.reader()
    }

    pub fn writer_buffer(&self) -> WriterBuffer {
        self.inner.writer_buffer.writer()
    }

    /// Drives the Init handshake without performing network IO.
    pub fn handle_init(&self) -> Result<bool> {
        self.inner.handle_init()
    }

    /// Parses complete response frames and invokes matching callbacks.
    pub fn handle_read(&self) -> Result<usize> {
        self.inner.handle_read()
    }

    /// Notifies the client that the caller-owned socket disconnected.
    pub fn handle_disconnect(&self) -> Result<bool> {
        self.inner.handle_disconnect()
    }

    /// Fails expired pending callbacks. The caller owns the timer.
    pub fn handle_timeout(&self, now: Instant) -> Result<usize> {
        self.inner.handle_timeout(now)
    }

    pub fn next_deadline(&self) -> Option<Instant> {
        self.inner.next_deadline()
    }

    pub fn cancel_request(&self, request_id: Id16) -> Result<bool> {
        self.inner.cancel_request(request_id)
    }

    pub fn is_inited(&self) -> bool {
        self.inner.is_inited()
    }

    pub fn init_type(&self) -> u8 {
        self.inner.init_type.load(Ordering::SeqCst)
    }

    pub fn pending_len(&self) -> usize {
        self.inner
            .requests
            .lock()
            .expect("callback requests mutex poisoned")
            .pending
            .len()
    }

    pub fn close(&self) -> Result<()> {
        self.inner.close()
    }

    pub fn node_clients(&self) -> Vec<ReplsetNodeClient> {
        vec![ReplsetNodeClient::new(0, String::new(), self.clone())]
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

    pub fn select_database(&self, db_id: u8) -> Database {
        Database::new(ClientBackend::Single(self.clone()), db_id)
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
        let active_transport = Arc::new(Mutex::new(Some(RequestTransport::new(
            0,
            String::new(),
            self.clone(),
        ))));
        let control = Arc::new(RequestControl::default());
        let request_id = self.inner.enqueue_command(
            command,
            active_request.clone(),
            active_transport.clone(),
            control.clone(),
            Box::new(callback),
        )?;
        Ok(RequestHandle {
            request_id,
            active_request,
            active_transport,
            control,
            owner: RequestOwner::Single(Arc::downgrade(&self.inner)),
        })
    }

    pub(crate) fn send_command_on_handle<F>(
        &self,
        command: Command,
        active_request: Arc<Mutex<Option<Id16>>>,
        active_transport: Arc<Mutex<Option<RequestTransport>>>,
        control: Arc<RequestControl>,
        callback: F,
    ) -> Result<Id16>
    where
        F: FnOnce(Result<CommandResult>) + Send + 'static,
    {
        {
            let mut transport = active_transport
                .lock()
                .expect("callback request transport mutex poisoned");
            if transport.is_none() {
                *transport = Some(RequestTransport::new(0, String::new(), self.clone()));
            }
        }
        self.inner.enqueue_command(
            command,
            active_request,
            active_transport,
            control,
            Box::new(callback),
        )
    }

    pub(crate) fn set_observer(&self, observer: ClientObserver) {
        *self
            .inner
            .observer
            .lock()
            .expect("callback observer mutex poisoned") = Some(observer);
    }

    pub(crate) fn mark_cancelled(&self, request_id: Id16) {
        self.inner
            .requests
            .lock()
            .expect("callback requests mutex poisoned")
            .mark_cancelled(request_id);
    }
}

impl Default for Client {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientInner {
    fn handle_init(self: &Arc<Self>) -> Result<bool> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(SlockError::ClientClosed);
        }

        let mut state = self.state.lock().expect("callback state mutex poisoned");
        match *state {
            CallbackState::New | CallbackState::Disconnected => {
                self.reader_buffer.clear();
                let client_id = self.client_id();
                let command = Command::Init(InitCommand::with_client_id(client_id)).encode()?;
                let request_id = command.request_id;
                self.writer_buffer.push(&command.frame());
                *state = CallbackState::InitSent(request_id);
                Ok(false)
            }
            CallbackState::InitSent(request_id) => {
                let Some(bytes) = self.reader_buffer.consume(HEADER_LEN) else {
                    return Ok(false);
                };
                let mut header = [0u8; HEADER_LEN];
                header.copy_from_slice(&bytes);
                let response = decode_response(&header, None)?;
                if response.request_id() != request_id {
                    return Err(SlockError::Protocol(
                        "init response request id mismatch".to_string(),
                    ));
                }
                if response.result_code() != COMMAND_RESULT_SUCCED {
                    return Err(SlockError::Server {
                        result: response.result_code(),
                    });
                }
                let CommandResult::Init(init) = response else {
                    return Err(SlockError::Protocol("expected init result".to_string()));
                };
                self.init_type.store(init.init_type, Ordering::SeqCst);
                *state = CallbackState::Inited;
                if let Some(observer) = self
                    .observer
                    .lock()
                    .expect("callback observer mutex poisoned")
                    .clone()
                {
                    if let Some(replset) = observer.replset.upgrade() {
                        replset.on_child_init(observer.node_index, init.init_type);
                    }
                }
                Ok(true)
            }
            CallbackState::Inited => Ok(true),
            CallbackState::Closed => Err(SlockError::ClientClosed),
        }
    }

    fn handle_read(self: &Arc<Self>) -> Result<usize> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(SlockError::ClientClosed);
        }
        let mut completed = 0usize;
        loop {
            let Some(header_bytes) = self.reader_buffer.peek(HEADER_LEN) else {
                break;
            };
            let mut header = [0u8; HEADER_LEN];
            header.copy_from_slice(&header_bytes);

            let data = if response_has_extra_data(&header) {
                let Some(len_bytes) = self.reader_buffer.peek_range(HEADER_LEN, 4) else {
                    break;
                };
                let payload_len = u32::from_le_bytes(
                    len_bytes
                        .as_slice()
                        .try_into()
                        .expect("peeked extra length is 4 bytes"),
                ) as usize;
                if payload_len > self.options.max_frame_size {
                    return Err(SlockError::Protocol(format!(
                        "extra data length {payload_len} exceeds max frame size {}",
                        self.options.max_frame_size
                    )));
                }
                let frame_len = HEADER_LEN
                    .checked_add(4)
                    .and_then(|len| len.checked_add(payload_len))
                    .ok_or_else(|| {
                        SlockError::Protocol(format!(
                            "extra data frame length overflow for payload {payload_len}"
                        ))
                    })?;
                let Some(frame) = self.reader_buffer.consume(frame_len) else {
                    break;
                };
                let mut raw = vec![0u8; payload_len + 4];
                raw[4..].copy_from_slice(&frame[HEADER_LEN + 4..]);
                Some(raw)
            } else {
                self.reader_buffer
                    .consume(HEADER_LEN)
                    .expect("peek confirmed complete header");
                None
            };

            let decoded = decode_response(&header, data)?;
            let request_id = decoded.request_id();
            let entry = self
                .requests
                .lock()
                .expect("callback requests mutex poisoned")
                .take_response(request_id);
            let pending = match entry {
                ResponseEntry::Pending(pending) => pending,
                ResponseEntry::Tombstone => continue,
                ResponseEntry::Unknown => {
                    return Err(SlockError::Protocol(format!(
                        "unknown response request id {request_id:?}"
                    )));
                }
            };
            pending.control.clear(request_id);
            pending.clear_active();
            (pending.complete)(Ok(decoded));
            completed += 1;
        }
        Ok(completed)
    }

    fn handle_disconnect(self: &Arc<Self>) -> Result<bool> {
        self.reader_buffer.clear();
        self.writer_buffer.clear();
        let callbacks = {
            let mut requests = self
                .requests
                .lock()
                .expect("callback requests mutex poisoned");
            requests.clear_tombstones();
            requests.drain()
        };
        for pending in callbacks {
            pending.clear_active();
            (pending.complete)(Err(SlockError::ClientDisconnected));
        }

        let mut state = self.state.lock().expect("callback state mutex poisoned");
        if matches!(*state, CallbackState::Closed) || self.closed.load(Ordering::SeqCst) {
            return Ok(false);
        }
        *state = CallbackState::Disconnected;
        if let Some(observer) = self
            .observer
            .lock()
            .expect("callback observer mutex poisoned")
            .clone()
        {
            if let Some(replset) = observer.replset.upgrade() {
                replset.on_child_disconnect(observer.node_index);
            }
        }
        Ok(self.options.auto_reconnect)
    }

    fn handle_timeout(self: &Arc<Self>, now: Instant) -> Result<usize> {
        let callbacks = {
            let mut requests = self
                .requests
                .lock()
                .expect("callback requests mutex poisoned");
            let expired = requests
                .pending
                .iter()
                .filter_map(|(request_id, pending)| {
                    (pending.deadline <= now).then_some(*request_id)
                })
                .collect::<Vec<_>>();
            expired
                .into_iter()
                .filter_map(|request_id| {
                    requests
                        .cancel(request_id)
                        .map(|pending| (request_id, pending))
                })
                .collect::<Vec<_>>()
        };
        let count = callbacks.len();
        for (request_id, pending) in callbacks {
            pending.control.clear(request_id);
            pending.clear_active();
            (pending.complete)(Err(SlockError::CommandTimeout));
        }
        Ok(count)
    }

    fn next_deadline(&self) -> Option<Instant> {
        self.requests
            .lock()
            .expect("callback requests mutex poisoned")
            .pending
            .values()
            .map(|pending| pending.deadline)
            .min()
    }

    fn cancel_request(&self, request_id: Id16) -> Result<bool> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(SlockError::ClientClosed);
        }
        match self.take_cancelled(request_id) {
            Some(pending) => {
                pending.clear_active();
                Ok(true)
            }
            None => Ok(false),
        }
    }

    fn take_cancelled(&self, request_id: Id16) -> Option<PendingCallback> {
        self.requests
            .lock()
            .expect("callback requests mutex poisoned")
            .cancel(request_id)
    }

    fn is_inited(&self) -> bool {
        matches!(
            *self.state.lock().expect("callback state mutex poisoned"),
            CallbackState::Inited
        )
    }

    fn close(self: &Arc<Self>) -> Result<()> {
        self.closed.store(true, Ordering::SeqCst);
        {
            let mut state = self.state.lock().expect("callback state mutex poisoned");
            *state = CallbackState::Closed;
        }
        self.reader_buffer.clear();
        self.writer_buffer.clear();
        let callbacks = {
            let mut requests = self
                .requests
                .lock()
                .expect("callback requests mutex poisoned");
            requests.clear_tombstones();
            requests.drain()
        };
        for pending in callbacks {
            pending.clear_active();
            (pending.complete)(Err(SlockError::ClientClosed));
        }
        if let Some(observer) = self
            .observer
            .lock()
            .expect("callback observer mutex poisoned")
            .clone()
        {
            if let Some(replset) = observer.replset.upgrade() {
                replset.on_child_disconnect(observer.node_index);
            }
        }
        Ok(())
    }

    fn enqueue_command(
        self: &Arc<Self>,
        command: Command,
        active_request: Arc<Mutex<Option<Id16>>>,
        _active_transport: Arc<Mutex<Option<RequestTransport>>>,
        control: Arc<RequestControl>,
        callback: CommandCallback,
    ) -> Result<Id16> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(SlockError::ClientClosed);
        }
        if !self.is_inited() {
            return Err(SlockError::NotConnected);
        }
        let encoded = command.encode()?;
        let request_id = encoded.request_id;
        let deadline = Instant::now() + encoded.timeout + self.options.command_timeout_grace;
        let pending_control = control.clone();
        control.begin(request_id, || {
            self.requests
                .lock()
                .expect("callback requests mutex poisoned")
                .insert(
                    request_id,
                    PendingCallback {
                        deadline,
                        active_request: active_request.clone(),
                        control: pending_control,
                        complete: callback,
                    },
                );
            *active_request
                .lock()
                .expect("callback request handle mutex poisoned") = Some(request_id);
            self.writer_buffer.push(&encoded.frame());
            Ok(())
        })?;
        Ok(request_id)
    }

    fn client_id(&self) -> Id16 {
        let mut client_id = self
            .client_id
            .lock()
            .expect("callback client id mutex poisoned");
        *client_id.get_or_insert_with(Id16::new)
    }
}

impl PendingCallback {
    fn clear_active(&self) {
        *self
            .active_request
            .lock()
            .expect("callback request handle mutex poisoned") = None;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};
    use std::thread;

    use super::*;

    #[test]
    fn tombstones_keep_recent_ids_and_evict_in_fifo_order() {
        let mut tombstones = Tombstones::new(2);
        let first = Id16::new();
        let second = Id16::new();
        let third = Id16::new();
        tombstones.insert(first);
        tombstones.insert(second);
        tombstones.insert(third);

        assert!(!tombstones.remove(&first));
        assert!(tombstones.remove(&second));
        assert!(tombstones.remove(&third));

        tombstones.insert(first);
        tombstones.insert(first);
        assert!(tombstones.remove(&first));
        tombstones.clear();
        assert!(!tombstones.remove(&second));
    }

    #[test]
    fn request_control_claim_is_transactional_under_races() {
        for _ in 0..500 {
            let control = Arc::new(RequestControl::default());
            let initial_id = Id16::new();
            control.begin(initial_id, || Ok(())).unwrap();

            let claim_control = control.clone();
            let advance_control = control.clone();
            let barrier = Arc::new(Barrier::new(2));
            let claim_barrier = barrier.clone();
            let advance_barrier = barrier.clone();

            let claimer = thread::spawn(move || {
                claim_barrier.wait();
                match claim_control.claim(|_| Some(())) {
                    CancelClaim::Claimed(()) => Some(()),
                    _ => None,
                }
            });
            let advancer = thread::spawn(move || {
                advance_barrier.wait();
                advance_control.begin(Id16::new(), || Ok(()))
            });

            let claimed = claimer.join().expect("claimer thread panicked");
            let advanced = advancer.join().expect("advancer thread panicked");
            if claimed.is_some() {
                assert!(control.is_cancelled());
                assert!(!control.is_current(initial_id));
                assert!(control.begin(Id16::new(), || Ok(())).is_err());
            } else {
                assert!(!control.is_cancelled());
                assert!(advanced.is_ok());
                assert!(control.begin(Id16::new(), || Ok(())).is_ok());
            }
        }
    }

    #[test]
    fn request_retirement_and_response_classification_are_atomic() {
        for _ in 0..1000 {
            let request_id = Id16::new();
            let requests = Arc::new(Mutex::new(CallbackRequests::new(16)));
            requests
                .lock()
                .expect("requests mutex poisoned")
                .insert(request_id, test_pending_callback());
            let canceller = requests.clone();
            let reader = requests.clone();
            let barrier = Arc::new(Barrier::new(2));
            let cancel_barrier = barrier.clone();
            let read_barrier = barrier.clone();

            let cancelled = thread::spawn(move || {
                cancel_barrier.wait();
                canceller
                    .lock()
                    .expect("requests mutex poisoned")
                    .cancel(request_id)
                    .is_some()
            });
            let classified = thread::spawn(move || {
                read_barrier.wait();
                match reader
                    .lock()
                    .expect("requests mutex poisoned")
                    .take_response(request_id)
                {
                    ResponseEntry::Pending(_) => "pending",
                    ResponseEntry::Tombstone => "tombstone",
                    ResponseEntry::Unknown => "unknown",
                }
            });

            let cancelled = cancelled.join().expect("canceller thread panicked");
            let classified = classified.join().expect("reader thread panicked");
            match (cancelled, classified) {
                (true, "tombstone") | (false, "pending") => {}
                (cancelled, classified) => panic!(
                    "retirement/classification race produced cancelled={cancelled} classified={classified}"
                ),
            }
        }
    }

    #[test]
    fn completed_attempt_is_not_mistaken_for_cancellation() {
        let control = RequestControl::default();
        let request_id = Id16::new();
        control.begin(request_id, || Ok(())).unwrap();

        // A very fast response can finish the operation before send_attempt's
        // post-send check runs. Completion clears the current slot but must not
        // look like a cancellation, otherwise the send is reported as failed.
        control.clear(request_id);
        assert!(!control.is_cancelled());
        assert!(matches!(control.claim(|_| Some(())), CancelClaim::Miss));
        assert!(!control.is_cancelled());
    }

    #[test]
    fn request_control_missed_claim_does_not_suppress_continuation() {
        let control = RequestControl::default();
        let first = Id16::new();
        control.begin(first, || Ok(())).unwrap();

        assert!(matches!(control.claim(|_| None::<()>), CancelClaim::Miss));
        assert!(!control.is_cancelled());

        let second = Id16::new();
        assert!(control.begin(second, || Ok(())).is_ok());
        assert!(control.is_current(second));
    }

    #[test]
    fn request_control_register_failure_rolls_back_current() {
        let control = RequestControl::default();
        let request_id = Id16::new();
        assert!(control
            .begin(request_id, || Err(SlockError::ClientClosed))
            .is_err());
        assert!(matches!(control.claim(|_| Some(())), CancelClaim::Miss));
        assert!(!control.is_cancelled());
    }

    #[test]
    fn staged_cancel_in_gap_commits_and_blocks_next_stage() {
        let control = RequestControl::staged();
        let first = Id16::new();
        control.begin(first, || Ok(())).unwrap();
        control.clear(first);

        assert!(matches!(control.claim(|_| None::<()>), CancelClaim::Gap));
        assert!(control.is_cancelled());
        assert!(control.begin(Id16::new(), || Ok(())).is_err());
    }

    #[test]
    fn staged_cancel_succeeds_when_transport_removal_is_lost() {
        let control = RequestControl::staged();
        let first = Id16::new();
        control.begin(first, || Ok(())).unwrap();

        assert!(matches!(control.claim(|_| None::<()>), CancelClaim::Gap));
        assert!(control.is_cancelled());
        assert!(control.begin(Id16::new(), || Ok(())).is_err());
    }

    #[test]
    fn staged_terminal_claim_wins_over_later_cancel() {
        let control = RequestControl::staged();
        let request_id = Id16::new();
        control.begin(request_id, || Ok(())).unwrap();
        control.clear(request_id);

        assert!(control.claim_terminal());
        assert!(!control.claim_terminal());
        assert!(matches!(control.claim(|_| Some(())), CancelClaim::Miss));
        assert!(!control.is_cancelled());
        assert!(control.begin(Id16::new(), || Ok(())).is_err());
    }

    #[test]
    fn staged_cancel_wins_over_terminal_claim() {
        let control = RequestControl::staged();
        let request_id = Id16::new();
        control.begin(request_id, || Ok(())).unwrap();

        assert!(matches!(
            control.claim(|_| Some(())),
            CancelClaim::Claimed(())
        ));
        assert!(!control.claim_terminal());
        assert!(control.is_cancelled());
    }

    fn test_pending_callback() -> PendingCallback {
        PendingCallback {
            deadline: Instant::now(),
            active_request: Arc::new(Mutex::new(None)),
            control: Arc::new(RequestControl::default()),
            complete: Box::new(|_| {}),
        }
    }
}

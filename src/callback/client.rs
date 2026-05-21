use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Instant;

use crate::callback::buffer::{ReaderBuffer, SharedBuffer, WriterBuffer};
use crate::callback::database::Database;
use crate::callback::primitives::{
    Event, GroupEvent, Lock, MaxConcurrentFlow, PriorityLock, ReadWriteLock, ReentrantLock,
    Semaphore, TokenBucketFlow, TreeLock,
};
use crate::error::{Result, SlockError};
use crate::options::ClientOptions;
use crate::protocol::codec::{decode_response, response_has_extra_data, HEADER_LEN};
use crate::protocol::command::{Command, InitCommand, PingCommand};
use crate::protocol::constants::COMMAND_RESULT_SUCCED;
use crate::protocol::id::Id16;
use crate::protocol::result::{CommandResult, PingCommandResult};

type CommandCallback = Box<dyn FnOnce(Result<CommandResult>) + Send + 'static>;

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
    pending: Mutex<HashMap<Id16, PendingCallback>>,
    cancelled: Mutex<HashSet<Id16>>,
    closed: AtomicBool,
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

struct PendingCallback {
    deadline: Instant,
    active_request: Arc<Mutex<Option<Id16>>>,
    complete: CommandCallback,
}

#[derive(Clone, Debug)]
pub struct RequestHandle {
    pub(crate) request_id: Id16,
    pub(crate) active_request: Arc<Mutex<Option<Id16>>>,
    pub(crate) client: Weak<ClientInner>,
}

impl RequestHandle {
    /// Returns the first request id created for this user operation.
    pub fn request_id(&self) -> Id16 {
        self.request_id
    }

    /// Cancels the currently pending request for this operation.
    ///
    /// Bytes already drained from `WriterBuffer` cannot be retracted. If the
    /// server later replies for the cancelled request, `handle_read` ignores
    /// that response and does not invoke the user callback.
    pub fn cancel(&self) -> Result<bool> {
        let Some(client) = self.client.upgrade() else {
            return Ok(false);
        };
        let Some(request_id) = *self
            .active_request
            .lock()
            .expect("callback request handle mutex poisoned")
        else {
            return Ok(false);
        };
        let cancelled = client.cancel_request(request_id)?;
        if cancelled {
            *self
                .active_request
                .lock()
                .expect("callback request handle mutex poisoned") = None;
        }
        Ok(cancelled)
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
                pending: Mutex::new(HashMap::new()),
                cancelled: Mutex::new(HashSet::new()),
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
            .pending
            .lock()
            .expect("callback pending mutex poisoned")
            .len()
    }

    pub fn close(&self) -> Result<()> {
        self.inner.close()
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
        Database::new(self.clone(), db_id)
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
        let request_id =
            self.inner
                .enqueue_command(command, active_request.clone(), Box::new(callback))?;
        Ok(RequestHandle {
            request_id,
            active_request,
            client: Arc::downgrade(&self.inner),
        })
    }

    pub(crate) fn send_command_on_handle<F>(
        &self,
        command: Command,
        active_request: Arc<Mutex<Option<Id16>>>,
        callback: F,
    ) -> Result<Id16>
    where
        F: FnOnce(Result<CommandResult>) + Send + 'static,
    {
        self.inner
            .enqueue_command(command, active_request, Box::new(callback))
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
                let frame_len = HEADER_LEN + 4 + payload_len;
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
            let pending = self
                .pending
                .lock()
                .expect("callback pending mutex poisoned")
                .remove(&request_id);
            let Some(pending) = pending else {
                if self
                    .cancelled
                    .lock()
                    .expect("callback cancelled mutex poisoned")
                    .remove(&request_id)
                {
                    continue;
                }
                return Err(SlockError::Protocol(format!(
                    "unknown response request id {request_id:?}"
                )));
            };
            pending.clear_active();
            (pending.complete)(Ok(decoded));
            completed += 1;
        }
        Ok(completed)
    }

    fn handle_disconnect(self: &Arc<Self>) -> Result<bool> {
        self.reader_buffer.clear();
        self.writer_buffer.clear();
        self.cancelled
            .lock()
            .expect("callback cancelled mutex poisoned")
            .clear();

        let callbacks = {
            let mut pending = self
                .pending
                .lock()
                .expect("callback pending mutex poisoned");
            pending
                .drain()
                .map(|(_, pending)| pending)
                .collect::<Vec<_>>()
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
        Ok(self.options.auto_reconnect)
    }

    fn handle_timeout(self: &Arc<Self>, now: Instant) -> Result<usize> {
        let callbacks = {
            let mut pending = self
                .pending
                .lock()
                .expect("callback pending mutex poisoned");
            let expired = pending
                .iter()
                .filter_map(|(request_id, pending)| {
                    (pending.deadline <= now).then_some(*request_id)
                })
                .collect::<Vec<_>>();
            expired
                .into_iter()
                .filter_map(|request_id| pending.remove(&request_id))
                .collect::<Vec<_>>()
        };
        let count = callbacks.len();
        for pending in callbacks {
            pending.clear_active();
            (pending.complete)(Err(SlockError::CommandTimeout));
        }
        Ok(count)
    }

    fn next_deadline(&self) -> Option<Instant> {
        self.pending
            .lock()
            .expect("callback pending mutex poisoned")
            .values()
            .map(|pending| pending.deadline)
            .min()
    }

    fn cancel_request(&self, request_id: Id16) -> Result<bool> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(SlockError::ClientClosed);
        }
        let removed = self
            .pending
            .lock()
            .expect("callback pending mutex poisoned")
            .remove(&request_id);
        if let Some(pending) = removed {
            pending.clear_active();
            self.cancelled
                .lock()
                .expect("callback cancelled mutex poisoned")
                .insert(request_id);
            Ok(true)
        } else {
            Ok(false)
        }
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
        self.cancelled
            .lock()
            .expect("callback cancelled mutex poisoned")
            .clear();
        let callbacks = {
            let mut pending = self
                .pending
                .lock()
                .expect("callback pending mutex poisoned");
            pending
                .drain()
                .map(|(_, pending)| pending)
                .collect::<Vec<_>>()
        };
        for pending in callbacks {
            pending.clear_active();
            (pending.complete)(Err(SlockError::ClientClosed));
        }
        Ok(())
    }

    fn enqueue_command(
        self: &Arc<Self>,
        command: Command,
        active_request: Arc<Mutex<Option<Id16>>>,
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
        self.pending
            .lock()
            .expect("callback pending mutex poisoned")
            .insert(
                request_id,
                PendingCallback {
                    deadline,
                    active_request: active_request.clone(),
                    complete: callback,
                },
            );
        *active_request
            .lock()
            .expect("callback request handle mutex poisoned") = Some(request_id);
        self.writer_buffer.push(&encoded.frame());
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

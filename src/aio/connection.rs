use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{oneshot, Mutex as AsyncMutex};

use crate::aio::raw::{RawCommand, RawSendFailure};
use crate::error::{Result, SlockError};
use crate::options::ClientOptions;
use crate::protocol::codec::{decode_response, response_has_extra_data, HEADER_LEN};
use crate::protocol::command::{Command, InitCommand};
use crate::protocol::constants::COMMAND_RESULT_SUCCED;
use crate::protocol::id::Id16;
use crate::protocol::result::CommandResult;

#[derive(Debug)]
enum PendingResponse {
    Typed(Result<CommandResult>),
    Raw(Result<Vec<u8>>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingKind {
    Typed,
    Raw,
}

#[derive(Debug)]
struct PendingEntry {
    kind: PendingKind,
    sender: oneshot::Sender<PendingResponse>,
    token: u64,
}

type PendingMap = Arc<AsyncMutex<HashMap<Id16, PendingEntry>>>;

struct PendingCleanup {
    pending: PendingMap,
    request_id: Id16,
    token: u64,
    active: bool,
}

impl PendingCleanup {
    fn new(pending: PendingMap, request_id: Id16, token: u64) -> Self {
        Self {
            pending,
            request_id,
            token,
            active: true,
        }
    }

    fn disarm(&mut self) {
        self.active = false;
    }

    /// Removes this guard's entry without dropping a newer submission that
    /// reused the same request id.
    async fn remove_now(&mut self) {
        if !self.active {
            return;
        }
        {
            let mut pending = self.pending.lock().await;
            if pending
                .get(&self.request_id)
                .is_some_and(|entry| entry.token == self.token)
            {
                pending.remove(&self.request_id);
            }
        }
        self.active = false;
    }
}

impl Drop for PendingCleanup {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        // Remove only the entry this guard registered. Request ids can be
        // reused after a cancelled send, so a newer submission with the same
        // id must survive.
        if let Ok(mut pending) = self.pending.try_lock() {
            if pending
                .get(&self.request_id)
                .is_some_and(|entry| entry.token == self.token)
            {
                pending.remove(&self.request_id);
            }
            return;
        }

        // Drop cannot await the async map. If another task holds it, schedule
        // the same token-checked removal so a cancelled send cannot leak a
        // pending slot.
        let pending = Arc::clone(&self.pending);
        let request_id = self.request_id;
        let token = self.token;
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let mut pending = pending.lock().await;
                if pending
                    .get(&request_id)
                    .is_some_and(|entry| entry.token == token)
                {
                    pending.remove(&request_id);
                }
            });
        }
    }
}

#[derive(Debug)]
pub(crate) struct Connection {
    address: String,
    options: ClientOptions,
    client_id: Mutex<Option<Id16>>,
    init_type: Arc<Mutex<u8>>,
    writer: Arc<AsyncMutex<Option<OwnedWriteHalf>>>,
    pending: PendingMap,
    pending_token: AtomicU64,
    closed: Arc<AtomicBool>,
    open_gate: AsyncMutex<()>,
    reader_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    cleanup_task: AsyncMutex<Option<tokio::task::JoinHandle<()>>>,
    initial_task: AsyncMutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Connection {
    pub(crate) fn new(address: String, options: ClientOptions) -> Self {
        Self {
            address,
            options,
            client_id: Mutex::new(None),
            init_type: Arc::new(Mutex::new(0)),
            writer: Arc::new(AsyncMutex::new(None)),
            pending: Arc::new(AsyncMutex::new(HashMap::new())),
            pending_token: AtomicU64::new(0),
            closed: Arc::new(AtomicBool::new(false)),
            open_gate: AsyncMutex::new(()),
            reader_task: Mutex::new(None),
            cleanup_task: AsyncMutex::new(None),
            initial_task: AsyncMutex::new(None),
        }
    }

    pub(crate) async fn open(&self) -> Result<()> {
        // Serialize lifecycle transitions so concurrent opens wait for the first
        // handshake instead of reporting success before the connection exists.
        let _gate = self.open_gate.lock().await;
        self.await_cleanup().await;
        // A strict open must never race a managed `try_open` retry: stop the
        // background reconnect and let this call own the handshake.
        self.stop_initial_reconnect().await;

        let stale_task = {
            let mut reader_task = self.reader_task.lock().expect("reader task mutex poisoned");
            match reader_task.as_ref() {
                Some(handle) if !handle.is_finished() => {
                    if !self.closed.load(Ordering::SeqCst) {
                        return Ok(());
                    }
                    // A cancelled lifecycle left a supervisor behind with the
                    // closed flag set; drop it and reconnect below.
                    reader_task.take()
                }
                Some(_) => reader_task.take(),
                None => None,
            }
        };
        if let Some(handle) = stale_task {
            handle.abort();
            let _ = handle.await;
        }

        let client_id = self.identity();
        let (reader, writer, init_type) =
            connect_once(&self.address, &self.options, client_id).await?;

        self.publish(reader, writer, init_type, client_id).await;
        Ok(())
    }

    /// Opens without turning an initial failure into an error.
    ///
    /// Returns `Ok(true)` when this call completed the Init handshake, and
    /// `Ok(false)` when the first attempt failed but a managed background
    /// reconnect was started with the same client identity. `close` cancels
    /// that reconnect. With `ClientOptions::auto_reconnect == false` the first
    /// error is returned, matching [`Connection::open`].
    pub(crate) async fn try_open(self: &Arc<Self>) -> Result<bool> {
        let _gate = self.open_gate.lock().await;
        self.await_cleanup().await;

        if self.has_live_reader() {
            return Ok(true);
        }

        // A managed retry is already in flight; do not start a second one.
        {
            let mut slot = self.initial_task.lock().await;
            match slot.as_ref() {
                Some(handle) if !handle.is_finished() => return Ok(false),
                Some(_) => {
                    if let Some(handle) = slot.take() {
                        let _ = handle.await;
                    }
                }
                None => {}
            }
        }

        // Drop a finished reader handle left by a previous lifecycle.
        let stale_reader = self
            .reader_task
            .lock()
            .expect("reader task mutex poisoned")
            .take();
        if let Some(handle) = stale_reader {
            handle.abort();
            let _ = handle.await;
        }

        let client_id = self.identity();
        self.closed.store(false, Ordering::SeqCst);
        match connect_once(&self.address, &self.options, client_id).await {
            Ok((reader, writer, init_type)) => {
                self.publish(reader, writer, init_type, client_id).await;
                Ok(true)
            }
            Err(err) => {
                if !self.options.auto_reconnect {
                    return Err(err);
                }
                self.spawn_initial_reconnect(client_id).await;
                Ok(false)
            }
        }
    }

    pub(crate) async fn close(&self) {
        let _gate = self.open_gate.lock().await;

        // Wait for a cleanup from an earlier cancelled close before starting a
        // new transition. Holding this guard across the await keeps the handle
        // stored if this close is itself cancelled.
        let mut cleanup_slot = self.cleanup_task.lock().await;
        if let Some(handle) = cleanup_slot.as_mut() {
            let _ = handle.await;
        }
        *cleanup_slot = None;

        self.closed.store(true, Ordering::SeqCst);

        // Stop the managed initial reconnect before taking the supervisor, so a
        // handshake that raced close cannot publish a socket afterwards.
        let initial = self.initial_task.lock().await.take();
        if let Some(handle) = initial.as_ref() {
            handle.abort();
        }
        if let Some(handle) = initial {
            let _ = handle.await;
        }
        // A publish that won the race above may have cleared the flag.
        self.closed.store(true, Ordering::SeqCst);

        // Take and abort the supervisor synchronously so it cannot publish a
        // new writer once cleanup starts. The retained cleanup task awaits it
        // and tears down the writer, so cancelling close stays safe.
        let supervisor = self
            .reader_task
            .lock()
            .expect("reader task mutex poisoned")
            .take();
        if let Some(handle) = supervisor.as_ref() {
            handle.abort();
        }

        let writer = Arc::clone(&self.writer);
        let pending = Arc::clone(&self.pending);
        let cleanup = tokio::spawn(async move {
            if let Some(handle) = supervisor {
                let _ = handle.await;
            }
            if let Some(mut writer) = writer.lock().await.take() {
                let _ = writer.shutdown().await;
            }
            let mut pending = pending.lock().await;
            for (_, entry) in pending.drain() {
                let response = match entry.kind {
                    PendingKind::Typed => PendingResponse::Typed(Err(SlockError::ClientClosed)),
                    PendingKind::Raw => PendingResponse::Raw(Err(SlockError::ClientClosed)),
                };
                let _ = entry.sender.send(response);
            }
        });
        *cleanup_slot = Some(cleanup);
        if let Some(handle) = cleanup_slot.as_mut() {
            let _ = handle.await;
        }
        *cleanup_slot = None;
    }

    fn identity(&self) -> Id16 {
        let mut guard = self.client_id.lock().expect("client id mutex poisoned");
        *guard.get_or_insert_with(Id16::new)
    }

    fn has_live_reader(&self) -> bool {
        !self.closed.load(Ordering::SeqCst)
            && self
                .reader_task
                .lock()
                .expect("reader task mutex poisoned")
                .as_ref()
                .is_some_and(|handle| !handle.is_finished())
    }

    async fn stop_initial_reconnect(&self) {
        let handle = self.initial_task.lock().await.take();
        if let Some(handle) = handle {
            handle.abort();
            let _ = handle.await;
        }
    }

    async fn spawn_initial_reconnect(self: &Arc<Self>, client_id: Id16) {
        // Hold only a weak reference so a dropped client can still release the
        // connection instead of keeping a reconnect task alive forever.
        let connection = Arc::downgrade(self);
        let handle = tokio::spawn(async move {
            loop {
                let Some(connection) = connection.upgrade() else {
                    break;
                };
                if connection.closed.load(Ordering::SeqCst) {
                    break;
                }
                match connect_once(&connection.address, &connection.options, client_id).await {
                    Ok((reader, writer, init_type)) => {
                        // If close raced this handshake, close aborts and
                        // awaits this task before its retained cleanup tears
                        // down the writer, so no socket outlives close.
                        connection
                            .publish(reader, writer, init_type, client_id)
                            .await;
                        break;
                    }
                    Err(_) => {
                        if connection.closed.load(Ordering::SeqCst) {
                            break;
                        }
                        tokio::time::sleep(connection.options.reconnect_interval).await;
                    }
                }
            }
        });
        *self.initial_task.lock().await = Some(handle);
    }

    async fn publish(
        &self,
        reader: OwnedReadHalf,
        writer: OwnedWriteHalf,
        init_type: u8,
        client_id: Id16,
    ) {
        // Strict `open`, `try_open`, and `close` share the lifecycle gate; the
        // managed reconnect is instead stopped and awaited by both lifecycle
        // operations before they touch the writer slot.
        let mut writer_slot = self.writer.lock().await;
        *self.init_type.lock().expect("init type mutex poisoned") = init_type;
        // Any writer surviving a previous lifecycle transition is stale.
        drop(writer_slot.take());
        *writer_slot = Some(writer);
        self.closed.store(false, Ordering::SeqCst);
        drop(writer_slot);
        self.spawn_reader(reader, client_id);
    }

    async fn await_cleanup(&self) {
        let mut cleanup_slot = self.cleanup_task.lock().await;
        if let Some(handle) = cleanup_slot.as_mut() {
            let _ = handle.await;
        }
        *cleanup_slot = None;
    }

    pub(crate) async fn pending_len(&self) -> usize {
        self.pending.lock().await.len()
    }

    pub(crate) fn init_type(&self) -> u8 {
        *self.init_type.lock().expect("init type mutex poisoned")
    }

    pub(crate) async fn send_command(&self, command: Command) -> Result<CommandResult> {
        self.send_command_classified(command)
            .await
            .map_err(RawSendFailure::into_error)
    }

    /// Replset-facing typed send that distinguishes "definitely not sent" from
    /// "write outcome unknown". Only `NotSent` may be routed to another member.
    pub(crate) async fn send_command_classified(
        &self,
        command: Command,
    ) -> std::result::Result<CommandResult, RawSendFailure> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(RawSendFailure::NotSent(SlockError::ClientClosed));
        }
        let encoded = command.encode().map_err(RawSendFailure::NotSent)?;
        let (rx, mut cleanup) = self
            .register_pending(encoded.request_id, PendingKind::Typed)
            .await
            .map_err(RawSendFailure::NotSent)?;

        if let Err(err) = write_frame(&self.writer, &encoded.header, encoded.extra.as_deref()).await
        {
            cleanup.remove_now().await;
            return match err {
                SlockError::NotConnected => Err(RawSendFailure::NotSent(err)),
                other => {
                    close_writer(&self.writer).await;
                    Err(RawSendFailure::Unknown(other))
                }
            };
        }

        let timeout = encoded
            .timeout
            .saturating_add(self.options.command_timeout_grace);
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(PendingResponse::Typed(result))) => {
                cleanup.disarm();
                result.map_err(RawSendFailure::Unknown)
            }
            Ok(Ok(PendingResponse::Raw(_))) => {
                cleanup.disarm();
                Err(RawSendFailure::Unknown(SlockError::Protocol(
                    "raw response matched a typed request".to_string(),
                )))
            }
            Ok(Err(_)) => {
                cleanup.disarm();
                Err(RawSendFailure::Unknown(SlockError::ClientClosed))
            }
            Err(_) => {
                cleanup.remove_now().await;
                Err(RawSendFailure::Unknown(SlockError::CommandTimeout))
            }
        }
    }

    /// Sends an already-validated raw command and returns the exact response
    /// frame. Lock extra data is preserved with its wire length prefix.
    ///
    /// This variant keeps "definitely not sent" separate from
    /// "write outcome unknown" so a command is never blindly replayed.
    pub(crate) async fn send_raw_classified(
        &self,
        command: RawCommand,
    ) -> std::result::Result<Vec<u8>, RawSendFailure> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(RawSendFailure::NotSent(SlockError::ClientClosed));
        }
        if !command.expects_response() {
            return Err(RawSendFailure::NotSent(SlockError::Protocol(
                "send_raw requires expects_response; use write_raw for a write-only command"
                    .to_string(),
            )));
        }
        let timeout = command.response_timeout(self.options.command_timeout_grace);
        let encoded = command.into_encoded();
        let (rx, mut cleanup) = match self
            .register_pending(encoded.request_id, PendingKind::Raw)
            .await
        {
            Ok(pair) => pair,
            Err(err) => return Err(RawSendFailure::NotSent(err)),
        };

        if let Err(err) = write_frame(&self.writer, &encoded.header, encoded.extra.as_deref()).await
        {
            cleanup.remove_now().await;
            return match err {
                SlockError::NotConnected => Err(RawSendFailure::NotSent(err)),
                other => {
                    close_writer(&self.writer).await;
                    Err(RawSendFailure::Unknown(other))
                }
            };
        }

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(PendingResponse::Raw(Ok(frame)))) => {
                cleanup.disarm();
                Ok(frame)
            }
            Ok(Ok(PendingResponse::Raw(Err(err)))) => {
                cleanup.disarm();
                Err(RawSendFailure::Unknown(err))
            }
            Ok(Ok(PendingResponse::Typed(_))) => {
                cleanup.disarm();
                Err(RawSendFailure::Unknown(SlockError::Protocol(
                    "typed response matched a raw request".to_string(),
                )))
            }
            Ok(Err(_)) => {
                cleanup.disarm();
                Err(RawSendFailure::Unknown(SlockError::ClientClosed))
            }
            Err(_) => {
                cleanup.remove_now().await;
                Err(RawSendFailure::Unknown(SlockError::CommandTimeout))
            }
        }
    }

    /// Writes an already-validated raw command without registering a pending
    /// request or waiting for any response.
    ///
    /// A transport failure after the write starts is `Unknown`; a missing
    /// writer or closed client is `NotSent`.
    pub(crate) async fn write_raw_classified(
        &self,
        command: RawCommand,
    ) -> std::result::Result<(), RawSendFailure> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(RawSendFailure::NotSent(SlockError::ClientClosed));
        }
        let encoded = command.into_encoded();
        match write_frame(&self.writer, &encoded.header, encoded.extra.as_deref()).await {
            Ok(()) => Ok(()),
            Err(SlockError::NotConnected) => Err(RawSendFailure::NotSent(SlockError::NotConnected)),
            Err(err) => {
                close_writer(&self.writer).await;
                Err(RawSendFailure::Unknown(err))
            }
        }
    }

    async fn register_pending(
        &self,
        request_id: Id16,
        kind: PendingKind,
    ) -> Result<(oneshot::Receiver<PendingResponse>, PendingCleanup)> {
        let (sender, receiver) = oneshot::channel();
        let token = self.pending_token.fetch_add(1, Ordering::Relaxed);
        {
            let mut pending = self.pending.lock().await;
            if pending.contains_key(&request_id) {
                return Err(SlockError::Protocol(format!(
                    "duplicate pending request id {request_id:?}"
                )));
            }
            pending.insert(
                request_id,
                PendingEntry {
                    kind,
                    sender,
                    token,
                },
            );
        }
        Ok((
            receiver,
            PendingCleanup::new(Arc::clone(&self.pending), request_id, token),
        ))
    }

    fn spawn_reader(&self, reader: OwnedReadHalf, client_id: Id16) {
        let address = self.address.clone();
        let options = self.options.clone();
        let init_type = Arc::clone(&self.init_type);
        let pending = Arc::clone(&self.pending);
        let writer = Arc::clone(&self.writer);
        let closed = Arc::clone(&self.closed);
        let handle = tokio::spawn(async move {
            let mut reader = Some(reader);
            while !closed.load(Ordering::SeqCst) {
                if let Some(mut current_reader) = reader.take() {
                    if let Err(err) = read_loop(
                        &mut current_reader,
                        &pending,
                        &closed,
                        options.max_frame_size,
                    )
                    .await
                    {
                        close_writer(&writer).await;
                        if !closed.load(Ordering::SeqCst) {
                            wake_all_io(&pending, err).await;
                        }
                    }
                }

                if !options.auto_reconnect || closed.load(Ordering::SeqCst) {
                    break;
                }

                loop {
                    if closed.load(Ordering::SeqCst) {
                        break;
                    }
                    match connect_once(&address, &options, client_id).await {
                        Ok((new_reader, new_writer, new_init_type)) => {
                            *init_type.lock().expect("init type mutex poisoned") = new_init_type;
                            *writer.lock().await = Some(new_writer);
                            reader = Some(new_reader);
                            break;
                        }
                        Err(_) => tokio::time::sleep(options.reconnect_interval).await,
                    }
                }
            }
            close_writer(&writer).await;
        });
        *self.reader_task.lock().expect("reader task mutex poisoned") = Some(handle);
    }
}

async fn connect_once(
    address: &str,
    options: &ClientOptions,
    client_id: Id16,
) -> Result<(OwnedReadHalf, OwnedWriteHalf, u8)> {
    let deadline = tokio::time::Instant::now() + options.connect_timeout;
    match tokio::time::timeout_at(deadline, connect_once_inner(address, options, client_id)).await {
        Ok(result) => result,
        Err(_) => Err(SlockError::CommandTimeout),
    }
}

async fn connect_once_inner(
    address: &str,
    options: &ClientOptions,
    client_id: Id16,
) -> Result<(OwnedReadHalf, OwnedWriteHalf, u8)> {
    let mut stream = TcpStream::connect(address).await?;
    stream.set_nodelay(options.tcp_nodelay)?;

    let init = Command::Init(InitCommand::with_client_id(client_id)).encode()?;
    stream.write_all(&init.header).await?;
    if let Some(extra) = init.extra {
        stream.write_all(&extra).await?;
    }
    stream.flush().await?;

    let mut response_header = [0u8; HEADER_LEN];
    stream.read_exact(&mut response_header).await?;
    let response = decode_response(&response_header, None)?;
    if response.result_code() != COMMAND_RESULT_SUCCED {
        return Err(SlockError::Server {
            result: response.result_code(),
        });
    }
    let CommandResult::Init(result) = response else {
        return Err(SlockError::Protocol("expected init result".to_string()));
    };

    let (reader, writer) = stream.into_split();
    Ok((reader, writer, result.init_type))
}

async fn read_loop(
    reader: &mut OwnedReadHalf,
    pending: &PendingMap,
    closed: &Arc<AtomicBool>,
    max_frame_size: usize,
) -> Result<()> {
    while !closed.load(Ordering::SeqCst) {
        let mut header = [0u8; HEADER_LEN];
        reader.read_exact(&mut header).await?;

        let data = if response_has_extra_data(&header) {
            Some(read_extra_data(reader, max_frame_size).await?)
        } else {
            None
        };

        let mut request_id_bytes = [0u8; 16];
        request_id_bytes.copy_from_slice(&header[3..19]);
        let request_id = Id16::from_bytes(request_id_bytes);

        // The pending entry kind decides whether the response is decoded into
        // the typed result model or preserved verbatim for the binding.
        let kind = {
            let pending = pending.lock().await;
            pending.get(&request_id).map(|entry| entry.kind)
        };
        let Some(kind) = kind else {
            continue;
        };
        let response = match kind {
            PendingKind::Typed => PendingResponse::Typed(decode_response(&header, data)),
            PendingKind::Raw => PendingResponse::Raw(Ok(raw_response_frame(&header, data))),
        };

        if let Some(entry) = pending.lock().await.remove(&request_id) {
            let _ = entry.sender.send(response);
        }
    }
    Ok(())
}

async fn write_frame(
    writer: &Arc<AsyncMutex<Option<OwnedWriteHalf>>>,
    header: &[u8; HEADER_LEN],
    extra: Option<&[u8]>,
) -> Result<()> {
    let mut writer = writer.lock().await;
    let Some(writer) = writer.as_mut() else {
        return Err(SlockError::NotConnected);
    };
    writer.write_all(header).await?;
    if let Some(extra) = extra {
        writer.write_all(extra).await?;
    }
    writer.flush().await?;
    Ok(())
}

fn raw_response_frame(header: &[u8; HEADER_LEN], data: Option<Vec<u8>>) -> Vec<u8> {
    let mut frame = Vec::with_capacity(HEADER_LEN + data.as_ref().map_or(0, Vec::len));
    frame.extend_from_slice(header);
    if let Some(data) = data {
        frame.extend_from_slice(&data);
    }
    frame
}

async fn close_writer(writer: &Arc<AsyncMutex<Option<OwnedWriteHalf>>>) {
    if let Some(mut writer) = writer.lock().await.take() {
        let _ = writer.shutdown().await;
    }
}

async fn wake_all_io(pending: &PendingMap, err: SlockError) {
    let mut pending = pending.lock().await;
    for (_, entry) in pending.drain() {
        let error = clone_error_like(&err);
        let response = match entry.kind {
            PendingKind::Typed => PendingResponse::Typed(Err(error)),
            PendingKind::Raw => PendingResponse::Raw(Err(error)),
        };
        let _ = entry.sender.send(response);
    }
}

fn clone_error_like(err: &SlockError) -> SlockError {
    match err {
        SlockError::Io(err) => SlockError::Io(std::io::Error::new(err.kind(), err.to_string())),
        SlockError::Protocol(message) => SlockError::Protocol(message.clone()),
        other => SlockError::Protocol(other.to_string()),
    }
}

async fn read_extra_data(reader: &mut OwnedReadHalf, max_frame_size: usize) -> Result<Vec<u8>> {
    let mut len_bytes = [0u8; 4];
    reader.read_exact(&mut len_bytes).await?;
    let len = u32::from_le_bytes(len_bytes) as usize;
    if len > max_frame_size {
        return Err(SlockError::Protocol(format!(
            "extra data length {len} exceeds max frame size {max_frame_size}"
        )));
    }
    let total = len.checked_add(4).ok_or_else(|| {
        SlockError::Protocol(format!("extra data length {len} overflows frame size"))
    })?;
    let mut data = vec![0u8; total];
    reader.read_exact(&mut data[4..]).await?;
    Ok(data)
}

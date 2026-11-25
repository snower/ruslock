use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{oneshot, Mutex as AsyncMutex};

use crate::error::{Result, SlockError};
use crate::options::ClientOptions;
use crate::protocol::codec::{decode_response, response_has_extra_data, HEADER_LEN};
use crate::protocol::command::{Command, InitCommand};
use crate::protocol::constants::COMMAND_RESULT_SUCCED;
use crate::protocol::id::Id16;
use crate::protocol::result::CommandResult;

type PendingSender = oneshot::Sender<Result<CommandResult>>;

struct PendingCleanup {
    pending: Arc<AsyncMutex<HashMap<Id16, PendingSender>>>,
    request_id: Id16,
    active: bool,
}

impl PendingCleanup {
    fn new(pending: Arc<AsyncMutex<HashMap<Id16, PendingSender>>>, request_id: Id16) -> Self {
        Self {
            pending,
            request_id,
            active: true,
        }
    }

    fn disarm(&mut self) {
        self.active = false;
    }
}

impl Drop for PendingCleanup {
    fn drop(&mut self) {
        if self.active {
            if let Ok(mut pending) = self.pending.try_lock() {
                pending.remove(&self.request_id);
            }
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
    pending: Arc<AsyncMutex<HashMap<Id16, PendingSender>>>,
    closed: Arc<AtomicBool>,
    open_gate: AsyncMutex<()>,
    reader_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    cleanup_task: AsyncMutex<Option<tokio::task::JoinHandle<()>>>,
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
            closed: Arc::new(AtomicBool::new(false)),
            open_gate: AsyncMutex::new(()),
            reader_task: Mutex::new(None),
            cleanup_task: AsyncMutex::new(None),
        }
    }

    pub(crate) async fn open(&self) -> Result<()> {
        // Serialize lifecycle transitions so concurrent opens wait for the first
        // handshake instead of reporting success before the connection exists.
        let _gate = self.open_gate.lock().await;
        self.await_cleanup().await;

        let stale_task = {
            let mut reader_task = self.reader_task.lock().expect("reader task mutex poisoned");
            match reader_task.as_ref() {
                Some(handle) if !handle.is_finished() => return Ok(()),
                Some(_) => reader_task.take(),
                None => None,
            }
        };
        if let Some(handle) = stale_task {
            handle.abort();
            let _ = handle.await;
        }

        let client_id = {
            let mut guard = self.client_id.lock().expect("client id mutex poisoned");
            *guard.get_or_insert_with(Id16::new)
        };
        let (reader, writer, init_type) =
            connect_once(&self.address, &self.options, client_id).await?;

        // From here to the end of the publication there is no await point, so a
        // cancelled open cannot leave a half-published connection.
        let mut writer_slot = self.writer.lock().await;
        *self.init_type.lock().expect("init type mutex poisoned") = init_type;
        // Any writer surviving a previous lifecycle transition is stale.
        drop(writer_slot.take());
        *writer_slot = Some(writer);
        self.closed.store(false, Ordering::SeqCst);
        self.spawn_reader(reader, client_id);
        Ok(())
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

        // Take and abort the supervisor synchronously so it cannot publish a
        // new writer once cleanup starts.
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
            for (_, tx) in pending.drain() {
                let _ = tx.send(Err(SlockError::ClientClosed));
            }
        });
        *cleanup_slot = Some(cleanup);
        if let Some(handle) = cleanup_slot.as_mut() {
            let _ = handle.await;
        }
        *cleanup_slot = None;
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
        if self.closed.load(Ordering::SeqCst) {
            return Err(SlockError::ClientClosed);
        }
        let encoded = command.encode()?;
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(encoded.request_id, tx);
        let mut cleanup = PendingCleanup::new(Arc::clone(&self.pending), encoded.request_id);

        let write_result = {
            let mut writer = self.writer.lock().await;
            let Some(writer) = writer.as_mut() else {
                self.pending.lock().await.remove(&encoded.request_id);
                cleanup.disarm();
                return Err(SlockError::NotConnected);
            };
            async {
                writer.write_all(&encoded.header).await?;
                if let Some(extra) = &encoded.extra {
                    writer.write_all(extra).await?;
                }
                writer.flush().await
            }
            .await
        };

        if let Err(err) = write_result {
            self.pending.lock().await.remove(&encoded.request_id);
            close_writer(&self.writer).await;
            cleanup.disarm();
            return Err(SlockError::Io(err));
        }

        let timeout = encoded.timeout + self.options.command_timeout_grace;
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(result)) => {
                cleanup.disarm();
                result
            }
            Ok(Err(_)) => {
                cleanup.disarm();
                Err(SlockError::ClientClosed)
            }
            Err(_) => {
                self.pending.lock().await.remove(&encoded.request_id);
                cleanup.disarm();
                Err(SlockError::CommandTimeout)
            }
        }
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
    pending: &Arc<AsyncMutex<HashMap<Id16, PendingSender>>>,
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

        let decoded = decode_response(&header, data);
        let request_id = decoded
            .as_ref()
            .ok()
            .map(CommandResult::request_id)
            .or_else(|| {
                let mut bytes = [0u8; 16];
                bytes.copy_from_slice(&header[3..19]);
                Some(Id16::from_bytes(bytes))
            });
        if let Some(request_id) = request_id {
            if let Some(tx) = pending.lock().await.remove(&request_id) {
                let _ = tx.send(decoded);
            }
        }
    }
    Ok(())
}

async fn close_writer(writer: &Arc<AsyncMutex<Option<OwnedWriteHalf>>>) {
    if let Some(mut writer) = writer.lock().await.take() {
        let _ = writer.shutdown().await;
    }
}

async fn wake_all_io(pending: &Arc<AsyncMutex<HashMap<Id16, PendingSender>>>, err: SlockError) {
    let mut pending = pending.lock().await;
    match err {
        SlockError::Io(err) => {
            for (_, tx) in pending.drain() {
                let _ = tx.send(Err(SlockError::Io(std::io::Error::new(
                    err.kind(),
                    err.to_string(),
                ))));
            }
        }
        SlockError::Protocol(message) => {
            for (_, tx) in pending.drain() {
                let _ = tx.send(Err(SlockError::Protocol(message.clone())));
            }
        }
        other => {
            let message = other.to_string();
            for (_, tx) in pending.drain() {
                let _ = tx.send(Err(SlockError::Protocol(message.clone())));
            }
        }
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

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
    init_type: Mutex<u8>,
    writer: Arc<AsyncMutex<Option<OwnedWriteHalf>>>,
    pending: Arc<AsyncMutex<HashMap<Id16, PendingSender>>>,
    closed: Arc<AtomicBool>,
}

impl Connection {
    pub(crate) fn new(address: String, options: ClientOptions) -> Self {
        Self {
            address,
            options,
            client_id: Mutex::new(None),
            init_type: Mutex::new(0),
            writer: Arc::new(AsyncMutex::new(None)),
            pending: Arc::new(AsyncMutex::new(HashMap::new())),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) async fn open(&self) -> Result<()> {
        if self.writer.lock().await.is_some() {
            return Ok(());
        }
        self.closed.store(false, Ordering::SeqCst);
        let stream = tokio::time::timeout(
            self.options.connect_timeout,
            TcpStream::connect(&self.address),
        )
        .await
        .map_err(|_| SlockError::CommandTimeout)??;
        stream.set_nodelay(self.options.tcp_nodelay)?;
        let mut stream = stream;

        let client_id = {
            let mut guard = self.client_id.lock().expect("client id mutex poisoned");
            *guard.get_or_insert_with(Id16::new)
        };
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
        *self.init_type.lock().expect("init type mutex poisoned") = result.init_type;

        let (reader, writer) = stream.into_split();
        *self.writer.lock().await = Some(writer);
        self.spawn_reader(reader);
        Ok(())
    }

    pub(crate) async fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        if let Some(mut writer) = self.writer.lock().await.take() {
            let _ = writer.shutdown().await;
        }
        let mut pending = self.pending.lock().await;
        for (_, tx) in pending.drain() {
            let _ = tx.send(Err(SlockError::ClientClosed));
        }
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

    fn spawn_reader(&self, reader: OwnedReadHalf) {
        let pending = Arc::clone(&self.pending);
        let writer = Arc::clone(&self.writer);
        let closed = Arc::clone(&self.closed);
        tokio::spawn(async move {
            read_loop(reader, pending, writer, closed).await;
        });
    }
}

async fn read_loop(
    mut reader: OwnedReadHalf,
    pending: Arc<AsyncMutex<HashMap<Id16, PendingSender>>>,
    writer: Arc<AsyncMutex<Option<OwnedWriteHalf>>>,
    closed: Arc<AtomicBool>,
) {
    loop {
        let mut header = [0u8; HEADER_LEN];
        if let Err(err) = reader.read_exact(&mut header).await {
            if !closed.load(Ordering::SeqCst) {
                let mut pending = pending.lock().await;
                for (_, tx) in pending.drain() {
                    let _ = tx.send(Err(SlockError::Io(std::io::Error::new(
                        err.kind(),
                        err.to_string(),
                    ))));
                }
                let _ = writer.lock().await.take();
            }
            break;
        }

        let data = if response_has_extra_data(&header) {
            match read_extra_data(&mut reader).await {
                Ok(data) => Some(data),
                Err(_) => {
                    let mut pending = pending.lock().await;
                    for (_, tx) in pending.drain() {
                        let _ = tx.send(Err(SlockError::ClientClosed));
                    }
                    break;
                }
            }
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
}

async fn read_extra_data(reader: &mut OwnedReadHalf) -> Result<Vec<u8>> {
    let mut len_bytes = [0u8; 4];
    reader.read_exact(&mut len_bytes).await?;
    let len = u32::from_le_bytes(len_bytes) as usize;
    let mut data = vec![0u8; len + 4];
    reader.read_exact(&mut data[4..]).await?;
    Ok(data)
}

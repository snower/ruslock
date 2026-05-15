use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;

use socket2::SockRef;

use crate::error::{Result, SlockError};
use crate::options::ClientOptions;
use crate::protocol::codec::{decode_response, response_has_extra_data, HEADER_LEN};
use crate::protocol::command::{Command, InitCommand};
use crate::protocol::constants::COMMAND_RESULT_SUCCED;
use crate::protocol::id::Id16;
use crate::protocol::result::CommandResult;

type PendingSender = mpsc::Sender<Result<CommandResult>>;

#[derive(Debug)]
pub(crate) struct Connection {
    address: String,
    options: ClientOptions,
    client_id: Mutex<Option<Id16>>,
    writer: Arc<Mutex<Option<TcpStream>>>,
    pending: Arc<Mutex<HashMap<Id16, PendingSender>>>,
    closed: Arc<AtomicBool>,
}

impl Connection {
    pub(crate) fn new(address: String, options: ClientOptions) -> Self {
        Self {
            address,
            options,
            client_id: Mutex::new(None),
            writer: Arc::new(Mutex::new(None)),
            pending: Arc::new(Mutex::new(HashMap::new())),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn open(&self) -> Result<()> {
        if self.writer.lock().expect("writer mutex poisoned").is_some() {
            return Ok(());
        }
        self.closed.store(false, Ordering::SeqCst);
        let socket_addr = self
            .address
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| SlockError::Protocol(format!("address {} did not resolve", self.address)))?;
        let mut stream = TcpStream::connect_timeout(&socket_addr, self.options.connect_timeout)?;
        stream.set_nodelay(self.options.tcp_nodelay)?;
        if self.options.tcp_keepalive {
            SockRef::from(&stream).set_keepalive(true)?;
        }

        let client_id = {
            let mut guard = self.client_id.lock().expect("client id mutex poisoned");
            *guard.get_or_insert_with(Id16::new)
        };
        let init = Command::Init(InitCommand::with_client_id(client_id)).encode()?;
        stream.write_all(&init.header)?;
        if let Some(extra) = init.extra {
            stream.write_all(&extra)?;
        }
        stream.flush()?;

        let mut response_header = [0u8; HEADER_LEN];
        stream.read_exact(&mut response_header)?;
        let response = decode_response(&response_header, None)?;
        if response.result_code() != COMMAND_RESULT_SUCCED {
            return Err(SlockError::Server {
                result: response.result_code(),
            });
        }

        let reader = stream.try_clone()?;
        *self.writer.lock().expect("writer mutex poisoned") = Some(stream);
        self.spawn_reader(reader);
        Ok(())
    }

    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        if let Some(stream) = self.writer.lock().expect("writer mutex poisoned").take() {
            let _ = stream.shutdown(Shutdown::Both);
        }
        self.wake_all(SlockError::ClientClosed);
    }

    pub(crate) fn pending_len(&self) -> usize {
        self.pending.lock().expect("pending mutex poisoned").len()
    }

    pub(crate) fn send_command(&self, command: Command) -> Result<CommandResult> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(SlockError::ClientClosed);
        }
        let encoded = command.encode()?;
        let (tx, rx) = mpsc::channel();
        self.pending
            .lock()
            .expect("pending mutex poisoned")
            .insert(encoded.request_id, tx);

        let write_result = {
            let mut writer = self.writer.lock().expect("writer mutex poisoned");
            let Some(stream) = writer.as_mut() else {
                self.pending
                    .lock()
                    .expect("pending mutex poisoned")
                    .remove(&encoded.request_id);
                return Err(SlockError::NotConnected);
            };
            stream.write_all(&encoded.header).and_then(|_| {
                if let Some(extra) = &encoded.extra {
                    stream.write_all(extra)?;
                }
                stream.flush()
            })
        };

        if let Err(err) = write_result {
            self.pending
                .lock()
                .expect("pending mutex poisoned")
                .remove(&encoded.request_id);
            return Err(SlockError::Io(err));
        }

        let timeout = encoded.timeout + self.options.command_timeout_grace;
        match rx.recv_timeout(timeout) {
            Ok(result) => result,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                self.pending
                    .lock()
                    .expect("pending mutex poisoned")
                    .remove(&encoded.request_id);
                Err(SlockError::CommandTimeout)
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(SlockError::ClientClosed),
        }
    }

    fn spawn_reader(&self, mut stream: TcpStream) {
        let pending = Arc::clone(&self.pending);
        let writer = Arc::clone(&self.writer);
        let closed = Arc::clone(&self.closed);
        thread::Builder::new()
            .name(format!("ruslock-io-{}", self.address))
            .spawn(move || loop {
                let mut header = [0u8; HEADER_LEN];
                if let Err(err) = stream.read_exact(&mut header) {
                    if !closed.load(Ordering::SeqCst) {
                        let mut pending = pending.lock().expect("pending mutex poisoned");
                        for (_, tx) in pending.drain() {
                            let _ = tx.send(Err(SlockError::Io(std::io::Error::new(err.kind(), err.to_string()))));
                        }
                        let _ = writer.lock().expect("writer mutex poisoned").take();
                    }
                    break;
                }

                let data = if response_has_extra_data(&header) {
                    match read_extra_data(&mut stream) {
                        Ok(data) => Some(data),
                        Err(_err) => {
                            let mut pending = pending.lock().expect("pending mutex poisoned");
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
                let request_id = decoded.as_ref().ok().map(CommandResult::request_id).or_else(|| {
                    let mut bytes = [0u8; 16];
                    bytes.copy_from_slice(&header[3..19]);
                    Some(Id16::from_bytes(bytes))
                });
                if let Some(request_id) = request_id {
                    if let Some(tx) = pending.lock().expect("pending mutex poisoned").remove(&request_id) {
                        let _ = tx.send(decoded);
                    }
                }
            })
            .expect("failed to spawn ruslock reader thread");
    }

    fn wake_all(&self, _error: SlockError) {
        let mut pending = self.pending.lock().expect("pending mutex poisoned");
        for (_, tx) in pending.drain() {
            let _ = tx.send(Err(SlockError::ClientClosed));
        }
    }
}

fn read_extra_data(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut len_bytes = [0u8; 4];
    stream.read_exact(&mut len_bytes)?;
    let len = u32::from_le_bytes(len_bytes) as usize;
    let mut data = vec![0u8; len + 4];
    stream.read_exact(&mut data[4..])?;
    Ok(data)
}

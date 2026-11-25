use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

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
    init_type: Arc<Mutex<u8>>,
    writer: Arc<Mutex<Option<TcpStream>>>,
    pending: Arc<Mutex<HashMap<Id16, PendingSender>>>,
    closed: Arc<AtomicBool>,
    reader_running: Arc<AtomicBool>,
}

impl Connection {
    pub(crate) fn new(address: String, options: ClientOptions) -> Self {
        Self {
            address,
            options,
            client_id: Mutex::new(None),
            init_type: Arc::new(Mutex::new(0)),
            writer: Arc::new(Mutex::new(None)),
            pending: Arc::new(Mutex::new(HashMap::new())),
            closed: Arc::new(AtomicBool::new(false)),
            reader_running: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn open(&self) -> Result<()> {
        if self.reader_running.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        self.closed.store(false, Ordering::SeqCst);
        let client_id = {
            let mut guard = self.client_id.lock().expect("client id mutex poisoned");
            *guard.get_or_insert_with(Id16::new)
        };
        match connect_once(&self.address, &self.options, client_id) {
            Ok((stream, reader, init_type)) => {
                *self.init_type.lock().expect("init type mutex poisoned") = init_type;
                *self.writer.lock().expect("writer mutex poisoned") = Some(stream);
                self.spawn_reader(reader, client_id);
                Ok(())
            }
            Err(err) => {
                self.reader_running.store(false, Ordering::SeqCst);
                Err(err)
            }
        }
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

    pub(crate) fn init_type(&self) -> u8 {
        *self.init_type.lock().expect("init type mutex poisoned")
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
            close_writer(&self.writer);
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

    fn spawn_reader(&self, stream: TcpStream, client_id: Id16) {
        let address = self.address.clone();
        let options = self.options.clone();
        let init_type = Arc::clone(&self.init_type);
        let pending = Arc::clone(&self.pending);
        let writer = Arc::clone(&self.writer);
        let closed = Arc::clone(&self.closed);
        let reader_running = Arc::clone(&self.reader_running);
        thread::Builder::new()
            .name(format!("ruslock-io-{}", self.address))
            .spawn(move || {
                let mut reader = Some(stream);
                while !closed.load(Ordering::SeqCst) {
                    if let Some(mut stream) = reader.take() {
                        if let Err(err) =
                            read_loop(&mut stream, &pending, &closed, options.max_frame_size)
                        {
                            close_writer(&writer);
                            if !closed.load(Ordering::SeqCst) {
                                wake_all_io(&pending, err);
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
                        match connect_once(&address, &options, client_id) {
                            Ok((stream, new_reader, new_init_type)) => {
                                *init_type.lock().expect("init type mutex poisoned") =
                                    new_init_type;
                                *writer.lock().expect("writer mutex poisoned") = Some(stream);
                                reader = Some(new_reader);
                                break;
                            }
                            Err(_) => thread::sleep(options.reconnect_interval),
                        }
                    }
                }
                close_writer(&writer);
                reader_running.store(false, Ordering::SeqCst);
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

fn connect_once(
    address: &str,
    options: &ClientOptions,
    client_id: Id16,
) -> Result<(TcpStream, TcpStream, u8)> {
    let socket_addr = address
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| SlockError::Protocol(format!("address {address} did not resolve")))?;
    let deadline = Instant::now() + options.connect_timeout;
    let mut stream = TcpStream::connect_timeout(&socket_addr, remaining(deadline)?)?;
    stream.set_nodelay(options.tcp_nodelay)?;
    if options.tcp_keepalive {
        SockRef::from(&stream).set_keepalive(true)?;
    }
    stream.set_read_timeout(Some(remaining(deadline)?))?;
    stream.set_write_timeout(Some(remaining(deadline)?))?;

    let init = Command::Init(InitCommand::with_client_id(client_id)).encode()?;
    write_all_deadline(&mut stream, &init.header, deadline)?;
    if let Some(extra) = init.extra {
        write_all_deadline(&mut stream, &extra, deadline)?;
    }
    flush_deadline(&mut stream, deadline)?;

    let mut response_header = [0u8; HEADER_LEN];
    read_exact_deadline(&mut stream, &mut response_header, deadline)?;
    let response = decode_response(&response_header, None)?;
    if response.result_code() != COMMAND_RESULT_SUCCED {
        return Err(SlockError::Server {
            result: response.result_code(),
        });
    }
    let CommandResult::Init(result) = response else {
        return Err(SlockError::Protocol("expected init result".to_string()));
    };

    stream.set_read_timeout(None)?;
    stream.set_write_timeout(None)?;

    let reader = stream.try_clone()?;
    Ok((stream, reader, result.init_type))
}

fn read_loop(
    stream: &mut TcpStream,
    pending: &Arc<Mutex<HashMap<Id16, PendingSender>>>,
    closed: &Arc<AtomicBool>,
    max_frame_size: usize,
) -> Result<()> {
    while !closed.load(Ordering::SeqCst) {
        let mut header = [0u8; HEADER_LEN];
        stream.read_exact(&mut header)?;

        let data = if response_has_extra_data(&header) {
            Some(read_extra_data(stream, max_frame_size)?)
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
            if let Some(tx) = pending
                .lock()
                .expect("pending mutex poisoned")
                .remove(&request_id)
            {
                let _ = tx.send(decoded);
            }
        }
    }
    Ok(())
}

fn close_writer(writer: &Arc<Mutex<Option<TcpStream>>>) {
    if let Some(stream) = writer.lock().expect("writer mutex poisoned").take() {
        let _ = stream.shutdown(Shutdown::Both);
    }
}

fn wake_all_io(pending: &Arc<Mutex<HashMap<Id16, PendingSender>>>, err: SlockError) {
    let mut pending = pending.lock().expect("pending mutex poisoned");
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

fn remaining(deadline: Instant) -> std::io::Result<Duration> {
    match deadline.checked_duration_since(Instant::now()) {
        Some(remaining) if !remaining.is_zero() => Ok(remaining),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "connect timed out",
        )),
    }
}

fn write_all_deadline(
    stream: &mut TcpStream,
    mut buf: &[u8],
    deadline: Instant,
) -> std::io::Result<()> {
    while !buf.is_empty() {
        stream.set_write_timeout(Some(remaining(deadline)?))?;
        match stream.write(buf) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "failed to write whole buffer",
                ))
            }
            Ok(written) => buf = &buf[written..],
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

fn read_exact_deadline(
    stream: &mut TcpStream,
    buf: &mut [u8],
    deadline: Instant,
) -> std::io::Result<()> {
    let mut filled = 0usize;
    while filled < buf.len() {
        stream.set_read_timeout(Some(remaining(deadline)?))?;
        match stream.read(&mut buf[filled..]) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                ))
            }
            Ok(read) => filled += read,
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

fn flush_deadline(stream: &mut TcpStream, deadline: Instant) -> std::io::Result<()> {
    stream.set_write_timeout(Some(remaining(deadline)?))?;
    stream.flush()
}

fn read_extra_data(stream: &mut TcpStream, max_frame_size: usize) -> Result<Vec<u8>> {
    let mut len_bytes = [0u8; 4];
    stream.read_exact(&mut len_bytes)?;
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
    stream.read_exact(&mut data[4..])?;
    Ok(data)
}

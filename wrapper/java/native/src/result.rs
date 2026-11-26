//! Structured operation result transport for Java.
//!
//! The native side never calls back into Java from a Tokio task. Every
//! submitted operation completes exactly once into a [`NativeResult`], which is
//! later serialized to a small little-endian byte frame that the package-private
//! `NativeBridge` parses on the calling Java thread.
//!
//! Frame layout (all integers little endian):
//!
//! ```text
//! u8  version (1)
//! u8  status (0 ok, 1 error)
//! u8  error_kind
//! u8  flags (bit0 has_lock_result, bit1 has_bool, bit2 bool_value,
//!            bit3 has_server_result, bit4 has_bytes)
//! u8  server_result
//! u8  reserved
//! u16 reserved
//! u32 message_len
//! u8[message_len] utf8 message
//! if has_bytes:
//!   u32 bytes_len
//!   u8[bytes_len] raw transport bytes
//! if has_lock_result:
//!   u8 command_type, u8 result, u8 flag, u8 db_id
//!   u8 lr_count, u8 r_count, u8 has_data, u8 reserved
//!   u16 l_count, u16 count
//!   u32 data_len
//!   u8[16] request_id, u8[16] lock_id, u8[16] lock_key
//!   u8[data_len] raw lock result data
//! ```

use ruslock::error::SlockError;
use ruslock::protocol::result::LockCommandResult;

pub(crate) const RESULT_VERSION: u8 = 1;
pub(crate) const STATUS_OK: u8 = 0;
pub(crate) const STATUS_ERROR: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum ErrorKind {
    None = 0,
    ClientClosed = 1,
    NotConnected = 2,
    ClientDisconnected = 3,
    CommandTimeout = 4,
    LockLocked = 5,
    LockUnlocked = 6,
    LockNotOwn = 7,
    LockTimeout = 8,
    LockExpired = 9,
    StateError = 10,
    Server = 11,
    Io = 12,
    Protocol = 13,
    LockData = 14,
    EventWaitTimeout = 15,
    InvalidArgument = 19,
    Internal = 20,
}

#[derive(Clone, Debug)]
pub(crate) struct NativeResult {
    status: u8,
    error: ErrorKind,
    has_value: bool,
    value: bool,
    lock: Option<LockCommandResult>,
    server_result: Option<u8>,
    bytes: Option<Vec<u8>>,
    message: Option<String>,
}

impl NativeResult {
    pub(crate) fn ok_unit() -> Self {
        Self {
            status: STATUS_OK,
            error: ErrorKind::None,
            has_value: false,
            value: false,
            lock: None,
            server_result: None,
            bytes: None,
            message: None,
        }
    }

    pub(crate) fn ok_bool(value: bool) -> Self {
        Self {
            status: STATUS_OK,
            error: ErrorKind::None,
            has_value: true,
            value,
            lock: None,
            server_result: None,
            bytes: None,
            message: None,
        }
    }

    pub(crate) fn ok_lock(result: LockCommandResult) -> Self {
        Self {
            status: STATUS_OK,
            error: ErrorKind::None,
            has_value: false,
            value: false,
            lock: Some(result),
            server_result: None,
            bytes: None,
            message: None,
        }
    }

    pub(crate) fn ok_optional_lock(result: Option<LockCommandResult>) -> Self {
        let mut value = Self::ok_unit();
        value.lock = result;
        value
    }

    pub(crate) fn ok_bytes(bytes: Vec<u8>) -> Self {
        let mut value = Self::ok_unit();
        value.bytes = Some(bytes);
        value
    }

    pub(crate) fn bytes(&self) -> Option<&[u8]> {
        self.bytes.as_deref()
    }

    pub(crate) fn error(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            status: STATUS_ERROR,
            error: kind,
            has_value: false,
            value: false,
            lock: None,
            server_result: None,
            bytes: None,
            message: Some(message.into()),
        }
    }

    pub(crate) fn error_with_lock(
        kind: ErrorKind,
        result: LockCommandResult,
        message: impl Into<String>,
    ) -> Self {
        Self {
            status: STATUS_ERROR,
            lock: Some(result),
            ..Self::error(kind, message)
        }
    }

    pub(crate) fn error_with_server(
        kind: ErrorKind,
        result: u8,
        message: impl Into<String>,
    ) -> Self {
        Self {
            status: STATUS_ERROR,
            server_result: Some(result),
            ..Self::error(kind, message)
        }
    }

    pub(crate) fn invalid_argument(message: impl Into<String>) -> Self {
        Self::error(ErrorKind::InvalidArgument, message)
    }

    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self::error(ErrorKind::Internal, message)
    }

    pub(crate) fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    pub(crate) fn error_kind(&self) -> ErrorKind {
        self.error
    }

    pub(crate) fn is_ok(&self) -> bool {
        self.status == STATUS_OK
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(64);
        out.push(RESULT_VERSION);
        out.push(self.status);
        out.push(self.error as u8);

        let mut flags = 0u8;
        if self.lock.is_some() {
            flags |= 1;
        }
        if self.has_value {
            flags |= 2;
        }
        if self.value {
            flags |= 4;
        }
        if self.server_result.is_some() {
            flags |= 8;
        }
        if self.bytes.is_some() {
            flags |= 16;
        }
        out.push(flags);
        out.push(self.server_result.unwrap_or(0));
        out.push(0);
        out.extend_from_slice(&0u16.to_le_bytes());

        let message = self.message.as_deref().unwrap_or("");
        out.extend_from_slice(&(message.len() as u32).to_le_bytes());
        out.extend_from_slice(message.as_bytes());

        if let Some(bytes) = &self.bytes {
            out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(bytes);
        }

        if let Some(lock) = &self.lock {
            out.push(lock.command_type);
            out.push(lock.result);
            out.push(lock.flag);
            out.push(lock.db_id);
            out.push(lock.lr_count);
            out.push(lock.r_count);

            let data = lock.data.as_ref().map(|data| data.raw()).unwrap_or(&[]);
            out.push(if lock.data.is_some() { 1 } else { 0 });
            out.push(0);
            out.extend_from_slice(&lock.l_count.to_le_bytes());
            out.extend_from_slice(&lock.count.to_le_bytes());
            out.extend_from_slice(&(data.len() as u32).to_le_bytes());
            out.extend_from_slice(lock.request_id.as_bytes());
            out.extend_from_slice(lock.lock_id.as_bytes());
            out.extend_from_slice(lock.lock_key.as_bytes());
            out.extend_from_slice(data);
        }

        out
    }
}

pub(crate) trait IntoNativeResult {
    fn into_native_result(self) -> NativeResult;
}

impl IntoNativeResult for () {
    fn into_native_result(self) -> NativeResult {
        NativeResult::ok_unit()
    }
}

impl IntoNativeResult for bool {
    fn into_native_result(self) -> NativeResult {
        NativeResult::ok_bool(self)
    }
}

impl IntoNativeResult for LockCommandResult {
    fn into_native_result(self) -> NativeResult {
        NativeResult::ok_lock(self)
    }
}

impl IntoNativeResult for Option<LockCommandResult> {
    fn into_native_result(self) -> NativeResult {
        NativeResult::ok_optional_lock(self)
    }
}

pub(crate) fn from_result<T: IntoNativeResult>(result: ruslock::Result<T>) -> NativeResult {
    match result {
        Ok(value) => value.into_native_result(),
        Err(err) => from_error(err),
    }
}

pub(crate) fn from_error(err: SlockError) -> NativeResult {
    let message = err.to_string();
    match err {
        SlockError::ClientClosed => NativeResult::error(ErrorKind::ClientClosed, message),
        SlockError::NotConnected => NativeResult::error(ErrorKind::NotConnected, message),
        SlockError::ClientDisconnected => {
            NativeResult::error(ErrorKind::ClientDisconnected, message)
        }
        SlockError::CommandTimeout => NativeResult::error(ErrorKind::CommandTimeout, message),
        SlockError::LockLocked(result) => {
            NativeResult::error_with_lock(ErrorKind::LockLocked, *result, message)
        }
        SlockError::LockUnlocked(result) => {
            NativeResult::error_with_lock(ErrorKind::LockUnlocked, *result, message)
        }
        SlockError::LockNotOwn(result) => {
            NativeResult::error_with_lock(ErrorKind::LockNotOwn, *result, message)
        }
        SlockError::LockTimeout(result) => {
            NativeResult::error_with_lock(ErrorKind::LockTimeout, *result, message)
        }
        SlockError::LockExpired(result) => {
            NativeResult::error_with_lock(ErrorKind::LockExpired, *result, message)
        }
        SlockError::StateError(result) => {
            NativeResult::error_with_lock(ErrorKind::StateError, *result, message)
        }
        SlockError::Server { result } => {
            NativeResult::error_with_server(ErrorKind::Server, result, message)
        }
        SlockError::Io(_) => NativeResult::error(ErrorKind::Io, message),
        SlockError::Protocol(_) => NativeResult::error(ErrorKind::Protocol, message),
        SlockError::LockData(_) => NativeResult::error(ErrorKind::LockData, message),
        SlockError::EventWaitTimeout => {
            NativeResult::error(ErrorKind::EventWaitTimeout, message)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ruslock::key::Key16;
    use ruslock::protocol::id::Id16;

    fn lock_result() -> LockCommandResult {
        LockCommandResult {
            command_type: 1,
            request_id: Id16::from_bytes([1; 16]),
            result: 0,
            flag: 2,
            db_id: 3,
            lock_id: Id16::from_bytes([4; 16]),
            lock_key: Key16::from_bytes([5; 16]),
            l_count: 6,
            count: 7,
            lr_count: 8,
            r_count: 9,
            data: None,
        }
    }

    #[test]
    fn error_results_keep_the_typed_lock_payload() {
        let encoded = NativeResult::error_with_lock(
            ErrorKind::LockLocked,
            lock_result(),
            "lock locked",
        )
        .encode();

        assert_eq!(encoded[0], RESULT_VERSION);
        assert_eq!(encoded[1], STATUS_ERROR);
        assert_eq!(encoded[2], ErrorKind::LockLocked as u8);
        assert_eq!(encoded[3] & 1, 1);
        let message_len = u32::from_le_bytes(encoded[8..12].try_into().unwrap()) as usize;
        assert_eq!(&encoded[12..12 + message_len], b"lock locked");
        let lock_offset = 12 + message_len;
        assert_eq!(encoded[lock_offset], 1);
        assert_eq!(encoded[lock_offset + 2], 2);
        assert_eq!(encoded[lock_offset + 3], 3);
    }
}

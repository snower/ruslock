//! Validated raw command frames for the binding's low-level entry points.
//!
//! Java exposes `sendCommand` / `writeCommand` for caller-defined `Command`
//! subclasses. The binding cannot route those through the built-in `Command`
//! enum without losing data, so this module accepts an already-encoded 64-byte
//! header plus its raw extra bytes and validates only what the transport needs:
//! the protocol magic/version, the request id, the command type, and the
//! header/extra boundary.
//!
//! Scope is deliberately partial. This module owns no socket and no retry
//! policy; the crate's `aio::connection` module remains the single transport
//! path and can reuse [`RawCommand`] or its [`EncodedCommand`] representation. The response
//! helper serializes decoded results back into the byte layout the Java
//! `CommandResult` decoders (`parseCommand` / `loadCommandData`) consume.
//!
//! Extra bytes are preserved exactly as supplied. For lock responses the raw
//! extra includes the Java `payload_len + 4` length prefix, because the Java
//! `LockResultData` offsets are relative to that prefix.

use std::time::Duration;

use crate::error::{Result, SlockError};
use crate::protocol::codec::{validate_header, HEADER_LEN};
use crate::protocol::command::EncodedCommand;
use crate::protocol::constants::{COMMAND_TYPE_INIT, COMMAND_TYPE_PING, MAGIC, VERSION};
use crate::protocol::id::Id16;
use crate::protocol::result::CommandResult;

/// A validated, caller-supplied command frame.
///
/// Construction never re-encodes known command types: the header and extra
/// bytes are kept verbatim so unknown/custom command types and reserved header
/// bytes survive the binding round trip.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawCommand {
    request_id: Id16,
    command_type: u8,
    header: [u8; HEADER_LEN],
    extra: Option<Vec<u8>>,
    timeout: Duration,
    expects_response: bool,
    exact_timeout: bool,
}

impl RawCommand {
    /// Validates raw header bytes and preserves them unchanged.
    ///
    /// `extra` distinguishes `None` (no extra frame) from `Some(Vec::new())`
    /// (an explicitly empty payload); both produce the same wire bytes.
    pub fn from_parts(
        header: [u8; HEADER_LEN],
        extra: Option<Vec<u8>>,
        timeout: Duration,
        expects_response: bool,
    ) -> Result<Self> {
        validate_header(&header)?;
        let mut request_id = [0u8; 16];
        request_id.copy_from_slice(&header[3..19]);
        Ok(Self {
            request_id: Id16::from_bytes(request_id),
            command_type: header[2],
            header,
            extra,
            timeout,
            expects_response,
            exact_timeout: false,
        })
    }

    /// Splits a complete command frame into its 64-byte header and extra bytes.
    ///
    /// Any bytes after the header are stored verbatim without adding or
    /// removing a length prefix.
    pub fn from_frame(frame: &[u8], timeout: Duration, expects_response: bool) -> Result<Self> {
        if frame.len() < HEADER_LEN {
            return Err(SlockError::Protocol(format!(
                "raw command frame is {} bytes, expected at least {HEADER_LEN}",
                frame.len()
            )));
        }
        let mut header = [0u8; HEADER_LEN];
        header.copy_from_slice(&frame[..HEADER_LEN]);
        let extra = (frame.len() > HEADER_LEN).then(|| frame[HEADER_LEN..].to_vec());
        Self::from_parts(header, extra, timeout, expects_response)
    }

    /// Validates an existing codec representation without re-encoding it.
    ///
    /// The redundant `command_type` field must agree with the header so a
    /// malformed shape cannot carry two different command types.
    pub fn from_encoded(encoded: EncodedCommand) -> Result<Self> {
        if encoded.command_type != encoded.header[2] {
            return Err(SlockError::Protocol(format!(
                "encoded command type {:#04x} disagrees with header type {:#04x}",
                encoded.command_type, encoded.header[2]
            )));
        }
        Self::from_parts(
            encoded.header,
            encoded.extra,
            encoded.timeout,
            encoded.expects_response,
        )
    }

    pub fn request_id(&self) -> Id16 {
        self.request_id
    }

    pub fn command_type(&self) -> u8 {
        self.command_type
    }

    pub fn header(&self) -> &[u8; HEADER_LEN] {
        &self.header
    }

    pub fn extra(&self) -> Option<&[u8]> {
        self.extra.as_deref()
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Whether the raw response wait uses [`RawCommand::timeout`] exactly.
    pub fn exact_timeout(&self) -> bool {
        self.exact_timeout
    }

    /// Opts the response wait into an exact deadline.
    ///
    /// By default a raw response wait is [`RawCommand::timeout`] plus
    /// [`ClientOptions::command_timeout_grace`](crate::options::ClientOptions::command_timeout_grace),
    /// matching the regular command path. With this opt-in the wait is exactly
    /// [`RawCommand::timeout`], so a caller that already computed a total
    /// deadline never gets a second grace period.
    ///
    /// The write itself is never wrapped in this deadline: only the wait for a
    /// response is shortened. This keeps a cancelled caller from abandoning a
    /// partially written frame.
    pub fn with_exact_timeout(mut self) -> Self {
        self.exact_timeout = true;
        self
    }

    /// The actual response-wait duration for this command.
    pub fn response_timeout(&self, grace: Duration) -> Duration {
        if self.exact_timeout {
            self.timeout
        } else {
            self.timeout_with_grace(grace)
        }
    }

    pub fn expects_response(&self) -> bool {
        self.expects_response
    }

    /// True for the Java `writeCommand` shape: write the frame and do not wait
    /// for, or fabricate, a response.
    pub fn is_write_only(&self) -> bool {
        !self.expects_response
    }

    /// The client-side deadline uses the same timeout-plus-grace rule as the
    /// regular command path.
    pub fn timeout_with_grace(&self, grace: Duration) -> Duration {
        self.timeout.saturating_add(grace)
    }

    /// The exact bytes to write: header followed by raw extra bytes, if any.
    pub fn frame(&self) -> Vec<u8> {
        let mut frame = Vec::with_capacity(HEADER_LEN + self.extra.as_ref().map_or(0, Vec::len));
        frame.extend_from_slice(&self.header);
        if let Some(extra) = &self.extra {
            frame.extend_from_slice(extra);
        }
        frame
    }

    /// Converts back to the shared codec representation for transport reuse.
    pub fn into_encoded(self) -> EncodedCommand {
        EncodedCommand::new(
            self.request_id,
            self.command_type,
            self.header,
            self.extra,
            self.timeout,
            self.expects_response,
        )
    }
}

/// Crate-internal classification of a raw send failure for replset routing.
///
/// Replset must not blindly replay a command whose write outcome is unknown.
/// `NotSent` is safe to retry on another node; `Unknown` means bytes may have
/// reached the server and the command must not be replayed as if unsent.
/// This stays crate-private so the public `SlockError` enum is not expanded.
#[derive(Debug)]
pub(crate) enum RawSendFailure {
    NotSent(SlockError),
    Unknown(SlockError),
}

impl RawSendFailure {
    pub(crate) fn into_error(self) -> SlockError {
        match self {
            Self::NotSent(err) | Self::Unknown(err) => err,
        }
    }
}

/// Serializes the 64-byte response header in the Java `parseCommand` layout.
pub fn encode_response_header(result: &CommandResult) -> [u8; HEADER_LEN] {
    let mut header = [0u8; HEADER_LEN];
    header[0] = MAGIC;
    header[1] = VERSION;
    match result {
        CommandResult::Init(result) => {
            header[2] = COMMAND_TYPE_INIT;
            header[3..19].copy_from_slice(result.request_id.as_bytes());
            header[19] = result.result;
            header[20] = result.init_type;
        }
        CommandResult::Ping(result) => {
            header[2] = COMMAND_TYPE_PING;
            header[3..19].copy_from_slice(result.request_id.as_bytes());
            header[19] = result.result;
        }
        CommandResult::Lock(result) => {
            header[2] = result.command_type;
            header[3..19].copy_from_slice(result.request_id.as_bytes());
            header[19] = result.result;
            header[20] = result.flag;
            header[21] = result.db_id;
            header[22..38].copy_from_slice(result.lock_id.as_bytes());
            header[38..54].copy_from_slice(result.lock_key.as_bytes());
            header[54..56].copy_from_slice(&result.l_count.to_le_bytes());
            header[56..58].copy_from_slice(&result.count.to_le_bytes());
            header[58] = result.lr_count;
            header[59] = result.r_count;
        }
    }
    header
}

/// Serializes a response into the exact bytes handed to Java for decoding.
///
/// Lock responses append the `LockResultData` raw bytes, including the 4-byte
/// length prefix expected by `LockCommandResult.loadCommandData`.
pub fn encode_response_frame(result: &CommandResult) -> Vec<u8> {
    let header = encode_response_header(result);
    let extra = match result {
        CommandResult::Lock(result) => result.data.as_ref().map(|data| data.raw()),
        CommandResult::Init(_) | CommandResult::Ping(_) => None,
    };
    let mut frame = Vec::with_capacity(HEADER_LEN + extra.map_or(0, <[u8]>::len));
    frame.extend_from_slice(&header);
    if let Some(extra) = extra {
        frame.extend_from_slice(extra);
    }
    frame
}

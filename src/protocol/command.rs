use std::time::Duration;

use crate::data::LockData;
use crate::error::Result;
use crate::key::Key16;
use crate::protocol::constants::*;
use crate::protocol::id::Id16;
use crate::time::PackedTime;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InitCommand {
    pub request_id: Id16,
    pub client_id: Id16,
}

impl InitCommand {
    pub const fn new(request_id: Id16, client_id: Id16) -> Self {
        Self { request_id, client_id }
    }

    pub fn with_client_id(client_id: Id16) -> Self {
        Self {
            request_id: Id16::new(),
            client_id,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PingCommand {
    pub request_id: Id16,
}

impl PingCommand {
    pub const fn new(request_id: Id16) -> Self {
        Self { request_id }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LockCommand {
    pub command_type: u8,
    pub flag: u8,
    pub db_id: u8,
    pub request_id: Id16,
    pub lock_key: Key16,
    pub lock_id: Id16,
    pub timeout: PackedTime,
    pub expired: PackedTime,
    pub count: u16,
    pub r_count: u8,
    pub data: Option<LockData>,
}

impl LockCommand {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        command_type: u8,
        flag: u8,
        db_id: u8,
        request_id: Id16,
        lock_key: Key16,
        lock_id: Id16,
        timeout: PackedTime,
        expired: PackedTime,
        count: u16,
        r_count: u8,
        data: Option<LockData>,
    ) -> Self {
        Self {
            command_type,
            flag,
            db_id,
            request_id,
            lock_key,
            lock_id,
            timeout,
            expired,
            count,
            r_count,
            data,
        }
    }

    pub fn has_extra_data(&self) -> bool {
        (self.flag & LOCK_FLAG_CONTAINS_DATA) != 0 || (self.flag & UNLOCK_FLAG_CONTAINS_DATA) != 0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Command {
    Init(InitCommand),
    Ping(PingCommand),
    Lock(LockCommand),
}

impl Command {
    pub fn request_id(&self) -> Id16 {
        match self {
            Command::Init(command) => command.request_id,
            Command::Ping(command) => command.request_id,
            Command::Lock(command) => command.request_id,
        }
    }

    pub fn command_type(&self) -> u8 {
        match self {
            Command::Init(_) => COMMAND_TYPE_INIT,
            Command::Ping(_) => COMMAND_TYPE_PING,
            Command::Lock(command) => command.command_type,
        }
    }

    pub fn encode(&self) -> Result<EncodedCommand> {
        match self {
            Command::Init(command) => {
                let mut header = [0u8; 64];
                header[0] = MAGIC;
                header[1] = VERSION;
                header[2] = COMMAND_TYPE_INIT;
                header[3..19].copy_from_slice(command.request_id.as_bytes());
                header[19..35].copy_from_slice(command.client_id.as_bytes());
                Ok(EncodedCommand::new(
                    command.request_id,
                    COMMAND_TYPE_INIT,
                    header,
                    None,
                    Duration::ZERO,
                    true,
                ))
            }
            Command::Ping(command) => {
                let mut header = [0u8; 64];
                header[0] = MAGIC;
                header[1] = VERSION;
                header[2] = COMMAND_TYPE_PING;
                header[3..19].copy_from_slice(command.request_id.as_bytes());
                Ok(EncodedCommand::new(
                    command.request_id,
                    COMMAND_TYPE_PING,
                    header,
                    None,
                    Duration::ZERO,
                    true,
                ))
            }
            Command::Lock(command) => {
                let mut header = [0u8; 64];
                header[0] = MAGIC;
                header[1] = VERSION;
                header[2] = command.command_type;
                header[3..19].copy_from_slice(command.request_id.as_bytes());
                header[19] = command.flag;
                header[20] = command.db_id;
                header[21..37].copy_from_slice(command.lock_id.as_bytes());
                header[37..53].copy_from_slice(command.lock_key.as_bytes());
                header[53..57].copy_from_slice(&command.timeout.bits().to_le_bytes());
                header[57..61].copy_from_slice(&command.expired.bits().to_le_bytes());
                header[61..63].copy_from_slice(&command.count.to_le_bytes());
                header[63] = command.r_count;
                let extra = if command.has_extra_data() {
                    Some(command.data.as_ref().ok_or_else(|| {
                        crate::error::SlockError::LockData("data flag set but data is missing".to_string())
                    })?.encode()?)
                } else {
                    None
                };
                Ok(EncodedCommand::new(
                    command.request_id,
                    command.command_type,
                    header,
                    extra,
                    Duration::from_secs(command.timeout.value() as u64),
                    true,
                ))
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedCommand {
    pub request_id: Id16,
    pub command_type: u8,
    pub header: [u8; 64],
    pub extra: Option<Vec<u8>>,
    pub timeout: Duration,
    pub expects_response: bool,
}

impl EncodedCommand {
    pub const fn new(
        request_id: Id16,
        command_type: u8,
        header: [u8; 64],
        extra: Option<Vec<u8>>,
        timeout: Duration,
        expects_response: bool,
    ) -> Self {
        Self {
            request_id,
            command_type,
            header,
            extra,
            timeout,
            expects_response,
        }
    }

    pub fn frame(&self) -> Vec<u8> {
        let mut frame = Vec::with_capacity(64 + self.extra.as_ref().map_or(0, Vec::len));
        frame.extend_from_slice(&self.header);
        if let Some(extra) = &self.extra {
            frame.extend_from_slice(extra);
        }
        frame
    }
}

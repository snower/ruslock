use crate::data::LockResultData;
use crate::error::{Result, SlockError};
use crate::key::Key16;
use crate::protocol::constants::*;
use crate::protocol::id::Id16;
use crate::protocol::result::{
    CommandResult, InitCommandResult, LockCommandResult, PingCommandResult,
};

pub const HEADER_LEN: usize = 64;

pub fn validate_header(header: &[u8; HEADER_LEN]) -> Result<()> {
    if header[0] != MAGIC {
        return Err(SlockError::Protocol(format!(
            "unknown magic {:02x}, expected {:02x}",
            header[0], MAGIC
        )));
    }
    if header[1] != VERSION {
        return Err(SlockError::Protocol(format!(
            "unknown version {:02x}, expected {:02x}",
            header[1], VERSION
        )));
    }
    Ok(())
}

pub fn decode_response_header(header: &[u8; HEADER_LEN]) -> Result<CommandResult> {
    decode_response(header, None)
}

pub fn decode_response(header: &[u8; HEADER_LEN], data: Option<Vec<u8>>) -> Result<CommandResult> {
    validate_header(header)?;
    let command_type = header[2];
    let request_id = read_id(&header[3..19]);
    let result = header[19];
    match command_type {
        COMMAND_TYPE_INIT => Ok(CommandResult::Init(InitCommandResult {
            request_id,
            result,
            init_type: header[20],
        })),
        COMMAND_TYPE_PING => Ok(CommandResult::Ping(PingCommandResult {
            request_id,
            result,
        })),
        COMMAND_TYPE_LOCK | COMMAND_TYPE_UNLOCK => {
            let lock_id = read_id(&header[22..38]);
            let lock_key = read_key(&header[38..54]);
            let l_count = u16::from_le_bytes([header[54], header[55]]);
            let count = u16::from_le_bytes([header[56], header[57]]);
            Ok(CommandResult::Lock(LockCommandResult {
                command_type,
                request_id,
                result,
                flag: header[20],
                db_id: header[21],
                lock_id,
                lock_key,
                l_count,
                count,
                lr_count: header[58],
                r_count: header[59],
                data: data.map(LockResultData::new),
            }))
        }
        other => Err(SlockError::Protocol(format!(
            "unknown command type {other:#04x}"
        ))),
    }
}

pub fn response_has_extra_data(header: &[u8; HEADER_LEN]) -> bool {
    matches!(header[2], COMMAND_TYPE_LOCK | COMMAND_TYPE_UNLOCK)
        && (header[20] & LOCK_FLAG_CONTAINS_DATA) != 0
}

fn read_id(bytes: &[u8]) -> Id16 {
    let mut id = [0u8; 16];
    id.copy_from_slice(bytes);
    Id16::from_bytes(id)
}

fn read_key(bytes: &[u8]) -> Key16 {
    let mut key = [0u8; 16];
    key.copy_from_slice(bytes);
    Key16::from_bytes(key)
}

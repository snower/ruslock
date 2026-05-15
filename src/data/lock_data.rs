use crate::error::{Result, SlockError};
use crate::protocol::command::LockCommand;
use crate::protocol::constants::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LockData {
    stage: u8,
    command_type: u8,
    command_flag: u8,
    value: LockDataValue,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum LockDataValue {
    Bytes(Vec<u8>),
    Pipeline(Vec<LockData>),
}

impl LockData {
    pub fn new(stage: u8, command_type: u8, command_flag: u8, value: Vec<u8>) -> Self {
        Self {
            stage,
            command_type,
            command_flag,
            value: LockDataValue::Bytes(value),
        }
    }

    pub fn set<V: Into<Vec<u8>>>(value: V) -> Self {
        Self::new(LOCK_DATA_STAGE_CURRENT, LOCK_DATA_COMMAND_TYPE_SET, 0, value.into())
    }

    pub fn set_with_flags<V: Into<Vec<u8>>>(value: V, flags: u8) -> Self {
        Self::new(LOCK_DATA_STAGE_CURRENT, LOCK_DATA_COMMAND_TYPE_SET, flags, value.into())
    }

    pub fn unset() -> Self {
        Self::new(LOCK_DATA_STAGE_CURRENT, LOCK_DATA_COMMAND_TYPE_UNSET, 0, Vec::new())
    }

    pub fn unset_with_flags(flags: u8) -> Self {
        Self::new(LOCK_DATA_STAGE_CURRENT, LOCK_DATA_COMMAND_TYPE_UNSET, flags, Vec::new())
    }

    pub fn incr(value: i64) -> Self {
        Self::new(
            LOCK_DATA_STAGE_CURRENT,
            LOCK_DATA_COMMAND_TYPE_INCR,
            LOCK_DATA_FLAG_VALUE_TYPE_NUMBER,
            value.to_le_bytes().to_vec(),
        )
    }

    pub fn incr_with_flags(value: i64, flags: u8) -> Self {
        Self::new(
            LOCK_DATA_STAGE_CURRENT,
            LOCK_DATA_COMMAND_TYPE_INCR,
            flags | LOCK_DATA_FLAG_VALUE_TYPE_NUMBER,
            value.to_le_bytes().to_vec(),
        )
    }

    pub fn append<V: Into<Vec<u8>>>(value: V) -> Self {
        Self::new(LOCK_DATA_STAGE_CURRENT, LOCK_DATA_COMMAND_TYPE_APPEND, 0, value.into())
    }

    pub fn append_with_flags<V: Into<Vec<u8>>>(value: V, flags: u8) -> Self {
        Self::new(LOCK_DATA_STAGE_CURRENT, LOCK_DATA_COMMAND_TYPE_APPEND, flags, value.into())
    }

    pub fn shift(length: u32) -> Self {
        Self::new(
            LOCK_DATA_STAGE_CURRENT,
            LOCK_DATA_COMMAND_TYPE_SHIFT,
            LOCK_DATA_FLAG_VALUE_TYPE_NUMBER,
            length.to_le_bytes().to_vec(),
        )
    }

    pub fn shift_with_flags(length: u32, flags: u8) -> Self {
        Self::new(
            LOCK_DATA_STAGE_CURRENT,
            LOCK_DATA_COMMAND_TYPE_SHIFT,
            flags | LOCK_DATA_FLAG_VALUE_TYPE_NUMBER,
            length.to_le_bytes().to_vec(),
        )
    }

    pub fn execute(command: &LockCommand) -> Result<Self> {
        Self::execute_with_stage(LOCK_DATA_STAGE_CURRENT, command, 0)
    }

    pub fn execute_with_stage(stage: u8, command: &LockCommand, flags: u8) -> Result<Self> {
        let encoded = crate::protocol::command::Command::Lock(command.clone()).encode()?;
        Ok(Self::new(stage, LOCK_DATA_COMMAND_TYPE_EXECUTE, flags, encoded.header.to_vec()))
    }

    pub fn pipeline(items: Vec<LockData>) -> Self {
        Self {
            stage: LOCK_DATA_STAGE_CURRENT,
            command_type: LOCK_DATA_COMMAND_TYPE_PIPELINE,
            command_flag: 0,
            value: LockDataValue::Pipeline(items),
        }
    }

    pub fn pipeline_with_flags(items: Vec<LockData>, flags: u8) -> Self {
        Self {
            stage: LOCK_DATA_STAGE_CURRENT,
            command_type: LOCK_DATA_COMMAND_TYPE_PIPELINE,
            command_flag: flags,
            value: LockDataValue::Pipeline(items),
        }
    }

    pub fn push<V: Into<Vec<u8>>>(value: V) -> Self {
        Self::new(LOCK_DATA_STAGE_CURRENT, LOCK_DATA_COMMAND_TYPE_PUSH, 0, value.into())
    }

    pub fn push_with_flags<V: Into<Vec<u8>>>(value: V, flags: u8) -> Self {
        Self::new(LOCK_DATA_STAGE_CURRENT, LOCK_DATA_COMMAND_TYPE_PUSH, flags, value.into())
    }

    pub fn pop(count: u32) -> Self {
        Self::new(
            LOCK_DATA_STAGE_CURRENT,
            LOCK_DATA_COMMAND_TYPE_POP,
            LOCK_DATA_FLAG_VALUE_TYPE_NUMBER,
            count.to_le_bytes().to_vec(),
        )
    }

    pub fn pop_with_flags(count: u32, flags: u8) -> Self {
        Self::new(
            LOCK_DATA_STAGE_CURRENT,
            LOCK_DATA_COMMAND_TYPE_POP,
            flags | LOCK_DATA_FLAG_VALUE_TYPE_NUMBER,
            count.to_le_bytes().to_vec(),
        )
    }

    pub fn stage(&self) -> u8 {
        self.stage
    }

    pub fn command_type(&self) -> u8 {
        self.command_type
    }

    pub fn command_flag(&self) -> u8 {
        self.command_flag
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        let value = match &self.value {
            LockDataValue::Bytes(value) => value.clone(),
            LockDataValue::Pipeline(items) => {
                if items.is_empty() {
                    return Err(SlockError::LockData("pipeline data is empty".to_string()));
                }
                let mut value = Vec::new();
                for item in items {
                    value.extend_from_slice(&item.encode()?);
                }
                value
            }
        };

        let len = value
            .len()
            .checked_add(2)
            .ok_or_else(|| SlockError::LockData("lock data length overflow".to_string()))?;
        let len = u32::try_from(len)
            .map_err(|_| SlockError::LockData("lock data length exceeds u32".to_string()))?;
        let mut data = Vec::with_capacity(value.len() + 6);
        data.extend_from_slice(&len.to_le_bytes());
        data.push(((self.stage << 6) & 0xc0) | (self.command_type & 0x3f));
        data.push(self.command_flag);
        data.extend_from_slice(&value);
        Ok(data)
    }
}

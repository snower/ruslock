use crate::data::LockResultData;
use crate::key::Key16;
use crate::protocol::id::Id16;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InitCommandResult {
    pub request_id: Id16,
    pub result: u8,
    pub init_type: u8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PingCommandResult {
    pub request_id: Id16,
    pub result: u8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LockCommandResult {
    pub command_type: u8,
    pub request_id: Id16,
    pub result: u8,
    pub flag: u8,
    pub db_id: u8,
    pub lock_id: Id16,
    pub lock_key: Key16,
    pub l_count: u16,
    pub count: u16,
    pub lr_count: u8,
    pub r_count: u8,
    pub data: Option<LockResultData>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommandResult {
    Init(InitCommandResult),
    Ping(PingCommandResult),
    Lock(LockCommandResult),
}

impl CommandResult {
    pub fn request_id(&self) -> Id16 {
        match self {
            CommandResult::Init(result) => result.request_id,
            CommandResult::Ping(result) => result.request_id,
            CommandResult::Lock(result) => result.request_id,
        }
    }

    pub fn result_code(&self) -> u8 {
        match self {
            CommandResult::Init(result) => result.result,
            CommandResult::Ping(result) => result.result,
            CommandResult::Lock(result) => result.result,
        }
    }
}

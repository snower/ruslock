pub type Result<T> = std::result::Result<T, SlockError>;

#[derive(Debug, thiserror::Error)]
pub enum SlockError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("client closed")]
    ClientClosed,

    #[error("client not connected")]
    NotConnected,

    #[error("command timeout")]
    CommandTimeout,

    #[error("lock locked")]
    LockLocked(Box<crate::protocol::result::LockCommandResult>),

    #[error("lock unlocked")]
    LockUnlocked(Box<crate::protocol::result::LockCommandResult>),

    #[error("lock not owned")]
    LockNotOwn(Box<crate::protocol::result::LockCommandResult>),

    #[error("lock timeout")]
    LockTimeout(Box<crate::protocol::result::LockCommandResult>),

    #[error("lock expired")]
    LockExpired(Box<crate::protocol::result::LockCommandResult>),

    #[error("server state error")]
    StateError(Box<crate::protocol::result::LockCommandResult>),

    #[error("server error result {result}")]
    Server { result: u8 },

    #[error("lock data error: {0}")]
    LockData(String),

    #[error("event wait timeout")]
    EventWaitTimeout,
}

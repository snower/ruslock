pub mod data;
pub mod error;
pub mod key;
pub mod options;
pub mod primitive;
pub mod protocol;
pub mod time;

#[cfg(feature = "blocking")]
pub mod blocking;

#[cfg(feature = "aio")]
pub mod aio;

pub use crate::data::{LockData, LockResultData};
pub use crate::error::{Result, SlockError};
pub use crate::key::Key16;
pub use crate::options::ClientOptions;
pub use crate::protocol::id::Id16;
pub use crate::time::PackedTime;

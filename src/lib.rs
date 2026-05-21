//! Rust client for `slock`.
//!
//! The crate provides independent blocking, async, and Sans-IO callback facades
//! over shared protocol and data types.
//!
//! ```no_run
//! # #[cfg(feature = "blocking")]
//! fn main() -> ruslock::Result<()> {
//! let client = ruslock::blocking::Client::connect("127.0.0.1:5658")?;
//! let mut lock = client.lock("example", 5, 10);
//! lock.acquire()?;
//! lock.release()?;
//! client.close();
//! Ok(())
//! }
//! # #[cfg(not(feature = "blocking"))]
//! # fn main() {}
//! ```
//!
//! Callback/Sans-IO users own the socket and drive buffers manually:
//!
//! ```no_run
//! fn run() -> ruslock::Result<()> {
//! let client = ruslock::callback::Client::new();
//! let reader = client.reader_buffer();
//! let writer = client.writer_buffer();
//! client.handle_init()?;
//! let _bytes_to_send = writer.drain();
//! # let response = vec![0u8; 64];
//! reader.push(&response);
//! let _ready = client.handle_init()?;
//! Ok(())
//! }
//! ```
//!
//! ```no_run
//! # #[cfg(feature = "aio")]
//! async fn run() -> ruslock::Result<()> {
//! let client = ruslock::aio::Client::connect("127.0.0.1:5658").await?;
//! let mut lock = client.lock("example", 5, 10);
//! lock.acquire().await?;
//! lock.release().await?;
//! client.close().await;
//! Ok(())
//! }
//! ```
//!
//! `LockData` mirrors the Java driver's command-data framing:
//!
//! ```
//! let data = ruslock::LockData::pipeline(vec![
//!     ruslock::LockData::set("aaa"),
//!     ruslock::LockData::append("bbb"),
//! ]);
//! assert!(data.encode().unwrap().len() > 6);
//! ```

pub mod callback;
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

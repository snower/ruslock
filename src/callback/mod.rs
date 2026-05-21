//! Sans-IO callback facade.
//!
//! `callback::Client` owns no socket and starts no reader thread. Callers push
//! received bytes into [`ReaderBuffer`], drain outgoing bytes from
//! [`WriterBuffer`], and drive protocol progress with `handle_init`,
//! `handle_read`, `handle_disconnect`, and `handle_timeout`.

mod buffer;
mod client;
mod database;
mod primitives;

pub use buffer::{ReaderBuffer, WriterBuffer};
pub use client::{Client, RequestHandle};
pub use database::Database;
pub use primitives::{
    Event, GroupEvent, Lock, MaxConcurrentFlow, PriorityLock, ReadWriteLock, ReentrantLock,
    Semaphore, TokenBucketFlow, TreeLeafLock, TreeLock,
};

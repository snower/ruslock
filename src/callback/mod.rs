//! Sans-IO callback facade.
//!
//! `callback::Client` owns no socket and starts no reader thread. Callers push
//! received bytes into [`ReaderBuffer`], drain outgoing bytes from
//! [`WriterBuffer`], and drive protocol progress with `handle_init`,
//! `handle_read`, `handle_disconnect`, and `handle_timeout`.
//!
//! `callback::ReplsetClient` is also Sans-IO. It exposes one child
//! [`Client`] per node through `node_clients()`, and a business
//! [`RequestHandle`] can report the current child transport before the caller
//! drains bytes to its own socket.

mod api;
mod buffer;
mod client;
mod database;
mod handle;
mod primitives;
mod replset;

pub use api::ClientApi;
pub use buffer::{ReaderBuffer, WriterBuffer};
pub use client::{Client, RequestHandle};
pub use database::Database;
pub use handle::{ClientHandle, ReplsetNodeClient, RequestTransport};
pub use primitives::{
    Event, GroupEvent, Lock, MaxConcurrentFlow, PriorityLock, ReadWriteLock, ReentrantLock,
    Semaphore, TokenBucketFlow, TreeLeafLock, TreeLock,
};
pub use replset::{IntoNodeList, ReplsetClient};

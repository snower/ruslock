//! Typed dispatch for the single native operation entry point.
//!
//! Java submits `(handle, op, arg0, encoded_data)` and receives a completion
//! token. Primitives are dispatched here; the Java side never inspects a Rust
//! pointer or reimplements lock state.

use std::future::Future;
use std::sync::Arc;

use ruslock::data::LockData;
use ruslock::time::PackedTime;
use tokio::runtime::Runtime;

use crate::completion::{spawn_result, CompletionCell};
use crate::handle::{
    ClientEntry, Entry, EventEntry, GroupEventEntry, LockEntry, MaxConcurrentEntry, PriorityEntry,
    ReadWriteEntry, ReentrantEntry, SemaphoreEntry, TokenBucketEntry, TreeEntry, TreeLeafEntry,
};
use crate::result::{from_result, ErrorKind, IntoNativeResult, NativeResult};
use crate::runtime;

pub(crate) const OP_LOCK_ACQUIRE: i32 = 1;
pub(crate) const OP_LOCK_RELEASE: i32 = 2;
pub(crate) const OP_LOCK_SHOW: i32 = 3;
pub(crate) const OP_LOCK_UPDATE: i32 = 4;
pub(crate) const OP_LOCK_RELEASE_HEAD: i32 = 5;
pub(crate) const OP_LOCK_RELEASE_HEAD_TO_LOCK_WAIT: i32 = 6;
pub(crate) const OP_LOCK_SET_COUNT: i32 = 7;
pub(crate) const OP_LOCK_SET_R_COUNT: i32 = 8;

pub(crate) const OP_EVENT_IS_SET: i32 = 20;
pub(crate) const OP_EVENT_SET: i32 = 21;
pub(crate) const OP_EVENT_CLEAR: i32 = 22;
pub(crate) const OP_EVENT_WAIT: i32 = 23;
pub(crate) const OP_EVENT_WAIT_RETRY_CLEAR: i32 = 24;

pub(crate) const OP_SEMAPHORE_ACQUIRE: i32 = 30;
pub(crate) const OP_SEMAPHORE_RELEASE: i32 = 31;

pub(crate) const OP_REENTRANT_ACQUIRE: i32 = 32;
pub(crate) const OP_REENTRANT_RELEASE: i32 = 33;

pub(crate) const OP_READ_WRITE_ACQUIRE_WRITE: i32 = 34;
pub(crate) const OP_READ_WRITE_RELEASE_WRITE: i32 = 35;
pub(crate) const OP_READ_WRITE_ACQUIRE_READ: i32 = 36;
pub(crate) const OP_READ_WRITE_RELEASE_READ: i32 = 37;

pub(crate) const OP_GROUP_EVENT_IS_SET: i32 = 40;
pub(crate) const OP_GROUP_EVENT_SET: i32 = 41;
pub(crate) const OP_GROUP_EVENT_CLEAR: i32 = 42;
pub(crate) const OP_GROUP_EVENT_WAKEUP: i32 = 43;
pub(crate) const OP_GROUP_EVENT_WAIT: i32 = 44;
pub(crate) const OP_GROUP_EVENT_WAIT_RETRY_CLEAR: i32 = 45;
pub(crate) const OP_GROUP_EVENT_WAKEUP_WITH_DATA: i32 = 46;

pub(crate) const OP_PRIORITY_ACQUIRE: i32 = 50;
pub(crate) const OP_PRIORITY_RELEASE: i32 = 51;
pub(crate) const OP_PRIORITY_SET_PRIORITY: i32 = 52;

pub(crate) const OP_MAX_CONCURRENT_ACQUIRE: i32 = 53;
pub(crate) const OP_MAX_CONCURRENT_RELEASE: i32 = 54;
pub(crate) const OP_MAX_CONCURRENT_SET_PRIORITY: i32 = 55;

pub(crate) const OP_TOKEN_BUCKET_ACQUIRE: i32 = 56;
pub(crate) const OP_TOKEN_BUCKET_SET_PRIORITY: i32 = 57;

pub(crate) const OP_TREE_ACQUIRE: i32 = 60;
pub(crate) const OP_TREE_RELEASE: i32 = 61;
pub(crate) const OP_TREE_WAIT: i32 = 62;

pub(crate) const OP_TREE_LEAF_ACQUIRE: i32 = 63;
pub(crate) const OP_TREE_LEAF_RELEASE: i32 = 64;

/// Decodes the Java `LockData.dumpData()` frame without reinterpreting its
/// command-specific value bytes.
///
/// The frame is: `u32 value_len + 2`, `u8 stage/type`, `u8 flag`, `value`.
/// `LockData::encode()` produces exactly the same bytes, so keeping the value
/// opaque preserves pipelines and execute frames supplied by Java.
pub(crate) fn decode_lock_data(bytes: &[u8]) -> Result<LockData, String> {
    if bytes.len() < 6 {
        return Err("lock data frame is shorter than its 6-byte header".to_string());
    }
    let value_len = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    if value_len < 2 {
        return Err(format!("lock data frame length {value_len} is too small"));
    }
    let expected = value_len
        .checked_add(4)
        .ok_or_else(|| "lock data frame length overflows".to_string())?;
    if expected != bytes.len() {
        return Err(format!(
            "lock data frame length mismatch: header says {expected} bytes, got {}",
            bytes.len()
        ));
    }

    let stage = (bytes[4] >> 6) & 0x03;
    let command_type = bytes[4] & 0x3f;
    Ok(LockData::new(
        stage,
        command_type,
        bytes[5],
        bytes[6..].to_vec(),
    ))
}

/// Dispatches one operation, completing `cell` exactly once.
///
/// `Err` means the operation was rejected synchronously (unknown op code or an
/// unsupported handle kind) and no completion token should be returned.
pub(crate) fn dispatch(
    entry: Entry,
    op: i32,
    arg0: i32,
    data: Option<LockData>,
    cell: &Arc<CompletionCell>,
) -> Result<(), NativeResult> {
    match entry {
        Entry::Lock(lock) => {
            if !ensure_open(&lock.client, cell) {
                return Ok(());
            }
            dispatch_lock(&lock, op, arg0, data, cell)
        }
        Entry::Event(event) => {
            if !ensure_open(&event.client, cell) {
                return Ok(());
            }
            dispatch_event(&event, op, arg0, data, cell)
        }
        Entry::Semaphore(semaphore) => {
            if !ensure_open(&semaphore.client, cell) {
                return Ok(());
            }
            dispatch_semaphore(&semaphore, op, cell)
        }
        Entry::Reentrant(lock) => {
            if !ensure_open(&lock.client, cell) {
                return Ok(());
            }
            dispatch_reentrant(&lock, op, cell)
        }
        Entry::ReadWrite(lock) => {
            if !ensure_open(&lock.client, cell) {
                return Ok(());
            }
            dispatch_read_write(&lock, op, cell)
        }
        Entry::GroupEvent(event) => {
            if !ensure_open(&event.client, cell) {
                return Ok(());
            }
            dispatch_group_event(&event, op, arg0, data, cell)
        }
        Entry::Priority(lock) => {
            if !ensure_open(&lock.client, cell) {
                return Ok(());
            }
            dispatch_priority(&lock, op, arg0, cell)
        }
        Entry::MaxConcurrent(flow) => {
            if !ensure_open(&flow.client, cell) {
                return Ok(());
            }
            dispatch_max_concurrent(&flow, op, arg0, cell)
        }
        Entry::TokenBucket(flow) => {
            if !ensure_open(&flow.client, cell) {
                return Ok(());
            }
            dispatch_token_bucket(&flow, op, arg0, cell)
        }
        Entry::Tree(tree) => {
            if !ensure_open(&tree.client, cell) {
                return Ok(());
            }
            dispatch_tree(&tree, op, arg0, cell)
        }
        Entry::TreeLeaf(leaf) => {
            if !ensure_open(&leaf.client, cell) {
                return Ok(());
            }
            dispatch_tree_leaf(&leaf, op, cell)
        }
        other => Err(NativeResult::invalid_argument(format!(
            "native handle kind {:?} does not accept operations",
            other.kind()
        ))),
    }
}

fn ensure_open(client: &ClientEntry, cell: &Arc<CompletionCell>) -> bool {
    if client.is_closed() {
        cell.complete(NativeResult::error(
            ErrorKind::ClientClosed,
            "client is closed",
        ));
        false
    } else {
        true
    }
}

fn dispatch_lock(
    lock: &LockEntry,
    op: i32,
    arg0: i32,
    data: Option<LockData>,
    cell: &Arc<CompletionCell>,
) -> Result<(), NativeResult> {
    let runtime = runtime::global().map_err(NativeResult::internal)?;
    match op {
        OP_LOCK_ACQUIRE => {
            spawn(
                &runtime,
                cell,
                lock.lock.acquire_with_flags(arg0 as u8, data),
            );
            Ok(())
        }
        OP_LOCK_RELEASE => {
            spawn(
                &runtime,
                cell,
                lock.lock.release_with_flags(arg0 as u8, data),
            );
            Ok(())
        }
        OP_LOCK_SHOW => {
            spawn(&runtime, cell, lock.lock.show());
            Ok(())
        }
        OP_LOCK_UPDATE => {
            spawn(&runtime, cell, lock.lock.update(data));
            Ok(())
        }
        OP_LOCK_RELEASE_HEAD => {
            spawn(&runtime, cell, lock.lock.release_head(data));
            Ok(())
        }
        OP_LOCK_RELEASE_HEAD_TO_LOCK_WAIT => {
            spawn(&runtime, cell, lock.lock.release_head_to_lock_wait(data));
            Ok(())
        }
        OP_LOCK_SET_COUNT => {
            lock.lock.set_count(arg0 as u16);
            cell.complete(NativeResult::ok_unit());
            Ok(())
        }
        OP_LOCK_SET_R_COUNT => {
            lock.lock.set_r_count(arg0 as u8);
            cell.complete(NativeResult::ok_unit());
            Ok(())
        }
        _ => Err(NativeResult::invalid_argument(format!(
            "unknown lock operation {op}"
        ))),
    }
}

fn dispatch_event(
    event: &EventEntry,
    op: i32,
    arg0: i32,
    data: Option<LockData>,
    cell: &Arc<CompletionCell>,
) -> Result<(), NativeResult> {
    let runtime = runtime::global().map_err(NativeResult::internal)?;
    match op {
        OP_EVENT_IS_SET => {
            spawn(&runtime, cell, event.event.is_set());
            Ok(())
        }
        OP_EVENT_SET => {
            spawn(&runtime, cell, event.event.set_with_data(data));
            Ok(())
        }
        OP_EVENT_CLEAR => {
            spawn(&runtime, cell, event.event.clear_with_data(data));
            Ok(())
        }
        OP_EVENT_WAIT => {
            spawn(
                &runtime,
                cell,
                event.event.wait(PackedTime::from_bits(arg0 as u32)),
            );
            Ok(())
        }
        OP_EVENT_WAIT_RETRY_CLEAR => {
            spawn(
                &runtime,
                cell,
                event
                    .event
                    .wait_and_timeout_retry_clear_packed(PackedTime::from_bits(arg0 as u32)),
            );
            Ok(())
        }
        _ => Err(NativeResult::invalid_argument(format!(
            "unknown event operation {op}"
        ))),
    }
}

fn dispatch_semaphore(
    semaphore: &SemaphoreEntry,
    op: i32,
    cell: &Arc<CompletionCell>,
) -> Result<(), NativeResult> {
    let runtime = runtime::global().map_err(NativeResult::internal)?;
    match op {
        OP_SEMAPHORE_ACQUIRE => {
            spawn(&runtime, cell, semaphore.semaphore.acquire());
            Ok(())
        }
        OP_SEMAPHORE_RELEASE => {
            spawn(&runtime, cell, semaphore.semaphore.release());
            Ok(())
        }
        _ => Err(NativeResult::invalid_argument(format!(
            "unknown semaphore operation {op}"
        ))),
    }
}

fn dispatch_reentrant(
    lock: &ReentrantEntry,
    op: i32,
    cell: &Arc<CompletionCell>,
) -> Result<(), NativeResult> {
    let runtime = runtime::global().map_err(NativeResult::internal)?;
    match op {
        OP_REENTRANT_ACQUIRE => {
            spawn(&runtime, cell, lock.lock.acquire());
            Ok(())
        }
        OP_REENTRANT_RELEASE => {
            spawn(&runtime, cell, lock.lock.release());
            Ok(())
        }
        _ => Err(NativeResult::invalid_argument(format!(
            "unknown reentrant lock operation {op}"
        ))),
    }
}

fn dispatch_read_write(
    lock: &ReadWriteEntry,
    op: i32,
    cell: &Arc<CompletionCell>,
) -> Result<(), NativeResult> {
    let runtime = runtime::global().map_err(NativeResult::internal)?;
    match op {
        OP_READ_WRITE_ACQUIRE_WRITE => {
            spawn(&runtime, cell, lock.lock.acquire_write());
            Ok(())
        }
        OP_READ_WRITE_RELEASE_WRITE => {
            spawn(&runtime, cell, lock.lock.release_write());
            Ok(())
        }
        OP_READ_WRITE_ACQUIRE_READ => {
            spawn(&runtime, cell, lock.lock.acquire_read());
            Ok(())
        }
        OP_READ_WRITE_RELEASE_READ => {
            spawn(&runtime, cell, lock.lock.release_read());
            Ok(())
        }
        _ => Err(NativeResult::invalid_argument(format!(
            "unknown read-write lock operation {op}"
        ))),
    }
}

fn dispatch_group_event(
    event: &GroupEventEntry,
    op: i32,
    arg0: i32,
    data: Option<LockData>,
    cell: &Arc<CompletionCell>,
) -> Result<(), NativeResult> {
    let runtime = runtime::global().map_err(NativeResult::internal)?;
    match op {
        OP_GROUP_EVENT_IS_SET => {
            spawn(&runtime, cell, event.event.is_set());
            Ok(())
        }
        OP_GROUP_EVENT_SET => {
            let future = match data {
                Some(data) => event.event.set_with_data(data),
                None => event.event.set(),
            };
            spawn(&runtime, cell, future);
            Ok(())
        }
        OP_GROUP_EVENT_CLEAR => {
            let future = match data {
                Some(data) => event.event.clear_with_data(data),
                None => event.event.clear(),
            };
            spawn(&runtime, cell, future);
            Ok(())
        }
        OP_GROUP_EVENT_WAKEUP => {
            spawn(&runtime, cell, event.event.wakeup());
            Ok(())
        }
        OP_GROUP_EVENT_WAKEUP_WITH_DATA => {
            spawn(&runtime, cell, event.event.wakeup_with_data(data));
            Ok(())
        }
        OP_GROUP_EVENT_WAIT => {
            spawn(
                &runtime,
                cell,
                event.event.wait_packed(PackedTime::from_bits(arg0 as u32)),
            );
            Ok(())
        }
        OP_GROUP_EVENT_WAIT_RETRY_CLEAR => {
            spawn(
                &runtime,
                cell,
                event
                    .event
                    .wait_and_timeout_retry_clear_packed(PackedTime::from_bits(arg0 as u32)),
            );
            Ok(())
        }
        _ => Err(NativeResult::invalid_argument(format!(
            "unknown group event operation {op}"
        ))),
    }
}

fn dispatch_priority(
    lock: &PriorityEntry,
    op: i32,
    arg0: i32,
    cell: &Arc<CompletionCell>,
) -> Result<(), NativeResult> {
    let runtime = runtime::global().map_err(NativeResult::internal)?;
    match op {
        OP_PRIORITY_ACQUIRE => {
            spawn(&runtime, cell, lock.lock.acquire());
            Ok(())
        }
        OP_PRIORITY_RELEASE => {
            spawn(&runtime, cell, lock.lock.release());
            Ok(())
        }
        OP_PRIORITY_SET_PRIORITY => {
            lock.lock.set_priority(arg0 as u8);
            cell.complete(NativeResult::ok_unit());
            Ok(())
        }
        _ => Err(NativeResult::invalid_argument(format!(
            "unknown priority lock operation {op}"
        ))),
    }
}

fn dispatch_max_concurrent(
    flow: &MaxConcurrentEntry,
    op: i32,
    arg0: i32,
    cell: &Arc<CompletionCell>,
) -> Result<(), NativeResult> {
    let runtime = runtime::global().map_err(NativeResult::internal)?;
    match op {
        OP_MAX_CONCURRENT_ACQUIRE => {
            spawn(&runtime, cell, flow.flow.acquire());
            Ok(())
        }
        OP_MAX_CONCURRENT_RELEASE => {
            spawn(&runtime, cell, flow.flow.release());
            Ok(())
        }
        OP_MAX_CONCURRENT_SET_PRIORITY => {
            flow.flow.set_priority(arg0 as u8);
            cell.complete(NativeResult::ok_unit());
            Ok(())
        }
        _ => Err(NativeResult::invalid_argument(format!(
            "unknown max-concurrent flow operation {op}"
        ))),
    }
}

fn dispatch_token_bucket(
    flow: &TokenBucketEntry,
    op: i32,
    arg0: i32,
    cell: &Arc<CompletionCell>,
) -> Result<(), NativeResult> {
    let runtime = runtime::global().map_err(NativeResult::internal)?;
    match op {
        OP_TOKEN_BUCKET_ACQUIRE => {
            spawn(&runtime, cell, flow.flow.acquire());
            Ok(())
        }
        OP_TOKEN_BUCKET_SET_PRIORITY => {
            flow.flow.set_priority(arg0 as u8);
            cell.complete(NativeResult::ok_unit());
            Ok(())
        }
        _ => Err(NativeResult::invalid_argument(format!(
            "unknown token-bucket flow operation {op}"
        ))),
    }
}

fn dispatch_tree(
    tree: &TreeEntry,
    op: i32,
    arg0: i32,
    cell: &Arc<CompletionCell>,
) -> Result<(), NativeResult> {
    let runtime = runtime::global().map_err(NativeResult::internal)?;
    match op {
        OP_TREE_ACQUIRE => {
            spawn(&runtime, cell, tree.tree.acquire());
            Ok(())
        }
        OP_TREE_RELEASE => {
            spawn(&runtime, cell, tree.tree.release());
            Ok(())
        }
        OP_TREE_WAIT => {
            spawn(&runtime, cell, tree.tree.wait(arg0 as u16));
            Ok(())
        }
        _ => Err(NativeResult::invalid_argument(format!(
            "unknown tree lock operation {op}"
        ))),
    }
}

fn dispatch_tree_leaf(
    leaf: &TreeLeafEntry,
    op: i32,
    cell: &Arc<CompletionCell>,
) -> Result<(), NativeResult> {
    let runtime = runtime::global().map_err(NativeResult::internal)?;
    match op {
        OP_TREE_LEAF_ACQUIRE => {
            spawn(&runtime, cell, leaf.leaf.acquire());
            Ok(())
        }
        OP_TREE_LEAF_RELEASE => {
            spawn(&runtime, cell, leaf.leaf.release());
            Ok(())
        }
        _ => Err(NativeResult::invalid_argument(format!(
            "unknown tree leaf operation {op}"
        ))),
    }
}

fn spawn<F, T>(runtime: &Runtime, cell: &Arc<CompletionCell>, future: F)
where
    F: Future<Output = ruslock::Result<T>> + Send + 'static,
    T: IntoNativeResult + 'static,
{
    spawn_result(runtime, Arc::clone(cell), async move {
        from_result(future.await)
    });
}

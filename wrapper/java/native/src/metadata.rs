//! Compact metadata frame for a live handle.
//!
//! Java needs read-only primitive metadata (ids, packed times, counts, default
//! database flags, mutable priorities and current lock data) without a native
//! call per field. The frame is deliberately versioned and little endian.
//!
//! Base fields are unchanged from the first bridge version. The optional
//! extension block at the end lets Java read the extended primitive state while
//! an older parser can still stop after the fields it understands.
//!
//! ```text
//! u8  version (1)
//! u8  kind
//! u8  flag_a     client closed / event default_set / completion done
//! u8  db_id
//! u32 timeout_bits
//! u32 expired_bits
//! u16 count
//! u8  r_count
//! u8  reserved
//! u16 default_timeout_flags
//! u16 default_expired_flags
//! u8  has_key
//! u8[16] key
//! u8  has_lock_id
//! u8[16] lock_id
//! u8  has_current_data
//! u32 current_data_len
//! u8[current_data_len] raw LockResultData
//! u8  ext_version (1)
//! u8  priority
//! u16 max
//! f64 period
//! u64 client_id
//! u64 version_id
//! u32 read_lock_count
//! u8  has_parent_key
//! u8[16] parent_key if present
//! u8  has_leaf_id
//! u8[16] leaf_id if present
//! ```

use ruslock::key::Key16;
use ruslock::protocol::id::Id16;

use crate::handle::{Entry, Kind};

pub(crate) const METADATA_VERSION: u8 = 1;
pub(crate) const METADATA_EXT_VERSION: u8 = 1;

pub(crate) fn encode(entry: &Entry) -> Vec<u8> {
    let mut out = Vec::with_capacity(160);
    out.push(METADATA_VERSION);
    out.push(kind_id(entry.kind()));

    let base = base_fields(entry);
    out.push(base.flag_a);
    out.push(base.db_id);
    out.extend_from_slice(&base.timeout.to_le_bytes());
    out.extend_from_slice(&base.expired.to_le_bytes());
    out.extend_from_slice(&base.count.to_le_bytes());
    out.push(base.r_count);
    out.push(0);
    out.extend_from_slice(&base.db_timeout.to_le_bytes());
    out.extend_from_slice(&base.db_expired.to_le_bytes());

    push_key(&mut out, base.key);
    push_lock_id(&mut out, base.lock_id);
    match base.current_data.as_deref() {
        Some(data) => {
            out.push(1);
            out.extend_from_slice(&(data.len() as u32).to_le_bytes());
            out.extend_from_slice(data);
        }
        None => {
            out.push(0);
            out.extend_from_slice(&0u32.to_le_bytes());
        }
    }

    out.push(METADATA_EXT_VERSION);
    out.push(base.priority);
    out.extend_from_slice(&base.max.to_le_bytes());
    out.extend_from_slice(&base.period.to_le_bytes());
    out.extend_from_slice(&base.client_id.to_le_bytes());
    out.extend_from_slice(&base.version_id.to_le_bytes());
    out.extend_from_slice(&(base.read_lock_count as u32).to_le_bytes());
    push_key(&mut out, base.parent_key);
    push_lock_id(&mut out, base.leaf_id);

    out
}

struct Base {
    flag_a: u8,
    db_id: u8,
    timeout: u32,
    expired: u32,
    count: u16,
    r_count: u8,
    db_timeout: u16,
    db_expired: u16,
    key: Option<Key16>,
    lock_id: Option<Id16>,
    current_data: Option<Vec<u8>>,
    priority: u8,
    max: u16,
    period: f64,
    client_id: u64,
    version_id: u64,
    read_lock_count: usize,
    parent_key: Option<Key16>,
    leaf_id: Option<Id16>,
}

impl Default for Base {
    fn default() -> Self {
        Self {
            flag_a: 0,
            db_id: 0,
            timeout: 0,
            expired: 0,
            count: 0,
            r_count: 0,
            db_timeout: 0,
            db_expired: 0,
            key: None,
            lock_id: None,
            current_data: None,
            priority: 0,
            max: 0,
            period: 0.0,
            client_id: 0,
            version_id: 0,
            read_lock_count: 0,
            parent_key: None,
            leaf_id: None,
        }
    }
}

fn base_fields(entry: &Entry) -> Base {
    match entry {
        Entry::Runtime(_) => Base::default(),
        Entry::Client(client) => Base {
            flag_a: client.is_closed() as u8,
            ..Base::default()
        },
        Entry::Database(database) => Base {
            db_id: database.database.db_id(),
            timeout: database.database.timeout(0).bits(),
            expired: database.database.expired(0).bits(),
            db_timeout: database.database.timeout(0).flags(),
            db_expired: database.database.expired(0).flags(),
            ..Base::default()
        },
        Entry::Lock(lock) => {
            let current = lock.lock.current_data();
            Base {
                db_id: lock.lock.db_id(),
                timeout: lock.lock.timeout().bits(),
                expired: lock.lock.expired().bits(),
                count: lock.lock.count(),
                r_count: lock.lock.r_count(),
                key: Some(lock.lock.lock_key()),
                lock_id: Some(lock.lock.lock_id()),
                current_data: current.as_ref().map(|data| data.raw().to_vec()),
                ..Base::default()
            }
        }
        Entry::Event(event) => {
            let current = event.event.current_data();
            Base {
                flag_a: event.event.default_set() as u8,
                db_id: event.event.db_id(),
                timeout: event.event.timeout().bits(),
                expired: event.event.expired().bits(),
                key: Some(event.event.lock_key()),
                current_data: current.as_ref().map(|data| data.raw().to_vec()),
                ..Base::default()
            }
        }
        Entry::Completion(completion) => Base {
            flag_a: completion.is_done() as u8,
            ..Base::default()
        },
        Entry::Semaphore(semaphore) => Base {
            db_id: semaphore.semaphore.db_id(),
            timeout: semaphore.semaphore.timeout().bits(),
            expired: semaphore.semaphore.expired().bits(),
            count: semaphore.semaphore.count(),
            key: Some(semaphore.semaphore.lock_key()),
            ..Base::default()
        },
        Entry::Reentrant(lock) => Base {
            db_id: lock.lock.db_id(),
            timeout: lock.lock.timeout().bits(),
            expired: lock.lock.expired().bits(),
            key: Some(lock.lock.lock_key()),
            lock_id: Some(lock.lock.lock_id()),
            ..Base::default()
        },
        Entry::ReadWrite(lock) => Base {
            db_id: lock.lock.db_id(),
            timeout: lock.lock.timeout().bits(),
            expired: lock.lock.expired().bits(),
            key: Some(lock.lock.lock_key()),
            lock_id: Some(lock.lock.write_lock_id()),
            read_lock_count: lock.lock.read_lock_count(),
            ..Base::default()
        },
        Entry::Priority(lock) => Base {
            db_id: lock.lock.db_id(),
            timeout: lock.lock.timeout().bits(),
            expired: lock.lock.expired().bits(),
            count: lock.count,
            key: Some(lock.lock.lock_key()),
            lock_id: Some(lock.lock.lock_id()),
            priority: lock.lock.priority(),
            ..Base::default()
        },
        Entry::MaxConcurrent(flow) => Base {
            db_id: flow.flow.db_id(),
            timeout: flow.flow.timeout().bits(),
            expired: flow.flow.expired().bits(),
            count: flow.flow.max(),
            max: flow.flow.max(),
            key: Some(flow.flow.lock_key()),
            lock_id: Some(flow.flow.lock_id()),
            priority: flow.flow.priority(),
            ..Base::default()
        },
        Entry::TokenBucket(flow) => Base {
            db_id: flow.flow.db_id(),
            timeout: flow.flow.timeout().bits(),
            count: flow.flow.count(),
            key: Some(flow.flow.lock_key()),
            priority: flow.flow.priority(),
            period: flow.flow.period(),
            ..Base::default()
        },
        Entry::GroupEvent(event) => {
            let current = event.event.current_data();
            Base {
                db_id: event.event.db_id(),
                timeout: event.event.timeout().bits(),
                expired: event.event.expired().bits(),
                key: Some(event.event.lock_key()),
                client_id: event.event.client_id(),
                version_id: event.event.version_id(),
                current_data: current.as_ref().map(|data| data.raw().to_vec()),
                ..Base::default()
            }
        }
        Entry::Tree(tree) => Base {
            db_id: tree.tree.db_id(),
            timeout: tree.tree.timeout().bits(),
            expired: tree.tree.expired().bits(),
            key: Some(tree.tree.lock_key()),
            parent_key: tree.tree.parent_key(),
            leaf_id: tree.tree.current_leaf_id(),
            ..Base::default()
        },
        Entry::TreeLeaf(leaf) => Base {
            key: Some(leaf.leaf.lock_key()),
            lock_id: Some(leaf.leaf.lock_id()),
            ..Base::default()
        },
    }
}

fn push_key(out: &mut Vec<u8>, key: Option<Key16>) {
    match key {
        Some(key) => {
            out.push(1);
            out.extend_from_slice(key.as_bytes());
        }
        None => out.push(0),
    }
}

fn push_lock_id(out: &mut Vec<u8>, lock_id: Option<Id16>) {
    match lock_id {
        Some(lock_id) => {
            out.push(1);
            out.extend_from_slice(lock_id.as_bytes());
        }
        None => out.push(0),
    }
}

fn kind_id(kind: Kind) -> u8 {
    kind as u8
}

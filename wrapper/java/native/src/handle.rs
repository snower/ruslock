//! Opaque, generation-checked JNI handle table.
//!
//! Java never receives a Rust pointer. Every object is stored in this table and
//! addressed by a `jlong` made of a 32-bit slot index and a 32-bit generation.
//! Releasing a handle bumps the generation, so a stale token can never resolve
//! to a newer object that reuses the same slot. Lookups also validate the kind,
//! which catches a token passed to the wrong lifecycle or operation entry point.
//!
//! The tests in this module are written before the table implementation. They
//! pin the two externally observable properties the bridge relies on: a token
//! is only valid for the handle kind it was created for, and a released token
//! can never resolve to a later object that reuses the same slot.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use ruslock::aio::{ClientHandle, Database};
use ruslock::binding::{
    BindingEvent, BindingGroupEvent, BindingLock, BindingMaxConcurrentFlow, BindingPriorityLock,
    BindingReadWriteLock, BindingReentrantLock, BindingSemaphore, BindingTokenBucketFlow,
    BindingTreeLeafLock, BindingTreeLock,
};
use tokio::runtime::Runtime;

use crate::completion::CompletionCell;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Kind {
    Runtime = 1,
    Client = 2,
    Database = 3,
    Lock = 4,
    Event = 5,
    Completion = 6,
    Semaphore = 7,
    Reentrant = 8,
    ReadWrite = 9,
    Priority = 10,
    MaxConcurrent = 11,
    TokenBucket = 12,
    GroupEvent = 13,
    Tree = 14,
    TreeLeaf = 15,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RuntimeEntry;

pub(crate) struct ClientEntry {
    pub(crate) client: ClientHandle,
    pub(crate) runtime: Arc<Runtime>,
    pub(crate) closed: AtomicBool,
}

impl ClientEntry {
    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Marks the client closed and returns true only for the first caller.
    pub(crate) fn mark_closed(&self) -> bool {
        !self.closed.swap(true, Ordering::SeqCst)
    }
}

pub(crate) struct DatabaseEntry {
    pub(crate) database: Database,
    pub(crate) client: Arc<ClientEntry>,
}

pub(crate) struct LockEntry {
    pub(crate) lock: BindingLock,
    pub(crate) client: Arc<ClientEntry>,
}

pub(crate) struct EventEntry {
    pub(crate) event: BindingEvent,
    pub(crate) client: Arc<ClientEntry>,
}

pub(crate) struct SemaphoreEntry {
    pub(crate) semaphore: BindingSemaphore,
    pub(crate) client: Arc<ClientEntry>,
}

pub(crate) struct ReentrantEntry {
    pub(crate) lock: BindingReentrantLock,
    pub(crate) client: Arc<ClientEntry>,
}

pub(crate) struct ReadWriteEntry {
    pub(crate) lock: BindingReadWriteLock,
    pub(crate) client: Arc<ClientEntry>,
}

pub(crate) struct PriorityEntry {
    pub(crate) lock: BindingPriorityLock,
    pub(crate) count: u16,
    pub(crate) client: Arc<ClientEntry>,
}

pub(crate) struct MaxConcurrentEntry {
    pub(crate) flow: BindingMaxConcurrentFlow,
    pub(crate) client: Arc<ClientEntry>,
}

pub(crate) struct TokenBucketEntry {
    pub(crate) flow: BindingTokenBucketFlow,
    pub(crate) client: Arc<ClientEntry>,
}

pub(crate) struct GroupEventEntry {
    pub(crate) event: BindingGroupEvent,
    pub(crate) client: Arc<ClientEntry>,
}

pub(crate) struct TreeEntry {
    pub(crate) tree: BindingTreeLock,
    pub(crate) client: Arc<ClientEntry>,
}

pub(crate) struct TreeLeafEntry {
    pub(crate) leaf: BindingTreeLeafLock,
    pub(crate) client: Arc<ClientEntry>,
}

#[derive(Clone)]
pub(crate) enum Entry {
    Runtime(RuntimeEntry),
    Client(Arc<ClientEntry>),
    Database(Arc<DatabaseEntry>),
    Lock(Arc<LockEntry>),
    Event(Arc<EventEntry>),
    Completion(Arc<CompletionCell>),
    Semaphore(Arc<SemaphoreEntry>),
    Reentrant(Arc<ReentrantEntry>),
    ReadWrite(Arc<ReadWriteEntry>),
    Priority(Arc<PriorityEntry>),
    MaxConcurrent(Arc<MaxConcurrentEntry>),
    TokenBucket(Arc<TokenBucketEntry>),
    GroupEvent(Arc<GroupEventEntry>),
    Tree(Arc<TreeEntry>),
    TreeLeaf(Arc<TreeLeafEntry>),
}

impl Entry {
    pub(crate) fn kind(&self) -> Kind {
        match self {
            Self::Runtime(_) => Kind::Runtime,
            Self::Client(_) => Kind::Client,
            Self::Database(_) => Kind::Database,
            Self::Lock(_) => Kind::Lock,
            Self::Event(_) => Kind::Event,
            Self::Completion(_) => Kind::Completion,
            Self::Semaphore(_) => Kind::Semaphore,
            Self::Reentrant(_) => Kind::Reentrant,
            Self::ReadWrite(_) => Kind::ReadWrite,
            Self::Priority(_) => Kind::Priority,
            Self::MaxConcurrent(_) => Kind::MaxConcurrent,
            Self::TokenBucket(_) => Kind::TokenBucket,
            Self::GroupEvent(_) => Kind::GroupEvent,
            Self::Tree(_) => Kind::Tree,
            Self::TreeLeaf(_) => Kind::TreeLeaf,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum HandleError {
    Invalid,
    Stale,
    WrongKind { expected: Kind, actual: Kind },
}

impl HandleError {
    pub(crate) fn message(&self) -> String {
        match self {
            Self::Invalid => "invalid native handle".to_string(),
            Self::Stale => "stale native handle: the object was already released".to_string(),
            Self::WrongKind { expected, actual } => {
                format!("native handle kind mismatch: expected {expected:?}, got {actual:?}")
            }
        }
    }
}

pub(crate) struct HandleTable {
    slots: Vec<Slot>,
    free: Vec<u32>,
}

struct Slot {
    generation: u32,
    entry: Option<Entry>,
}

impl Default for HandleTable {
    fn default() -> Self {
        Self {
            slots: Vec::new(),
            free: Vec::new(),
        }
    }
}

impl HandleTable {
    pub(crate) fn insert(&mut self, entry: Entry) -> i64 {
        if let Some(index) = self.free.pop() {
            let slot = &mut self.slots[index as usize];
            let token = Self::encode(index, slot.generation);
            slot.entry = Some(entry);
            return token;
        }

        let index = self.slots.len() as u32;
        self.slots.push(Slot {
            generation: 1,
            entry: Some(entry),
        });
        Self::encode(index, 1)
    }

    pub(crate) fn get(&self, token: i64, expected: Kind) -> Result<Entry, HandleError> {
        let (index, generation) = Self::decode(token).ok_or(HandleError::Invalid)?;
        let slot = self.slots.get(index).ok_or(HandleError::Invalid)?;
        if slot.generation != generation {
            return Err(HandleError::Stale);
        }
        let entry = slot.entry.as_ref().ok_or(HandleError::Stale)?;
        if entry.kind() != expected {
            return Err(HandleError::WrongKind {
                expected,
                actual: entry.kind(),
            });
        }
        Ok(entry.clone())
    }

    pub(crate) fn get_any(&self, token: i64) -> Result<Entry, HandleError> {
        let (index, generation) = Self::decode(token).ok_or(HandleError::Invalid)?;
        let slot = self.slots.get(index).ok_or(HandleError::Invalid)?;
        if slot.generation != generation {
            return Err(HandleError::Stale);
        }
        slot.entry.clone().ok_or(HandleError::Stale)
    }

    /// Removes a handle. Returning `None` makes double release idempotent.
    pub(crate) fn release(&mut self, token: i64) -> Option<Entry> {
        let (index, generation) = Self::decode(token)?;
        let slot = self.slots.get_mut(index)?;
        if slot.generation != generation {
            return None;
        }
        let entry = slot.entry.take()?;
        slot.generation = slot.generation.wrapping_add(1);
        if slot.generation == 0 {
            slot.generation = 1;
        }
        self.free.push(index as u32);
        Some(entry)
    }

    fn encode(index: u32, generation: u32) -> i64 {
        (((generation as u64) << 32) | index as u64) as i64
    }

    fn decode(token: i64) -> Option<(usize, u32)> {
        if token <= 0 {
            return None;
        }
        let raw = token as u64;
        let index = (raw & 0xffff_ffff) as usize;
        let generation = (raw >> 32) as u32;
        if generation == 0 {
            return None;
        }
        Some((index, generation))
    }
}

static TABLE: OnceLock<Mutex<HandleTable>> = OnceLock::new();

pub(crate) fn table() -> MutexGuard<'static, HandleTable> {
    TABLE
        .get_or_init(|| Mutex::new(HandleTable::default()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ruslock::time::PackedTime;

    fn runtime() -> Arc<Runtime> {
        static RUNTIME: OnceLock<Arc<Runtime>> = OnceLock::new();
        Arc::clone(RUNTIME.get_or_init(|| Arc::new(Runtime::new().unwrap())))
    }

    fn lock() -> Entry {
        let client = ClientHandle::new("127.0.0.1:1");
        let database = client.select_database(0);
        let lock = BindingLock::new(
            database,
            "handle-test",
            PackedTime::new(1),
            PackedTime::new(1),
        );
        let client = Arc::new(ClientEntry {
            client,
            runtime: runtime(),
            closed: AtomicBool::new(false),
        });
        Entry::Lock(Arc::new(LockEntry { lock, client }))
    }

    #[test]
    fn wrong_kind_is_rejected_without_invalidating_the_handle() {
        let mut table = HandleTable::default();
        let token = table.insert(lock());

        assert_eq!(
            table.get(token, Kind::Database).err(),
            Some(HandleError::WrongKind {
                expected: Kind::Database,
                actual: Kind::Lock,
            })
        );
        assert!(matches!(table.get(token, Kind::Lock), Ok(Entry::Lock(_))));
    }

    #[test]
    fn stale_token_is_rejected_after_release() {
        let mut table = HandleTable::default();
        let token = table.insert(lock());
        assert!(matches!(table.release(token), Some(Entry::Lock(_))));

        assert_eq!(table.get(token, Kind::Lock).err(), Some(HandleError::Stale));
        assert!(table.release(token).is_none());
    }

    #[test]
    fn reusing_a_slot_never_revives_an_old_token() {
        let mut table = HandleTable::default();
        let first = table.insert(lock());
        assert!(table.release(first).is_some());
        let second = table.insert(lock());

        assert_ne!(first, second);
        assert_eq!(table.get(first, Kind::Lock).err(), Some(HandleError::Stale));
        assert!(matches!(table.get(second, Kind::Lock), Ok(Entry::Lock(_))));
    }
}

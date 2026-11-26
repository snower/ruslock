//! JNI bridge for the RusLock Java 8 wrapper.
//!
//! Every native method is a thin boundary: it validates typed opaque handles,
//! converts Java values, and hands work to the Rust `binding` / `aio` layer.
//! Operations are submitted once and complete into a [`CompletionCell`]; a Java
//! thread later awaits the completion, so no user callback or Java code runs on
//! a Tokio I/O task.
//!
//! Java-facing signatures live in the package-private
//! `io.github.snower.jaslock.NativeBridge` class. This crate is the only owner
//! of the native symbol names and the handle/result wire formats.

mod completion;
mod handle;
mod jni_util;
mod metadata;
mod operations;
mod result;
mod runtime;
mod signal;

use std::ffi::c_void;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use jni::objects::{JByteArray, JClass, JObjectArray};
use jni::sys::{jboolean, jbyteArray, jdouble, jint, jlong, JNI_VERSION_1_8};
use jni::JNIEnv;
use ruslock::aio::raw::RawCommand;
use ruslock::aio::ClientHandle;
use ruslock::binding::{
    BindingEvent, BindingGroupEvent, BindingLock, BindingMaxConcurrentFlow, BindingPriorityLock,
    BindingReadWriteLock, BindingReentrantLock, BindingSemaphore, BindingTokenBucketFlow,
    BindingTreeLock,
};
use ruslock::key::Key16;
use ruslock::options::ClientOptions;
use ruslock::protocol::id::Id16;
use ruslock::time::PackedTime;

use crate::completion::{spawn_result, CompletionCell};
use crate::handle::{
    ClientEntry, DatabaseEntry, Entry, EventEntry, GroupEventEntry, HandleError, Kind, LockEntry,
    MaxConcurrentEntry, PriorityEntry, ReadWriteEntry, ReentrantEntry, RuntimeEntry,
    SemaphoreEntry, TokenBucketEntry, TreeEntry, TreeLeafEntry,
};
use crate::result::{ErrorKind, NativeResult};

struct JniError {
    class: &'static str,
    message: String,
}

impl JniError {
    fn illegal_state(message: impl Into<String>) -> Self {
        Self {
            class: "java/lang/IllegalStateException",
            message: message.into(),
        }
    }

    fn illegal_argument(message: impl Into<String>) -> Self {
        Self {
            class: "java/lang/IllegalArgumentException",
            message: message.into(),
        }
    }

    fn io(message: impl Into<String>) -> Self {
        Self {
            class: "java/io/IOException",
            message: message.into(),
        }
    }

    fn raw(kind: ErrorKind, message: impl Into<String>) -> Self {
        let transport = matches!(
            kind,
            ErrorKind::ClientClosed
                | ErrorKind::NotConnected
                | ErrorKind::ClientDisconnected
                | ErrorKind::CommandTimeout
                | ErrorKind::Io
                | ErrorKind::Protocol
        );
        Self {
            class: if transport {
                "java/io/IOException"
            } else {
                "java/lang/IllegalStateException"
            },
            message: message.into(),
        }
    }

    fn from_handle(error: HandleError) -> Self {
        match error {
            HandleError::WrongKind { .. } => Self::illegal_argument(error.message()),
            HandleError::Invalid | HandleError::Stale => Self::illegal_state(error.message()),
        }
    }

    fn from_dispatch(error: NativeResult) -> Self {
        let message = error
            .message()
            .unwrap_or("operation rejected by the native bridge")
            .to_string();
        if error.error_kind() == ErrorKind::Internal {
            Self::illegal_state(message)
        } else {
            Self::illegal_argument(message)
        }
    }
}

impl From<String> for JniError {
    fn from(message: String) -> Self {
        Self::illegal_argument(message)
    }
}

fn guard<T>(
    env: &mut JNIEnv,
    default: T,
    body: impl FnOnce(&mut JNIEnv) -> Result<T, JniError>,
) -> T {
    let outcome = catch_unwind(AssertUnwindSafe(|| body(env)));
    match outcome {
        Ok(Ok(value)) => value,
        Ok(Err(error)) => {
            let _ = env.throw_new(error.class, error.message);
            default
        }
        Err(_) => {
            let _ = env.throw_new(
                "java/lang/IllegalStateException",
                "panic in the RusLock JNI bridge",
            );
            default
        }
    }
}

fn entry(token: i64, expected: Kind) -> Result<Entry, JniError> {
    handle::table()
        .get(token, expected)
        .map_err(JniError::from_handle)
}

fn client(token: i64) -> Result<Arc<ClientEntry>, JniError> {
    match entry(token, Kind::Client)? {
        Entry::Client(value) => Ok(value),
        // `entry` already enforced the kind.
        _ => Err(JniError::illegal_state("unreachable client handle kind")),
    }
}

fn database(token: i64) -> Result<Arc<DatabaseEntry>, JniError> {
    match entry(token, Kind::Database)? {
        Entry::Database(value) => Ok(value),
        _ => Err(JniError::illegal_state("unreachable database handle kind")),
    }
}

fn completion(token: i64) -> Result<Arc<CompletionCell>, JniError> {
    match entry(token, Kind::Completion)? {
        Entry::Completion(value) => Ok(value),
        _ => Err(JniError::illegal_state("unreachable completion handle kind")),
    }
}

fn require_open(client: &ClientEntry) -> Result<(), JniError> {
    if client.is_closed() {
        Err(JniError::illegal_state(
            "the native client handle is closed; create a new client",
        ))
    } else {
        Ok(())
    }
}

fn raw_command(
    frame: &[u8],
    timeout_millis: i64,
    expects_response: bool,
) -> Result<RawCommand, JniError> {
    if timeout_millis < 0 {
        return Err(JniError::illegal_argument(
            "raw command timeout must not be negative",
        ));
    }
    RawCommand::from_frame(
        frame,
        Duration::from_millis(timeout_millis as u64),
        expects_response,
    )
    .map_err(|err| JniError::illegal_argument(format!("invalid raw command frame: {err}")))
}

#[no_mangle]
pub extern "system" fn JNI_OnLoad(
    _vm: *mut jni::sys::JavaVM,
    _reserved: *mut c_void,
) -> jint {
    JNI_VERSION_1_8
}

#[no_mangle]
pub extern "system" fn Java_io_github_snower_jaslock_NativeBridge_runtimeCreate<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jlong {
    guard(&mut env, 0, |_env| {
        runtime::global().map_err(JniError::illegal_state)?;
        Ok(handle::table().insert(Entry::Runtime(RuntimeEntry)))
    })
}

#[no_mangle]
pub extern "system" fn Java_io_github_snower_jaslock_NativeBridge_clientCreateNodes<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    runtime_handle: jlong,
    nodes: JObjectArray<'local>,
) -> jlong {
    guard(&mut env, 0, |env| {
        entry(runtime_handle, Kind::Runtime)?;
        let nodes = jni_util::string_array(env, nodes)?;
        if nodes.is_empty() {
            return Err(JniError::illegal_argument(
                "client node list must not be empty",
            ));
        }
        let runtime = runtime::global().map_err(JniError::illegal_state)?;
        // Pin the wrapper's primitive grace to the 120-second Jaslock command
        // window instead of inheriting a future crate default.
        let mut options = ClientOptions::default();
        options.command_timeout_grace = Duration::from_secs(120);
        let client = ClientHandle::with_options(nodes, options).with_conservative_replay();
        Ok(handle::table().insert(Entry::Client(Arc::new(ClientEntry {
            client,
            runtime,
            closed: AtomicBool::new(false),
        }))))
    })
}

#[no_mangle]
pub extern "system" fn Java_io_github_snower_jaslock_NativeBridge_clientOpen<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    client_handle: jlong,
) {
    guard(&mut env, (), |_env| {
        let client = client(client_handle)?;
        require_open(&client)?;
        client
            .runtime
            .block_on(client.client.open())
            .map_err(|err| JniError::io(format!("client open failed: {err}")))
    })
}

#[no_mangle]
pub extern "system" fn Java_io_github_snower_jaslock_NativeBridge_clientTryOpen<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    client_handle: jlong,
) -> jint {
    guard(&mut env, 0, |_env| {
        let client = client(client_handle)?;
        require_open(&client)?;
        let opened = client
            .runtime
            .block_on(client.client.try_open())
            .map_err(|err| JniError::io(format!("client tryOpen failed: {err}")))?;
        Ok(if opened { 1 } else { 0 })
    })
}

#[no_mangle]
pub extern "system" fn Java_io_github_snower_jaslock_NativeBridge_clientClose<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    client_handle: jlong,
) {
    guard(&mut env, (), |_env| {
        let client = client(client_handle)?;
        if client.mark_closed() {
            client.runtime.block_on(client.client.close());
        }
        Ok(())
    })
}

#[no_mangle]
pub extern "system" fn Java_io_github_snower_jaslock_NativeBridge_clientIsClosed<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    client_handle: jlong,
) -> jboolean {
    guard(&mut env, 0, |_env| {
        let client = client(client_handle)?;
        Ok(if client.is_closed() { 1 } else { 0 })
    })
}

#[no_mangle]
pub extern "system" fn Java_io_github_snower_jaslock_NativeBridge_databaseCreate<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    client_handle: jlong,
    db_id: jint,
) -> jlong {
    guard(&mut env, 0, |_env| {
        let client = client(client_handle)?;
        require_open(&client)?;
        let database = client.client.select_database(db_id as u8);
        Ok(handle::table().insert(Entry::Database(Arc::new(DatabaseEntry {
            database,
            client,
        }))))
    })
}

#[no_mangle]
pub extern "system" fn Java_io_github_snower_jaslock_NativeBridge_databaseSetDefaultFlags<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    database_handle: jlong,
    timeout_flags: jint,
    expired_flags: jint,
) {
    guard(&mut env, (), |_env| {
        let database = database(database_handle)?;
        database
            .database
            .set_default_timeout_flags(timeout_flags as u16);
        database
            .database
            .set_default_expired_flags(expired_flags as u16);
        Ok(())
    })
}

#[no_mangle]
pub extern "system" fn Java_io_github_snower_jaslock_NativeBridge_lockCreate<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    database_handle: jlong,
    key: JByteArray<'local>,
    timeout_bits: jint,
    expired_bits: jint,
    lock_id: JByteArray<'local>,
) -> jlong {
    guard(&mut env, 0, |env| {
        let database = database(database_handle)?;
        require_open(&database.client)?;
        let key = jni_util::bytes(env, key)?;
        let timeout = PackedTime::from_bits(timeout_bits as u32);
        let expired = PackedTime::from_bits(expired_bits as u32);
        let lock = match jni_util::optional_bytes(env, lock_id)? {
            None => BindingLock::new(database.database.clone(), key, timeout, expired),
            Some(lock_id) => {
                let lock_id: [u8; 16] = lock_id.try_into().map_err(|_| {
                    JniError::illegal_argument("explicit lock id must be exactly 16 bytes")
                })?;
                BindingLock::with_lock_id(
                    database.database.clone(),
                    key,
                    Id16::from_bytes(lock_id),
                    timeout,
                    expired,
                )
            }
        };
        Ok(handle::table().insert(Entry::Lock(Arc::new(LockEntry {
            lock,
            client: Arc::clone(&database.client),
        }))))
    })
}

#[no_mangle]
pub extern "system" fn Java_io_github_snower_jaslock_NativeBridge_eventCreate<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    database_handle: jlong,
    key: JByteArray<'local>,
    timeout_bits: jint,
    expired_bits: jint,
    default_set: jboolean,
) -> jlong {
    guard(&mut env, 0, |env| {
        let database = database(database_handle)?;
        require_open(&database.client)?;
        let key = jni_util::bytes(env, key)?;
        let event = BindingEvent::new(
            database.database.clone(),
            key,
            PackedTime::from_bits(timeout_bits as u32),
            PackedTime::from_bits(expired_bits as u32),
            default_set != 0,
        );
        Ok(handle::table().insert(Entry::Event(Arc::new(EventEntry {
            event,
            client: Arc::clone(&database.client),
        }))))
    })
}

#[no_mangle]
pub extern "system" fn Java_io_github_snower_jaslock_NativeBridge_semaphoreCreate<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    database_handle: jlong,
    key: JByteArray<'local>,
    count: jint,
    timeout_bits: jint,
    expired_bits: jint,
) -> jlong {
    guard(&mut env, 0, |env| {
        let database = database(database_handle)?;
        require_open(&database.client)?;
        let key = jni_util::bytes(env, key)?;
        let semaphore = BindingSemaphore::new(
            database.database.clone(),
            key,
            count as u16,
            PackedTime::from_bits(timeout_bits as u32),
            PackedTime::from_bits(expired_bits as u32),
        );
        Ok(handle::table().insert(Entry::Semaphore(Arc::new(SemaphoreEntry {
            semaphore,
            client: Arc::clone(&database.client),
        }))))
    })
}

#[no_mangle]
pub extern "system" fn Java_io_github_snower_jaslock_NativeBridge_reentrantLockCreate<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    database_handle: jlong,
    key: JByteArray<'local>,
    timeout_bits: jint,
    expired_bits: jint,
) -> jlong {
    guard(&mut env, 0, |env| {
        let database = database(database_handle)?;
        require_open(&database.client)?;
        let key = jni_util::bytes(env, key)?;
        let lock = BindingReentrantLock::new(
            database.database.clone(),
            key,
            PackedTime::from_bits(timeout_bits as u32),
            PackedTime::from_bits(expired_bits as u32),
        );
        Ok(handle::table().insert(Entry::Reentrant(Arc::new(ReentrantEntry {
            lock,
            client: Arc::clone(&database.client),
        }))))
    })
}

#[no_mangle]
pub extern "system" fn Java_io_github_snower_jaslock_NativeBridge_readWriteLockCreate<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    database_handle: jlong,
    key: JByteArray<'local>,
    timeout_bits: jint,
    expired_bits: jint,
) -> jlong {
    guard(&mut env, 0, |env| {
        let database = database(database_handle)?;
        require_open(&database.client)?;
        let key = jni_util::bytes(env, key)?;
        let lock = BindingReadWriteLock::new(
            database.database.clone(),
            key,
            PackedTime::from_bits(timeout_bits as u32),
            PackedTime::from_bits(expired_bits as u32),
        );
        Ok(handle::table().insert(Entry::ReadWrite(Arc::new(ReadWriteEntry {
            lock,
            client: Arc::clone(&database.client),
        }))))
    })
}

#[no_mangle]
pub extern "system" fn Java_io_github_snower_jaslock_NativeBridge_priorityLockCreate<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    database_handle: jlong,
    key: JByteArray<'local>,
    priority: jint,
    count: jint,
    timeout_bits: jint,
    expired_bits: jint,
) -> jlong {
    guard(&mut env, 0, |env| {
        let database = database(database_handle)?;
        require_open(&database.client)?;
        let key = jni_util::bytes(env, key)?;
        let lock = BindingPriorityLock::new(
            database.database.clone(),
            key,
            priority as u8,
            count as u16,
            PackedTime::from_bits(timeout_bits as u32),
            PackedTime::from_bits(expired_bits as u32),
        );
        Ok(handle::table().insert(Entry::Priority(Arc::new(PriorityEntry {
            lock,
            count: count as u16,
            client: Arc::clone(&database.client),
        }))))
    })
}

#[no_mangle]
pub extern "system" fn Java_io_github_snower_jaslock_NativeBridge_maxConcurrentFlowCreate<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    database_handle: jlong,
    key: JByteArray<'local>,
    count: jint,
    timeout_bits: jint,
    expired_bits: jint,
    priority: jint,
) -> jlong {
    guard(&mut env, 0, |env| {
        let database = database(database_handle)?;
        require_open(&database.client)?;
        let key = jni_util::bytes(env, key)?;
        let flow = BindingMaxConcurrentFlow::new(
            database.database.clone(),
            key,
            count as u16,
            priority as u8,
            PackedTime::from_bits(timeout_bits as u32),
            PackedTime::from_bits(expired_bits as u32),
        );
        Ok(handle::table().insert(Entry::MaxConcurrent(Arc::new(MaxConcurrentEntry {
            flow,
            client: Arc::clone(&database.client),
        }))))
    })
}

#[no_mangle]
pub extern "system" fn Java_io_github_snower_jaslock_NativeBridge_tokenBucketFlowCreate<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    database_handle: jlong,
    key: JByteArray<'local>,
    count: jint,
    timeout_bits: jint,
    period: jdouble,
    priority: jint,
) -> jlong {
    guard(&mut env, 0, |env| {
        let database = database(database_handle)?;
        require_open(&database.client)?;
        let key = jni_util::bytes(env, key)?;
        let flow = BindingTokenBucketFlow::new(
            database.database.clone(),
            key,
            count as u16,
            PackedTime::from_bits(timeout_bits as u32),
            period,
            priority as u8,
        );
        Ok(handle::table().insert(Entry::TokenBucket(Arc::new(TokenBucketEntry {
            flow,
            client: Arc::clone(&database.client),
        }))))
    })
}

#[no_mangle]
pub extern "system" fn Java_io_github_snower_jaslock_NativeBridge_groupEventCreate<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    database_handle: jlong,
    key: JByteArray<'local>,
    client_id: jlong,
    version_id: jlong,
    timeout_bits: jint,
    expired_bits: jint,
) -> jlong {
    guard(&mut env, 0, |env| {
        let database = database(database_handle)?;
        require_open(&database.client)?;
        let key = jni_util::bytes(env, key)?;
        let event = BindingGroupEvent::new(
            database.database.clone(),
            key,
            client_id as u64,
            version_id as u64,
            PackedTime::from_bits(timeout_bits as u32),
            PackedTime::from_bits(expired_bits as u32),
        );
        Ok(handle::table().insert(Entry::GroupEvent(Arc::new(GroupEventEntry {
            event,
            client: Arc::clone(&database.client),
        }))))
    })
}

#[no_mangle]
pub extern "system" fn Java_io_github_snower_jaslock_NativeBridge_groupEventVersionId<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    group_event: jlong,
) -> jlong {
    guard(&mut env, 0, |_env| {
        let entry = handle::table()
            .get(group_event, Kind::GroupEvent)
            .map_err(JniError::from_handle)?;
        let Entry::GroupEvent(event) = entry else {
            return Err(JniError::illegal_argument("group event handle changed kind"));
        };
        Ok(event.event.version_id() as i64)
    })
}

#[no_mangle]
pub extern "system" fn Java_io_github_snower_jaslock_NativeBridge_treeLockCreate<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    database_handle: jlong,
    parent_key: JByteArray<'local>,
    key: JByteArray<'local>,
    timeout_bits: jint,
    expired_bits: jint,
) -> jlong {
    guard(&mut env, 0, |env| {
        let database = database(database_handle)?;
        require_open(&database.client)?;
        let parent = match jni_util::optional_bytes(env, parent_key)? {
            Some(raw) if !raw.is_empty() => Some(exact_key16(raw, "parent key")?),
            _ => None,
        };
        let key = jni_util::bytes(env, key)?;
        let tree = BindingTreeLock::with_parent(
            database.database.clone(),
            parent,
            key,
            PackedTime::from_bits(timeout_bits as u32),
            PackedTime::from_bits(expired_bits as u32),
        );
        Ok(handle::table().insert(Entry::Tree(Arc::new(TreeEntry {
            tree,
            client: Arc::clone(&database.client),
        }))))
    })
}

#[no_mangle]
pub extern "system" fn Java_io_github_snower_jaslock_NativeBridge_treeLeafCreate<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    tree_handle: jlong,
    lock_id: JByteArray<'local>,
) -> jlong {
    guard(&mut env, 0, |env| {
        let entry = handle::table()
            .get(tree_handle, Kind::Tree)
            .map_err(JniError::from_handle)?;
        let Entry::Tree(tree) = entry else {
            return Err(JniError::illegal_argument("tree handle changed kind"));
        };
        let leaf = match jni_util::optional_bytes(env, lock_id)? {
            Some(raw) if !raw.is_empty() => tree.tree.load_leaf_lock(exact_id16(raw, "leaf id")?),
            _ => tree.tree.new_leaf_lock(),
        };
        Ok(handle::table().insert(Entry::TreeLeaf(Arc::new(TreeLeafEntry {
            leaf,
            client: Arc::clone(&tree.client),
        }))))
    })
}

#[no_mangle]
pub extern "system" fn Java_io_github_snower_jaslock_NativeBridge_submitNative<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    target: jlong,
    op: jint,
    arg0: jint,
    data: JByteArray<'local>,
) -> jlong {
    guard(&mut env, 0, |env| {
        let entry = handle::table()
            .get_any(target)
            .map_err(JniError::from_handle)?;
        let data = match jni_util::optional_bytes(env, data)? {
            Some(raw) => Some(operations::decode_lock_data(&raw).map_err(JniError::illegal_argument)?),
            None => None,
        };

        let cell = CompletionCell::new();
        operations::dispatch(entry, op, arg0, data, &cell).map_err(JniError::from_dispatch)?;
        Ok(handle::table().insert(Entry::Completion(cell)))
    })
}

/// Submits a low-level `sendCommand` frame. The returned completion resolves to
/// the exact response frame (64-byte header plus any extra bytes).
///
/// `timeout_millis` is the caller's total wait budget, already including the
/// Jaslock command window. `RawCommand::with_exact_timeout` makes the core
/// transport use that duration without adding its configurable grace. The core
/// deadline covers only the response wait; the frame write is never cancelled
/// from Java/native, so a caller timeout cannot abandon a partially written
/// frame.
///
/// The task runs through [`spawn_result`], so a panic writes an `Internal`
/// completion and signals the Java completion pump instead of leaving the
/// waiter blocked forever.
#[no_mangle]
pub extern "system" fn Java_io_github_snower_jaslock_NativeBridge_sendRawSubmitNative<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    client_handle: jlong,
    frame: JByteArray<'local>,
    timeout_millis: jlong,
) -> jlong {
    guard(&mut env, 0, |env| {
        let client = client(client_handle)?;
        require_open(&client)?;
        if timeout_millis < 0 {
            return Err(JniError::illegal_argument(
                "raw command timeout must not be negative",
            ));
        }
        let frame = jni_util::bytes(env, frame)?;
        let command = raw_command(&frame, timeout_millis, true)?.with_exact_timeout();
        let cell = CompletionCell::new();
        let runtime = Arc::clone(&client.runtime);
        spawn_result(&runtime, Arc::clone(&cell), async move {
            match client.client.send_raw(command).await {
                Ok(bytes) => NativeResult::ok_bytes(bytes),
                Err(err) => result::from_error(err),
            }
        });
        Ok(handle::table().insert(Entry::Completion(cell)))
    })
}

/// Submits a low-level `writeCommand` frame. The returned completion resolves to
/// an empty success payload once the frame has been written and flushed.
#[no_mangle]
pub extern "system" fn Java_io_github_snower_jaslock_NativeBridge_writeRawSubmitNative<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    client_handle: jlong,
    frame: JByteArray<'local>,
) -> jlong {
    guard(&mut env, 0, |env| {
        let client = client(client_handle)?;
        require_open(&client)?;
        let frame = jni_util::bytes(env, frame)?;
        let command = raw_command(&frame, 0, false)?;
        let cell = CompletionCell::new();
        let runtime = Arc::clone(&client.runtime);
        spawn_result(&runtime, Arc::clone(&cell), async move {
            match client.client.write_raw(command).await {
                Ok(()) => NativeResult::ok_unit(),
                Err(err) => result::from_error(err),
            }
        });
        Ok(handle::table().insert(Entry::Completion(cell)))
    })
}

/// Waits for a raw submit completion on the calling Java thread.
///
/// Success returns the response frame bytes; a write-only completion returns an
/// empty array. Transport failures throw {@link java.io.IOException}; other
/// bridge failures throw {@link java.lang.IllegalStateException}. The completion
/// token is not consumed and must be released by the caller.
#[no_mangle]
pub extern "system" fn Java_io_github_snower_jaslock_NativeBridge_sendRawAwait<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    completion_handle: jlong,
) -> jbyteArray {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let completion = completion(completion_handle)?;
        let result = completion.wait();
        if result.is_ok() {
            let bytes = result.bytes().unwrap_or(&[]);
            jni_util::to_byte_array(env, bytes).map_err(JniError::illegal_state)
        } else {
            Err(JniError::raw(
                result.error_kind(),
                result
                    .message()
                    .unwrap_or("raw command failed without a message")
                    .to_string(),
            ))
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_io_github_snower_jaslock_NativeBridge_completionIsDone<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    completion_handle: jlong,
) -> jboolean {
    guard(&mut env, 0, |_env| {
        let completion = completion(completion_handle)?;
        Ok(if completion.is_done() { 1 } else { 0 })
    })
}

#[no_mangle]
pub extern "system" fn Java_io_github_snower_jaslock_NativeBridge_completionAwait<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    completion_handle: jlong,
) -> jbyteArray {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let completion = completion(completion_handle)?;
        let encoded = completion.wait().encode();
        jni_util::to_byte_array(env, &encoded).map_err(JniError::illegal_state)
    })
}

#[no_mangle]
pub extern "system" fn Java_io_github_snower_jaslock_NativeBridge_completionEpoch<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jlong {
    guard(&mut env, 0, |_env| Ok(signal::epoch() as i64))
}

#[no_mangle]
pub extern "system" fn Java_io_github_snower_jaslock_NativeBridge_awaitCompletionChange<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    observed: jlong,
    timeout_millis: jlong,
) -> jlong {
    guard(&mut env, observed, |_env| {
        let timeout = if timeout_millis < 0 {
            None
        } else {
            Some(Duration::from_millis(timeout_millis as u64))
        };
        Ok(signal::await_change(observed as u64, timeout) as i64)
    })
}

#[no_mangle]
pub extern "system" fn Java_io_github_snower_jaslock_NativeBridge_signalCompletionPump<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
) {
    let _ = guard(&mut env, (), |_env| {
        signal::bump();
        Ok(())
    });
}

/// Replaces the packed timeout/expired bits stored on a live primitive handle.
///
/// Bits are assigned verbatim by the Rust binding; database defaults are not
/// re-merged. Only timing changes: acquire counts, ids, read queues, version and
/// lazy state are preserved. All eleven timing-bearing binding kinds are
/// supported; any other handle kind is rejected as an illegal argument.
#[no_mangle]
pub extern "system" fn Java_io_github_snower_jaslock_NativeBridge_primitiveSetTiming<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    timeout_bits: jint,
    expired_bits: jint,
) {
    let _ = guard(&mut env, (), |_env| {
        let timeout = PackedTime::from_bits(timeout_bits as u32);
        let expired = PackedTime::from_bits(expired_bits as u32);
        let entry = handle::table()
            .get_any(handle)
            .map_err(JniError::from_handle)?;
        match entry {
            Entry::Lock(value) => value.lock.set_timing(timeout, expired),
            Entry::Event(value) => value.event.set_timing(timeout, expired),
            Entry::Semaphore(value) => value.semaphore.set_timing(timeout, expired),
            Entry::Reentrant(value) => value.lock.set_timing(timeout, expired),
            Entry::ReadWrite(value) => value.lock.set_timing(timeout, expired),
            Entry::Priority(value) => value.lock.set_timing(timeout, expired),
            Entry::MaxConcurrent(value) => value.flow.set_timing(timeout, expired),
            Entry::TokenBucket(value) => value.flow.set_timing(timeout, expired),
            Entry::GroupEvent(value) => value.event.set_timing(timeout, expired),
            Entry::Tree(value) => value.tree.set_timing(timeout, expired),
            Entry::TreeLeaf(value) => value.leaf.set_timing(timeout, expired),
            other => {
                return Err(JniError::illegal_argument(format!(
                    "native handle kind {:?} does not support timing updates",
                    other.kind()
                )))
            }
        }
        Ok(())
    });
}

/// Replaces the stored wire count on a counting primitive.
///
/// `count` is the Java facade's already-encoded unsigned short, passed through
/// verbatim with no offset. Only semaphore and token-bucket handles carry a
/// mutable wire count; timing, period and priority are preserved and the
/// primitive is never recreated. Values outside `0..=65535` are rejected before
/// any handle lookup.
#[no_mangle]
pub extern "system" fn Java_io_github_snower_jaslock_NativeBridge_primitiveSetCount<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    count: jint,
) {
    let _ = guard(&mut env, (), |_env| {
        let count = u16::try_from(count).map_err(|_| {
            JniError::illegal_argument(format!("primitive count must be in 0..=65535, got {count}"))
        })?;
        let entry = handle::table()
            .get_any(handle)
            .map_err(JniError::from_handle)?;
        match entry {
            Entry::Semaphore(value) => value.semaphore.set_count(count),
            Entry::TokenBucket(value) => value.flow.set_count(count),
            other => {
                return Err(JniError::illegal_argument(format!(
                    "native handle kind {:?} does not support wire count updates",
                    other.kind()
                )))
            }
        }
        Ok(())
    });
}

#[no_mangle]
pub extern "system" fn Java_io_github_snower_jaslock_NativeBridge_metadataNative<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let entry = handle::table()
            .get_any(handle)
            .map_err(JniError::from_handle)?;
        let encoded = metadata::encode(&entry);
        jni_util::to_byte_array(env, &encoded).map_err(JniError::illegal_state)
    })
}

#[no_mangle]
pub extern "system" fn Java_io_github_snower_jaslock_NativeBridge_release<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jboolean {
    guard(&mut env, 0, |_env| {
        let released = handle::table().release(handle);
        if let Some(Entry::Client(client)) = released.as_ref() {
            if client.mark_closed() {
                client.runtime.block_on(client.client.close());
            }
        }
        Ok(if released.is_some() { 1 } else { 0 })
    })
}

fn exact_key16(raw: Vec<u8>, what: &str) -> Result<Key16, JniError> {
    let bytes: [u8; 16] = raw
        .try_into()
        .map_err(|_| JniError::illegal_argument(format!("{what} must be exactly 16 bytes")))?;
    Ok(Key16::from_bytes(bytes))
}

fn exact_id16(raw: Vec<u8>, what: &str) -> Result<Id16, JniError> {
    let bytes: [u8; 16] = raw
        .try_into()
        .map_err(|_| JniError::illegal_argument(format!("{what} must be exactly 16 bytes")))?;
    Ok(Id16::from_bytes(bytes))
}

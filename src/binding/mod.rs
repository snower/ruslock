//! Language-neutral handoff layer for JNI and other bindings.
//!
//! This module exposes strongly typed Rust primitive objects whose operations
//! return owned, `Send + 'static` completions. It deliberately has no
//! dependency on `JNIEnv`, Java types or a Java executor, so the same entry
//! points can later back a C or other language binding.
//!
//! Operations snapshot the shared primitive state, perform the existing Rust
//! `aio` primitive call, and then merge the result back under a short-lived
//! state mutex. No state lock is held across an `.await`, which lets a `set()`
//! on the same `BindingEvent` proceed while another task is waiting on it.
//!
//! Opaque handle storage for JNI (typed, generation-checked `jlong` tokens)
//! belongs to the JNI/native wrapper crate, not here. This module exposes the
//! objects that such a handle table would own.

mod extended;
mod primitives;

use std::sync::{Arc, Mutex};

use crate::time::PackedTime;

/// Mutable packed timeout/expired bits shared by cheap binding clones.
///
/// Mutating timing never re-merges database defaults, never changes the lock
/// identity/count/queue/version, and only affects the next submitted request.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct TimingState {
    pub(crate) timeout: PackedTime,
    pub(crate) expired: PackedTime,
}

pub(crate) fn shared_timing(timeout: PackedTime, expired: PackedTime) -> Arc<Mutex<TimingState>> {
    Arc::new(Mutex::new(TimingState { timeout, expired }))
}

pub(crate) fn read_timing(timing: &Mutex<TimingState>) -> TimingState {
    *timing
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub(crate) fn write_timing(
    timing: &Mutex<TimingState>,
    timeout: PackedTime,
    expired: PackedTime,
) {
    *timing
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = TimingState { timeout, expired };
}

pub use extended::{
    BindingGroupEvent, BindingMaxConcurrentFlow, BindingPriorityLock, BindingReadWriteLock,
    BindingReentrantLock, BindingSemaphore, BindingTokenBucketFlow, BindingTreeLeafLock,
    BindingTreeLock,
};
pub use primitives::{BindingCompletion, BindingEvent, BindingLock};

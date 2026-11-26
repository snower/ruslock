//! Process-wide Tokio runtime used by every client handle.
//!
//! JNI calls always enter from JVM threads, so `Runtime::block_on` for open and
//! close is safe, while submissions are spawned and awaited by the Java facade.
//! The runtime is intentionally process-wide: completing an operation after the
//! Java client handle is released must not race runtime shutdown.

use std::sync::{Arc, OnceLock};

use tokio::runtime::Runtime;

static RUNTIME: OnceLock<Arc<Runtime>> = OnceLock::new();

pub(crate) fn global() -> Result<Arc<Runtime>, String> {
    if let Some(runtime) = RUNTIME.get() {
        return Ok(Arc::clone(runtime));
    }

    let runtime = Arc::new(
        Runtime::new().map_err(|err| format!("failed to create the RusLock Tokio runtime: {err}"))?,
    );
    match RUNTIME.set(Arc::clone(&runtime)) {
        Ok(()) => Ok(runtime),
        // Another JNI call won the initialization race; both callers must use
        // the installed runtime so tasks and handles share one executor.
        Err(_) => Ok(Arc::clone(RUNTIME.get().expect("runtime was already installed"))),
    }
}

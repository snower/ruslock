//! One-shot completion cells and panic containment for native operations.

use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll};

use tokio::runtime::Runtime;

use crate::result::NativeResult;

/// A completion written by a Rust task and consumed by a Java thread.
///
/// The cell keeps the result even after `wait()` returns, so the Java side can
/// inspect it and only then release the opaque completion handle. Releasing the
/// handle while a Java thread is blocked is safe because the waiter owns an
/// `Arc` clone of the cell.
pub(crate) struct CompletionCell {
    result: Mutex<Option<NativeResult>>,
    ready: Condvar,
    done: AtomicBool,
}

impl CompletionCell {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            result: Mutex::new(None),
            ready: Condvar::new(),
            done: AtomicBool::new(false),
        })
    }

    pub(crate) fn complete(&self, result: NativeResult) {
        let mut slot = self.result.lock().unwrap_or_else(|e| e.into_inner());
        if slot.is_some() {
            return;
        }
        *slot = Some(result);
        self.done.store(true, Ordering::SeqCst);
        self.ready.notify_all();
        // The result is visible before the generation changes, so a Java pump
        // that reads the new epoch then scans is guaranteed to see `done`.
        crate::signal::bump();
    }

    pub(crate) fn is_done(&self) -> bool {
        self.done.load(Ordering::SeqCst)
    }

    pub(crate) fn wait(&self) -> NativeResult {
        let mut slot = self.result.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            if let Some(result) = slot.as_ref() {
                return result.clone();
            }
            slot = self.ready.wait(slot).unwrap_or_else(|e| e.into_inner());
        }
    }
}

/// Polls an inner future with a panic boundary.
///
/// A panic inside a primitive operation therefore becomes an `Internal`
/// completion instead of unwinding through the Tokio runtime and leaving the
/// Java waiter blocked forever.
pub(crate) struct CatchUnwind<F> {
    inner: F,
}

impl<F> CatchUnwind<F> {
    pub(crate) fn new(inner: F) -> Self {
        Self { inner }
    }
}

impl<F: Future> Future for CatchUnwind<F> {
    type Output = std::thread::Result<F::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: `inner` is structurally pinned: `CatchUnwind` never moves it
        // after construction, and the projection hands out a pinned reference.
        let inner = unsafe { self.map_unchecked_mut(|this| &mut this.inner) };
        match catch_unwind(AssertUnwindSafe(|| inner.poll(cx))) {
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(payload) => Poll::Ready(Err(payload)),
        }
    }
}

/// Runs one result-producing future to completion under a panic boundary.
///
/// Every async native task must use this helper (or an equivalent
/// `CatchUnwind`) so a panic always writes an `Internal` result. Without it the
/// completion stays pending forever and a Java waiter has no recovery path.
pub(crate) fn spawn_result<F>(runtime: &Runtime, cell: Arc<CompletionCell>, future: F)
where
    F: Future<Output = NativeResult> + Send + 'static,
{
    runtime.spawn(async move {
        let result = match CatchUnwind::new(future).await {
            Ok(result) => result,
            Err(_) => NativeResult::internal("native operation panicked"),
        };
        cell.complete(result);
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::result::ErrorKind;
    use std::sync::mpsc;
    use std::time::Duration;

    #[test]
    fn panicking_future_completes_internal_and_signals_epoch() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("test runtime");
        let cell = CompletionCell::new();
        let observed = crate::signal::epoch();

        spawn_result(&runtime, Arc::clone(&cell), async {
            panic!("simulated raw send panic");
            #[allow(unreachable_code)]
            NativeResult::ok_unit()
        });

        let (sender, receiver) = mpsc::channel();
        let waiter = Arc::clone(&cell);
        std::thread::spawn(move || {
            let _ = sender.send(waiter.wait());
        });
        let result = receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("panic completion did not resolve");

        assert!(!result.is_ok());
        assert_eq!(result.error_kind(), ErrorKind::Internal);
        assert_ne!(crate::signal::epoch(), observed);
    }
}

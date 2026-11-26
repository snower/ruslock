//! Event-driven completion notification for the Java pump thread.
//!
//! The Java asynchronous path needs to know when *any* native completion has
//! become visible without polling every handle. This module provides one
//! process-wide generation counter and a `Condvar`. A native result bumps the
//! generation after its result is visible; the pump reads the generation before
//! scanning and then waits only if no change happened during the scan.
//!
//! There is deliberately no per-completion queue here: queue ownership and
//! token lifetime stay on the Java side, so a released completion cannot leak a
//! native record and no user callback ever runs on a Rust I/O task.

use std::sync::{Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

/// One monotonic-ish generation counter guarded by its wait condition.
#[derive(Default)]
pub(crate) struct CompletionSignal {
    state: Mutex<u64>,
    changed: Condvar,
}

impl CompletionSignal {
    pub(crate) fn epoch(&self) -> u64 {
        *self.state.lock().unwrap_or_else(|error| error.into_inner())
    }

    /// Makes all current and future `await_change` calls observe a new epoch.
    pub(crate) fn bump(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        *state = state.wrapping_add(1);
        self.changed.notify_all();
    }

    /// Returns the current epoch, waiting while it still equals `observed`.
    ///
    /// `None` waits indefinitely; `Some(Duration::ZERO)` is a pure read. The
    /// timeout is bounded by a single deadline and spurious wakeups re-check it.
    pub(crate) fn await_change(&self, observed: u64, timeout: Option<Duration>) -> u64 {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if *state != observed {
            return *state;
        }

        match timeout {
            None => loop {
                state = self
                    .changed
                    .wait(state)
                    .unwrap_or_else(|error| error.into_inner());
                if *state != observed {
                    return *state;
                }
            },
            Some(duration) => {
                let deadline = Instant::now() + duration;
                loop {
                    if *state != observed {
                        return *state;
                    }
                    let now = Instant::now();
                    if now >= deadline {
                        return *state;
                    }
                    let remaining = deadline.saturating_duration_since(now);
                    let (next, _) = self
                        .changed
                        .wait_timeout(state, remaining)
                        .unwrap_or_else(|error| error.into_inner());
                    state = next;
                }
            }
        }
    }
}

static GLOBAL: OnceLock<CompletionSignal> = OnceLock::new();

fn global() -> &'static CompletionSignal {
    GLOBAL.get_or_init(CompletionSignal::default)
}

pub(crate) fn epoch() -> u64 {
    global().epoch()
}

pub(crate) fn bump() {
    global().bump();
}

pub(crate) fn await_change(observed: u64, timeout: Option<Duration>) -> u64 {
    global().await_change(observed, timeout)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn bump_wakes_a_waiter_and_advances_the_epoch() {
        let signal = Arc::new(CompletionSignal::default());
        let observed = signal.epoch();
        let waiter = {
            let signal = Arc::clone(&signal);
            thread::spawn(move || signal.await_change(observed, Some(Duration::from_secs(2))))
        };

        thread::sleep(Duration::from_millis(10));
        signal.bump();

        let after = waiter.join().expect("waiter panicked");
        assert_ne!(after, observed);
        assert_eq!(after, signal.epoch());
    }

    #[test]
    fn bounded_wait_returns_the_same_epoch_on_timeout() {
        let signal = CompletionSignal::default();
        let observed = signal.epoch();
        let start = Instant::now();

        let after = signal.await_change(observed, Some(Duration::from_millis(20)));

        assert_eq!(after, observed);
        assert!(start.elapsed() >= Duration::from_millis(10));
    }

    #[test]
    fn an_already_changed_epoch_returns_without_waiting() {
        let signal = CompletionSignal::default();
        let observed = signal.epoch();
        signal.bump();
        let current = signal.epoch();

        assert_eq!(
            signal.await_change(observed, Some(Duration::from_secs(2))),
            current
        );
    }
}

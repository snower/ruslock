# ruslock Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` for parallelizable phases or `superpowers:executing-plans` for inline execution. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Rust `slock` client library that provides `ruslock::blocking` synchronous APIs, `ruslock::aio` async/await APIs, and `ruslock::callback` Sans-IO callback APIs, with behavior verified against the Java `jaslock` implementation.

**Architecture:** Reuse one shared protocol/data/primitive-logic core, then implement separate blocking, async, and callback transport facades. Blocking uses `std::net::TcpStream` plus a reader supervisor thread; async uses tokio reader supervisor tasks; callback is Sans-IO and owns only reader/writer buffers plus pending callback state. Blocking and async own TCP connections, while callback is driven entirely by the caller's scheduler.

**Tech Stack:** Rust, Cargo, tokio for `aio`, bitflags, md-5, rand, optional socket2 for `blocking`, thiserror, Sans-IO buffers for `callback`, local `slock` service for integration/parity tests.

---

## Summary

The repository currently contains design documents and the implemented Rust crate. This plan tracks the completed protocol/blocking/async/replset/parity work and adds the callback/Sans-IO extension described in `design.md`.

```text
D:\workspace\github\jaslock\src\test\java\io\github\snower\jaslock\ClientTest.java
```

The implementation must preserve the protocol details documented in `docs/Architecture.md` and `design.md`: 64-byte command frames, little-endian numeric fields, LockData framing, requestId response matching, key normalization, timeout/expired flags, single-connection auto-reconnect behavior, replset retry behavior, and callback/Sans-IO reader/writer buffer semantics.

New requirement added on 2026-05-19: `Client` and `ReplsetClient` must implement the same public abstraction inside each calling model, so business code can switch between single IP and multi-IP deployment by changing construction/configuration only. After construction, usage must be identical through `ClientApi`/`ClientHandle`, shared `Database`, and shared primitive facade types.

New requirement added on 2026-05-20: `Client::open()` must start the reader only after the first TCP connect + Init succeeds. After that, the reader thread/task owns disconnect detection and automatic reconnect, reuses the same `clientId`, refreshes `init_type`, and keeps retrying by `ClientOptions::reconnect_interval` until `close()` or `auto_reconnect=false`. Single-node clients do not silently replay failed in-flight business commands; replset retry remains handled by the replset layer.

New requirement added on 2026-05-20: replset support must not be exposed as a separate Cargo feature. `blocking::ReplsetClient` is compiled with `blocking`, and `aio::ReplsetClient` is compiled with `aio`; default features are only `["blocking", "aio"]`.

New requirement added on 2026-05-21: add `ruslock::callback` as a Sans-IO callback facade. It must not create TCP connections, hold sockets, start reader threads, or depend on tokio/socket2 when built with `default-features=false`. It exposes split `ReaderBuffer` and `WriterBuffer`; `handle_init`, `handle_read`, `handle_disconnect`, `handle_timeout`, and request cancellation drive protocol state; lock/event/semaphore and other primitive operations write commands to `writer_buffer` and return results only through callbacks. It must use `ClientDisconnected` for unexpected disconnects, preserve Java extra-data `payload_len + 4` layout, reuse the same `clientId` across reconnect init, and compute pending deadlines from `encoded.timeout + command_timeout_grace`.

## Execution Status

Last updated: 2026-05-21.

Completed:

- [x] Task 0 crate scaffold is implemented and `cargo check --all-features` passes.
- [x] Task 1 shared protocol foundation is implemented: `SlockError`, `ClientOptions`, `PackedTime`, `Key16`, `Id16`, and Java `ICommand` constants.
- [x] Task 2 command encode/decode is implemented for Init, Ping, Lock, Unlock, response headers, and Lock extra-data framing.
- [x] Task 3 LockData core is implemented for set, unset, incr, append, shift, execute, pipeline, push, pop, and LockResultData accessors.
- [x] Task 3 LockData tests now explicitly cover all command variants and Java property-offset parsing.
- [x] Task 4 blocking single client is implemented with TCP connect/init, reader supervisor thread, automatic reconnect, pending request matching, timeout cleanup, close wakeup, ping, database selection, and root lock factory.
- [x] Task 5 blocking Lock API is implemented with acquire/release/show/update/release_head/release_head_to_lock_wait/current_data and lock result error mapping.
- [x] Task 6 async single client is implemented with tokio TCP connect/init, reader supervisor task, automatic reconnect, pending request matching, timeout cleanup, close wakeup, ping, database selection, and root lock factory.
- [x] Task 6 async cancellation cleanup is implemented and covered by tokio mock transport tests.
- [x] Task 7 async explicit `LockGuard` API is implemented; release is explicit and awaited.
- [x] Task 7 async lock error mapping tests now mirror the blocking mappings for locked, unlocked, unown, and timeout results.
- [x] Task 8 database factories are implemented for all current blocking and async primitives, including full `u8` db ids and default flag merge.
- [x] Task 8A unified client abstraction is implemented for blocking and async: `ClientApi`, `ClientHandle`, unified `Database` backend dispatch, and shared primitive facade return types for `Client`/`ReplsetClient`.
- [x] Task 9 blocking primitive API surface is implemented for Event, GroupEvent, Semaphore, ReentrantLock, ReadWriteLock, PriorityLock, MaxConcurrentFlow, TokenBucketFlow, and TreeLock.
- [x] Task 10 async primitive API surface is implemented for the same primitive set.
- [x] Task 9 and Task 10 now have mock command-construction coverage for every primitive in both blocking and async APIs.
- [x] Task 9 and Task 10 now have mock state-transition coverage for every primitive in both blocking and async APIs, including flags/count/r_count, Event wait data, GroupEvent version updates, and TreeLock leaf release.
- [x] TreeLock now exposes Java-parity `wait` and `lock_key` helpers for blocking and async APIs.
- [x] TreeLock now exposes Java-parity `new_child`, `load_child`, `new_leaf_lock`, `load_leaf_lock`, `TreeLeafLock::acquire/release`, and leaf `lock_id` helpers for blocking and async APIs.
- [x] Task 11 replset now maintains per-node clients, falls back to the first live node, prefers nodes whose Init response marks `INIT_TYPE_FLAG_IS_LEADER`, and has blocking/async mock coverage for those paths.
- [x] Task 11 replset lock send path now has blocking/async mock coverage for write-failure retry and lock `STATE_ERROR` retry.
- [x] Task 11 replset pending-wakeup behavior is now covered for blocking and async when all nodes are initially down and one node appears before the command deadline.
- [x] Task 11 replset now tracks explicit `PendingCommand` state and `RetryType` transitions for origin, redirected, pending, and woken command retries.
- [x] Task 12 Java parity test files exist and run against local `slock` when available; benchmark parity is ignored by default unless explicitly requested.
- [x] Task 12 Java parity tests now include stronger assertion chains for Event, GroupEvent, ReadWriteLock, Replset lock data, LockData set/incr/append/shift/push/pop, TreeLock wait/child, MaxConcurrentFlow, and TokenBucketFlow.
- [x] Task 12 Java parity now covers LockData execute/pipeline live side effects, TreeLock recursive leaf/child helper behavior, MaxConcurrentFlow millisecond expiry, and the 1000-task async PriorityLock callback stress.
- [x] Task 13 README/rustdoc quickstarts, feature flags, LockData examples, and local slock test notes are documented.
- [x] Task 13 README now documents `ClientHandle` runtime selection for single-node versus replset deployments.
- [x] Task 13 Architecture/design docs now document Java-compatible single-connection auto-reconnect after reader failure.
- [x] Task 13 feature cleanup removes the standalone `replset` Cargo feature and makes replset tests run under `blocking`/`aio`.
- [x] Task 13 design doc now includes callback/Sans-IO API design, buffer semantics, callback pending behavior, disconnect/timeout handling, and callback test requirements.
- [x] Task 14 callback Sans-IO buffer and client state machine is implemented with split buffers, Init/read/timeout/disconnect/cancel handling, callback-only build support, and mock tests.
- [x] Task 15 callback primitive facade is implemented for Lock, Event, GroupEvent, Semaphore, ReentrantLock, ReadWriteLock, PriorityLock, MaxConcurrentFlow, TokenBucketFlow, TreeLock, and TreeLeafLock.
- [x] Task 16 callback docs, examples, and final verification updates are implemented.

Remaining:

- [x] Plan commit steps are represented by one final implementation commit rather than rewritten as historical per-task commits.
- [x] No callback implementation tasks remain open; create the final implementation commit when requested.

Latest completed verification:

- [x] `cargo check --all-features`
- [x] `cargo fmt --check`
- [x] `cargo clippy --all-features --all-targets -- -D warnings`
- [x] `cargo test --lib --all-features`
- [x] `cargo test --features blocking --no-default-features`
- [x] `cargo test --features aio --no-default-features`
- [x] `cargo test --all-features`
- [x] `cargo test --all-features --test client_api`
- [x] `cargo test --doc --all-features`
- [x] `git diff --check`
- [x] `cargo test --test protocol_foundation --all-features`
- [x] `cargo test --test aio_mock --features aio --no-default-features`
- [x] `cargo test --test primitive_commands --all-features`
- [x] `cargo test --test primitive_commands --features blocking --no-default-features`
- [x] `cargo test --test primitive_commands --features aio --no-default-features`
- [x] `cargo test --all-features --test primitive_state_mock`
- [x] `cargo test --test replset_mock --all-features`
- [x] `cargo test --no-default-features --features blocking --test replset_mock`
- [x] `cargo test --no-default-features --features aio --test replset_mock`
- [x] `cargo test --no-default-features --features blocking --test java_parity_replset`
- [x] `cargo test --no-default-features --features aio --test java_parity_replset`
- [x] `SLOCK_TEST_HOST=127.0.0.1 SLOCK_TEST_PORT=5658 cargo test --all-features --test java_parity_blocking --test java_parity_async --test java_parity_replset --test java_parity_lock_data --test java_parity_flow_tree`
- [x] `cargo test --all-features --test java_parity_benchmark -- --ignored`
- [x] `cargo check --no-default-features`
- [x] `cargo test --no-default-features --test callback_buffer --test callback_client --test callback_primitives --test callback_state_mock`
- [x] `cargo test --all-features --test callback_buffer --test callback_client --test callback_primitives --test callback_state_mock`

## Public API Targets

- `ruslock::blocking::ClientApi`
- `ruslock::blocking::ClientHandle`
- `ruslock::blocking::Client`
- `ruslock::blocking::ReplsetClient`
- `ruslock::blocking::Database`
- `ruslock::blocking::{Lock, Event, GroupEvent, Semaphore, ReentrantLock, ReadWriteLock, PriorityLock, MaxConcurrentFlow, TokenBucketFlow, TreeLock}`
- `ruslock::aio::ClientApi`
- `ruslock::aio::ClientHandle`
- `ruslock::aio::Client`
- `ruslock::aio::ReplsetClient`
- `ruslock::aio::Database`
- `ruslock::aio::{Lock, Event, GroupEvent, Semaphore, ReentrantLock, ReadWriteLock, PriorityLock, MaxConcurrentFlow, TokenBucketFlow, TreeLock}`
- `ruslock::callback::Client`
- `ruslock::callback::Database`
- `ruslock::callback::{Lock, Event, GroupEvent, Semaphore, ReentrantLock, ReadWriteLock, PriorityLock, MaxConcurrentFlow, TokenBucketFlow, TreeLock}`
- `ruslock::callback::{ReaderBuffer, WriterBuffer, RequestHandle}`
- Shared data/error/protocol types: `SlockError`, `Result<T>`, `ClientOptions`, `PackedTime`, `Id16`, `Key16`, `LockData`, `LockResultData`, command/result structs.

## Task 0: Crate Scaffold

**Files:**
- Create: `Cargo.toml`
- Create: `src/lib.rs`
- Create: `src/error.rs`
- Create: `src/options.rs`
- Create: `src/time.rs`
- Create: `src/key.rs`
- Create directories: `src/protocol/`, `src/data/`, `src/primitive/`, `src/blocking/`, `src/aio/`

- [x] Create `Cargo.toml` with package metadata and features:
  - `default = ["blocking", "aio"]`
  - `blocking = []`
  - `aio = ["dep:tokio"]`
  - dependencies: `bitflags`, `md-5`, `rand`, `socket2`, `thiserror`, optional `tokio` with `net`, `sync`, `time`, `rt`, `macros`, `io-util`.
- [x] Create module skeletons and public exports in `src/lib.rs`.
- [x] Add empty module files so `cargo check --all-features` reaches dependency resolution.
- [x] Run `cargo check --all-features`.
- [x] Commit scaffold if the check succeeds. Superseded by final implementation commit.

## Task 1: Shared Protocol Foundation

**Files:**
- Create/modify: `src/error.rs`
- Create/modify: `src/options.rs`
- Create/modify: `src/time.rs`
- Create/modify: `src/key.rs`
- Create: `src/protocol/constants.rs`
- Create: `src/protocol/id.rs`
- Test: module tests in these files

- [x] Implement `SlockError` and `pub type Result<T> = std::result::Result<T, SlockError>`.
- [x] Implement `ClientOptions` defaults matching Java behavior: 5s connect timeout, 2s reconnect interval, 120s command timeout grace, auto reconnect enabled, TCP nodelay and keepalive enabled.
- [x] Implement `PackedTime` with low 16-bit value and high 16-bit flags.
- [x] Implement `Key16` normalization:
  - input length > 16: MD5 digest.
  - input length <= 16: left-pad zeroes and right-align bytes.
- [x] Implement `Id16` generation for requestId, lockId, and clientId:
  - 6 timestamp bytes.
  - 6 random bytes.
  - 4 counter bytes.
- [x] Port every Java `ICommand` constant into `protocol::constants`.
- [x] Add unit tests for key normalization, packed time, ID length, and repeated ID uniqueness.
- [x] Run `cargo test --lib --all-features`.
- [x] Commit shared foundation. Superseded by final implementation commit.

## Task 2: Command Encode/Decode

**Files:**
- Create: `src/protocol/command.rs`
- Create: `src/protocol/result.rs`
- Create: `src/protocol/codec.rs`
- Modify: `src/protocol/mod.rs`
- Test: protocol module tests

- [x] Implement command structs: `InitCommand`, `PingCommand`, `LockCommand`, and `Command`.
- [x] Implement `EncodedCommand` containing request id, command type, 64-byte header, optional extra bytes, timeout, and response expectation.
- [x] Encode Init, Ping, Lock, and Unlock headers using exact offsets from `docs/Architecture.md`.
- [x] Implement result structs: `InitCommandResult`, `PingCommandResult`, `LockCommandResult`, and `CommandResult`.
- [x] Decode 64-byte response headers and validate magic/version.
- [x] Decode Lock result extra data framing: 4-byte length followed by payload.
- [x] Add tests:
  - Init/Ping/Lock encoded header length is 64.
  - LockCommand field offsets match Java layout.
  - timeout/expired/count are little-endian.
  - invalid magic/version returns `SlockError::Protocol`.
- [x] Run `cargo test --lib --all-features`.
- [x] Commit command codec. Superseded by final implementation commit.

## Task 3: LockData

**Files:**
- Create: `src/data/mod.rs`
- Create: `src/data/lock_data.rs`
- Create: `src/data/lock_result_data.rs`
- Modify: `src/lib.rs`
- Test: data module tests

- [x] Implement `LockData` constructors for set, unset, incr, append, shift, execute, pipeline, push, and pop.
- [x] Encode LockData as length + stage/type + flags + value.
- [x] Encode pipeline by concatenating nested LockData bytes and recalculating total length.
- [x] Implement `LockResultData` accessors for bytes, string, i64, list, string list, map, and string map.
- [x] Preserve Java property offset behavior when `CONTAINS_PROPERTY` is present.
- [x] Add tests for every LockData variant and every LockResultData accessor.
- [x] Run `cargo test --lib --all-features`.
- [x] Commit LockData support. Superseded by final implementation commit.

## Task 4: Blocking Single Client

**Files:**
- Create: `src/blocking/mod.rs`
- Create: `src/blocking/client.rs`
- Create: `src/blocking/connection.rs`
- Create: `src/blocking/database.rs`
- Modify: `src/lib.rs`
- Test: blocking mock transport tests

- [x] Implement `blocking::Client` with `new`, `with_options`, `connect`, `open`, `close`, `ping`, `select_database`, and root `lock`.
- [x] Implement `blocking::Connection` using `std::net::TcpStream`, a writer mutex, pending request map, and one reader supervisor thread.
- [x] Ensure `open()` connects, sets TCP options via `socket2`, sends Init, reads Init result, and only then starts the reader supervisor thread.
- [x] Ensure `send_command()` inserts pending before writing, writes header + optional extra, flushes, waits using Java-compatible timeout, and removes pending on timeout.
- [x] Ensure the reader supervisor reconnects after disconnect/read failure, reuses the same clientId, refreshes init_type, and stops retrying after `close()`.
- [x] Ensure `close()` stops reconnect, closes socket, and wakes pending waiters with `ClientClosed`.
- [x] Implement `blocking::Database` with db id and default flag storage.
- [x] Add mock-server tests for Init-first behavior, ping requestId matching, timeout cleanup, close cleanup, and reconnect-until-close behavior.
- [x] Run `cargo test --features blocking --no-default-features`.
- [x] Commit blocking transport. Superseded by final implementation commit.

## Task 5: Blocking Lock API

**Files:**
- Create: `src/primitive/state.rs`
- Create: `src/primitive/lock_logic.rs`
- Create: `src/blocking/primitives.rs`
- Modify: `src/blocking/mod.rs`
- Modify: `src/blocking/database.rs`
- Test: lock logic tests and blocking tests

- [x] Implement lock state containing db id, lock key, lock id, timeout, expired, count, r_count, and current data in the public lock facade.
- [x] Implement command builders for acquire, release, show, update, release_head, and release_head_to_lock_wait.
- [x] Implement `blocking::Lock` methods:
  - `acquire`
  - `acquire_with_data`
  - `release`
  - `release_with_data`
  - `show`
  - `update`
  - `release_head`
  - `release_head_to_lock_wait`
  - `current_data`
- [x] Map lock result codes to exact `SlockError` variants.
- [x] Add tests for successful current data update and lock error mapping.
- [x] Add local slock smoke test for acquire/release, gated so it skips when no slock service is available.
- [x] Run `cargo test --features blocking --no-default-features`.
- [x] Commit blocking lock API. Superseded by final implementation commit.

## Task 6: Async Single Client

**Files:**
- Create: `src/aio/mod.rs`
- Create: `src/aio/client.rs`
- Create: `src/aio/connection.rs`
- Create: `src/aio/database.rs`
- Modify: `src/lib.rs`
- Test: tokio mock transport tests

- [x] Implement `aio::Client` with `new`, `with_options`, `connect`, `open`, `close`, `ping`, `select_database`, and root `lock`.
- [x] Implement tokio connection state that owns the writer, pending map, reader supervisor task, reconnect handling, and close signal.
- [x] Connection task must handle command ops, decoded frames, timeout cleanup, reader failure, reconnect, and close.
- [x] Ensure async reconnect reuses the same clientId, refreshes init_type, wakes current pending commands on reader failure, and stops retrying after `close().await`.
- [x] `Client::close().await` must stop reconnect and wake all pending operations.
- [x] Add tokio mock-server tests for Init/Ping, requestId response matching, timeout cleanup, cancellation cleanup, close cleanup, and reconnect-until-close behavior.
- [x] Run `cargo test --features aio --no-default-features`.
- [x] Commit async transport. Superseded by final implementation commit.

## Task 7: Async Lock API

**Files:**
- Create/modify: `src/aio/primitives.rs`
- Modify: `src/aio/database.rs`
- Test: async lock tests

- [x] Implement `aio::Lock` with the same method names as blocking, using `async fn` and `.await`.
- [x] Add explicit async guard API whose release is explicit and awaited.
- [x] Do not rely on async Drop for lock release.
- [x] Mirror Task 5 tests for async success and error mapping.
- [x] Run `cargo test --features aio --no-default-features`.
- [x] Commit async lock API. Superseded by final implementation commit.

## Task 8: Database Factories

**Files:**
- Modify: `src/blocking/database.rs`
- Modify: `src/aio/database.rs`
- Modify: `src/blocking/client.rs`
- Modify: `src/aio/client.rs`
- Test: database tests

- [x] Implement factory methods for all primitives in blocking and async databases.
- [x] Implement default timeout flag and expired flag setters.
- [x] Merge default flags into new primitive `PackedTime` values.
- [x] Use `u8` db id and support full 0..255 selection.
- [x] Add tests for db 0, db 255, default flag merge, and root client factory delegation to db 0.
- [x] Run `cargo test --all-features`.
- [x] Commit database factories. Superseded by final implementation commit.

## Task 8A: Unified Client Abstraction

**Files:**
- Create: `src/blocking/api.rs`
- Create: `src/blocking/handle.rs`
- Create: `src/aio/api.rs`
- Create: `src/aio/handle.rs`
- Modify: `src/blocking/client.rs`
- Modify: `src/blocking/replset.rs`
- Modify: `src/blocking/database.rs`
- Modify: `src/blocking/primitives.rs`
- Modify: `src/aio/client.rs`
- Modify: `src/aio/replset.rs`
- Modify: `src/aio/database.rs`
- Modify: `src/aio/primitives.rs`
- Modify: `src/blocking/mod.rs`
- Modify: `src/aio/mod.rs`
- Test: API interchangeability tests

- [x] Define `blocking::ClientApi` implemented by `blocking::Client`, `blocking::ReplsetClient`, and `blocking::ClientHandle`.
- [x] Define `aio::ClientApi` implemented by `aio::Client`, `aio::ReplsetClient`, and `aio::ClientHandle`, using boxed futures so the public trait does not require `async-trait`.
- [x] Implement `ClientHandle` enum/facade for blocking and async; it must choose single-node or replset backend from node-count/configuration at construction time.
- [x] Ensure `ClientApi::select_database` returns the same public `Database` type for `Client`, `ReplsetClient`, and `ClientHandle`.
- [x] Ensure root primitive factories (`lock`, `event`, `group_event`, `semaphore`, `reentrant_lock`, `read_write_lock`, `priority_lock`, `max_concurrent_flow`, `token_bucket_flow`, `tree_lock`) return the same public primitive facade types regardless of backend.
- [x] Move replset-specific send/retry behavior behind a shared command-sender abstraction so business primitives do not need `ReplsetLock`/`ReplsetEvent` public types.
- [x] Add compile-time tests that the same generic function over `blocking::ClientApi` works with `Client`, `ReplsetClient`, and `ClientHandle`.
- [x] Add async compile-time/runtime tests that the same function over `aio::ClientApi` works with `Client`, `ReplsetClient`, and `ClientHandle`.
- [x] Add configuration/factory tests proving a single address creates a single-node backend and multiple addresses create a replset backend while usage code is unchanged.
- [x] Run `cargo test --all-features`.
- [x] Commit unified client abstraction. Superseded by final implementation commit.

## Task 9: Remaining Blocking Primitives

**Files:**
- Modify: `src/blocking/primitives.rs`
- Test: `tests/primitive_commands.rs`, `tests/primitive_state_mock.rs`, and blocking Java parity tests
- Note: shared extraction files were not needed yet; logic remains close to the public facade while tests pin the protocol surface.

- [x] Implement blocking Event.
- [x] Implement blocking GroupEvent.
- [x] Implement blocking Semaphore.
- [x] Implement blocking ReentrantLock.
- [x] Implement blocking ReadWriteLock.
- [x] Implement blocking PriorityLock.
- [x] Implement blocking MaxConcurrentFlow.
- [x] Implement blocking TokenBucketFlow.
- [x] Implement blocking TreeLock.
- [x] Add command construction tests for each blocking primitive.
- [x] Add state transition tests for each blocking primitive.
- [x] Add local slock smoke tests for each primitive, skipped when service is unavailable.
- [x] Run `cargo test --features blocking --no-default-features`.
- [x] Commit blocking primitives. Superseded by final implementation commit.

## Task 10: Remaining Async Primitives

**Files:**
- Modify: `src/aio/primitives.rs`
- Test: `tests/primitive_commands.rs`, `tests/primitive_state_mock.rs`, and async Java parity tests

- [x] Implement async Event.
- [x] Implement async GroupEvent.
- [x] Implement async Semaphore.
- [x] Implement async ReentrantLock.
- [x] Implement async ReadWriteLock.
- [x] Implement async PriorityLock.
- [x] Implement async MaxConcurrentFlow.
- [x] Implement async TokenBucketFlow.
- [x] Implement async TreeLock.
- [x] Keep names and behavior aligned with blocking APIs.
- [x] Add async command construction tests for every primitive.
- [x] Add async smoke tests for every primitive.
- [x] Run `cargo test --features aio --no-default-features`.
- [x] Commit async primitives. Superseded by final implementation commit.

## Task 11: Replset

**Files:**
- Create: `src/blocking/replset.rs`
- Create: `src/aio/replset.rs`
- Modify: `src/blocking/mod.rs`
- Modify: `src/aio/mod.rs`
- Test: replset tests

- [x] Implement node parsing from comma strings and string slices.
- [x] Track per-node clients and the active leader/live node index.
- [x] Track shared requests, pending commands, and retry type.
- [x] Prefer leader, fallback to first live node.
- [x] Retry on write failure and lock `STATE_ERROR`.
- [x] Wake pending commands when leader/live node appears before the original command deadline.
- [x] Implement blocking and async `ReplsetClient` factory methods matching single client APIs.
- [x] Add tests for node parsing, single-node replset behavior, live-node fallback, and leader selection.
- [x] Add tests for write-failure retry and state-error retry.
- [x] Add tests for pending wakeup.
- [x] Run `cargo test --all-features`.
- [x] Commit replset support. Superseded by final implementation commit.

## Task 12: Java ClientTest Parity Suite

**Files:**
- Create: `tests/java_parity_blocking.rs`
- Create: `tests/java_parity_async.rs`
- Create: `tests/java_parity_replset.rs`
- Create: `tests/java_parity_lock_data.rs`
- Create: `tests/java_parity_flow_tree.rs`
- Create: `tests/java_parity_benchmark.rs`

- [x] Add helpers for test endpoint defaults:
  - `SLOCK_TEST_HOST=127.0.0.1`
  - `SLOCK_TEST_PORT=5658`
  - optional `SLOCK_REPLSET_NODES`
- [x] Migrate `testClientLock`.
- [x] Migrate `testReplsetClientLock`.
- [x] Migrate `testClientAsyncLock`.
- [x] Migrate `testReplsetClientAsyncLock`.
- [x] Migrate `testEventDefaultSeted`.
- [x] Migrate `testEventDefaultUnseted`.
- [x] Migrate `testEventAsyncDefaultSeted`.
- [x] Migrate `testEventAsyncDefaultUnseted`.
- [x] Migrate `testGroupEvent`.
- [x] Migrate `testGroupEventAsync`.
- [x] Migrate `testReadWriteLock`.
- [x] Migrate `testReadWriteLockAsync`.
- [x] Migrate `testReentrantLock`.
- [x] Migrate `testReentrantLockAsync`.
- [x] Migrate `testSemaphore`.
- [x] Migrate `testSemaphoreAsync`.
- [x] Migrate `testTreeLock` root wait/child coverage.
- [x] Migrate `testTreeLock` full recursive leaf helper behavior.
- [x] Migrate `testMaxConcurrentFlow`.
- [x] Migrate `testMaxConcurrentFlowAsync`.
- [x] Migrate `testTokenBucketFlow`.
- [x] Migrate `testTokenBucketFlowAsync`.
- [x] Migrate `testPriorityLock`.
- [x] Migrate `testLockData`.
- [x] Migrate `testBenchmark` as ignored by default.
- [x] Preserve Java assertion values exactly where current public API supports them: `"aaa"`, `"bbb"`, `"ccc"`, version `2/3`, semaphore count `10`, priority value, LockData list sizes and content.
- [x] Run parity tests against local slock service.
- [x] Commit parity suite. Superseded by final implementation commit.

## Task 13: Docs and Public API Cleanup

**Files:**
- Modify: `README.md`
- Modify: `docs/Architecture.md`
- Modify: `design.md`
- Modify: `plan.md`
- Modify rustdoc comments in public modules

- [x] Add blocking quickstart.
- [x] Add async quickstart.
- [x] Add ReplsetClient quickstart.
- [x] Add ClientApi/ClientHandle quickstart showing single-node/replset runtime switching without business-code changes.
- [x] Add LockData usage examples.
- [x] Document feature flags.
- [x] Document local slock requirement for integration/parity tests.
- [x] Document why blocking transport does not wrap async runtime.
- [x] Document `Client::open()` reader startup and auto-reconnect lifecycle against the Java implementation.
- [x] Document that replset is included with `blocking`/`aio` and is not a standalone feature.
- [x] Run `cargo test --doc --all-features`.
- [x] Commit docs cleanup. Superseded by final implementation commit.

## Task 14: Callback Sans-IO Buffer and Client Core

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/error.rs`
- Modify: `src/options.rs`
- Create: `src/callback/mod.rs`
- Create: `src/callback/buffer.rs`
- Create: `src/callback/client.rs`
- Create: `src/callback/database.rs`
- Modify: `src/lib.rs`
- Test: `tests/callback_buffer.rs`
- Test: `tests/callback_client.rs`

- [x] Make `socket2` optional and attach it only to `blocking = ["dep:socket2"]`; verify callback-only builds do not pull tokio or socket2.
- [x] Add `SlockError::ClientDisconnected` and `ClientOptions::max_frame_size` with a default of 16 MiB.
- [x] Add unconditional `pub mod callback` export in `src/lib.rs`; callback must not be gated behind `blocking`, `aio`, or a new feature.
- [x] Create internal `SharedBuffer` plus public `ReaderBuffer` and `WriterBuffer` handles.
- [x] Expose only `ReaderBuffer::{len,is_empty,push,clear}` and `WriterBuffer::{len,is_empty,drain,drain_into,clear}`.
- [x] Add buffer tests proving reader cannot be drained through the public API, writer cannot be pushed through the public API, `push`/`drain` preserve FIFO order, `drain_into` appends into caller storage, and clone handles observe the same buffer.
- [x] Run `cargo test --no-default-features --test callback_buffer`; expected red before buffer types exist, green after implementation.
- [x] Create `callback::Client` with `ClientOptions`, reused `client_id`, `init_type`, `CallbackState`, split buffers, pending map, cancelled request set, and closed flag.
- [x] Add `Client::new`, `Client::with_options`, `reader_buffer`, `writer_buffer`, `close`, `select_database`, `ping`, `is_inited`, `init_type`, `pending_len`, and `cancel_request` with concrete state initialization so public API compiles.
- [x] Add init tests:
  - first `handle_init()` writes exactly one 64-byte Init frame into `writer_buffer` and returns `Ok(false)`;
  - repeated `handle_init()` before response does not write duplicate Init frames;
  - partial Init response in `reader_buffer` returns `Ok(false)` and preserves bytes;
  - full Init response returns `Ok(true)`, consumes the response, and stores `init_type`;
  - after `handle_disconnect()`, the next Init reuses the same `clientId`.
- [x] Implement `handle_init()` as the `New -> InitSent -> Inited -> Disconnected -> InitSent` state machine from `design.md`.
- [x] Add internal command-dispatch tests using a test-only Ping/Lock command:
  - command registration inserts pending before appending encoded bytes to `writer_buffer`;
  - pending deadline equals `encoded.timeout.low_16_seconds + command_timeout_grace`, including timeout `0`;
  - `ping(callback)` resolves by requestId;
  - `handle_read()` parses one complete response and invokes exactly the matching callback;
  - `handle_read()` parses two sticky responses in arrival order;
  - response half packets remain buffered until the full frame arrives;
  - Lock responses with extra data wait for 4-byte length and complete payload, reject payload length greater than `max_frame_size`, and preserve Java `payload_len + 4` raw layout.
- [x] Implement internal `send_command_callback(command, callback)` and `handle_read()` dispatch over pending requestId, with deadline computed from the encoded command rather than supplied by callers.
- [x] Ensure callbacks are invoked after releasing pending/state/buffer locks by adding a regression test whose callback starts another command on the same client.
- [x] Implement `RequestHandle::cancel()` and `Client::cancel_request()` so pending is removed, late responses are ignored through the cancelled request set, and the user callback is not invoked.
- [x] Add error-path tests for invalid magic/version, unknown requestId, and malformed or oversized extra-data length returning `SlockError::Protocol`.
- [x] Add cancellation tests proving cancel returns `Ok(true)` once, returns `Ok(false)` after completion/unknown id, and ignores a late cancelled response without firing callback.
- [x] Add `handle_disconnect()` tests proving it clears reader/writer buffers, fails all pending callbacks with `ClientDisconnected`, clears cancelled ids, moves state to `Disconnected`, and returns `Ok(true)` only when `auto_reconnect` is enabled and client is not closed.
- [x] Add `next_deadline()` and `handle_timeout(now)` tests proving the earliest pending deadline is returned and expired callbacks are failed and removed.
- [x] Run `cargo check --no-default-features`.
- [x] Run `cargo test --no-default-features --test callback_client`.
- [x] Run `cargo test --all-features --test callback_buffer --test callback_client`.
- [x] Commit callback buffer/client core. Superseded by final implementation commit.

## Task 15: Callback Primitive Facade

**Files:**
- Modify: `src/callback/database.rs`
- Create: `src/callback/primitives.rs`
- Modify: `src/callback/mod.rs`
- Modify: `src/primitive/state.rs`
- Modify: `src/primitive/lock_logic.rs`
- Modify: `src/primitive/event_logic.rs`
- Modify: `src/primitive/group_event_logic.rs`
- Modify: `src/primitive/flow_logic.rs`
- Modify: `src/primitive/tree_lock_logic.rs`
- Test: `tests/callback_primitives.rs`
- Test: `tests/callback_state_mock.rs`

- [x] Implement `callback::Database` with db id, default timeout/expired flags, `send_command_callback`, and factory methods for every primitive.
- [x] Implement `callback::RequestHandle { request_id: Id16, client: Weak<ClientInner> }` with `request_id()` and `cancel()`, and return it from all callback business methods.
- [x] Implement callback continuation support for multi-command primitive operations; each public operation must call the user callback exactly once and must stop sending follow-up commands after an error or cancel.
- [x] Implement `callback::Lock` with shared internal state so cloned handles and callbacks can observe `current_data` as an owned `Option<LockResultData>` snapshot.
- [x] Add Lock tests proving `acquire`, `acquire_with_data`, `release`, `release_with_data`, `show`, `update`, `release_head`, and `release_head_to_lock_wait` write the same command headers/flags/data as existing blocking/aio APIs.
- [x] Add Lock result-mapping tests proving `LOCKED_ERROR`, `UNLOCK_ERROR`, `UNOWN_ERROR`, `TIMEOUT`, `EXPRIED`, and `STATE_ERROR` map to the same `SlockError` variants as blocking/aio before invoking the user callback.
- [x] Add current-data tests proving callback state is updated before the callback body runs and `current_data()` returns a clone snapshot rather than an internal reference.
- [x] Implement callback `Event` with `is_set`, `clear`, `set`, `wait`, `clear_with_data`, `set_with_data`, and `current_data`.
- [x] Add Event tests for default-set and default-unset command construction, timeout mapping to `EventWaitTimeout`, and wait data callback propagation.
- [x] Implement callback `GroupEvent`, including version lockId update from wait/wakeup responses.
- [x] Add GroupEvent tests for version `2/3` parity behavior and callback data propagation.
- [x] Implement callback `Semaphore`, `ReentrantLock`, `ReadWriteLock`, `PriorityLock`, `MaxConcurrentFlow`, and `TokenBucketFlow`.
- [x] Add command-construction and callback-result tests for each flow/lock primitive, preserving existing count, r_count, priority, millisecond-expired, and release semantics.
- [x] Implement callback `TreeLock` and `TreeLeafLock` APIs matching the blocking/aio Java-parity helper surface.
- [x] Add TreeLock tests for root/child/leaf command sequencing, continuation success, continuation failure, cancellation during an intermediate step, and callback exactly-once behavior.
- [x] Ensure every callback primitive returns `NotConnected` without writing to `writer_buffer` when client state is not `Inited`.
- [x] Run `cargo test --no-default-features --test callback_primitives --test callback_state_mock`.
- [x] Run `cargo test --all-features --test primitive_commands --test primitive_state_mock --test callback_primitives --test callback_state_mock`.
- [x] Run `cargo test --features blocking --no-default-features` and `cargo test --features aio --no-default-features` to prove shared helper changes did not regress existing facades.
- [x] Commit callback primitive facade. Superseded by final implementation commit.

## Task 16: Callback Docs, Examples, and Final Verification

**Files:**
- Modify: `README.md`
- Modify: `docs/Architecture.md`
- Modify: `design.md`
- Modify: `plan.md`
- Modify rustdoc comments in `src/callback/mod.rs`, `src/callback/client.rs`, `src/callback/buffer.rs`, and `src/callback/primitives.rs`
- Test: callback doctests in `src/lib.rs` and `src/callback/mod.rs`

- [x] Add README callback quickstart showing caller-created TCP/event-loop ownership: call `handle_init()`, drain `WriterBuffer`, push bytes into `ReaderBuffer`, call `handle_read()`, and handle disconnect with `handle_disconnect()`.
- [x] Add README note that callback is always compiled, owns no socket, starts no thread, and depends on no tokio/socket2 runtime dependency under `default-features=false`.
- [x] Add rustdoc examples for `callback::Client`, `ReaderBuffer`, `WriterBuffer`, `RequestHandle::cancel`, `Lock::acquire`, `Event::wait`, `handle_disconnect`, and `handle_timeout`.
- [x] Update `docs/Architecture.md` with a callback/Sans-IO section covering split buffer ownership, init flow, command write flow, Java extra-data raw layout, cancellation, callback trigger order, disconnect, and timeout.
- [x] Update `plan.md` execution status after implementation, marking Task 14, Task 15, and Task 16 completed only after their verification commands pass.
- [x] Run `cargo test --doc --all-features`.
- [x] Run `cargo check --no-default-features`.
- [x] Run `cargo test --no-default-features --test callback_buffer --test callback_client --test callback_primitives --test callback_state_mock`.
- [x] Run `cargo test --features blocking --no-default-features`.
- [x] Run `cargo test --features aio --no-default-features`.
- [x] Run `cargo test --all-features`.
- [x] Run `cargo fmt --check`.
- [x] Run `cargo clippy --all-features --all-targets -- -D warnings`.
- [x] Run `git diff --check`.
- [x] Commit callback docs and final verification updates. Superseded by final implementation commit.

## Verification Commands

- [x] `cargo fmt --check`
- [x] `cargo clippy --all-features --all-targets -- -D warnings`
- [x] `cargo test --lib --all-features`
- [x] `cargo test --features blocking --no-default-features`
- [x] `cargo test --features aio --no-default-features`
- [x] `cargo test --all-features`
- [x] `SLOCK_TEST_HOST=127.0.0.1 SLOCK_TEST_PORT=5658 cargo test --all-features --test java_parity_blocking --test java_parity_async --test java_parity_replset --test java_parity_lock_data --test java_parity_flow_tree`
- [x] `cargo test --all-features --test java_parity_benchmark -- --ignored`
- [x] `cargo check --no-default-features`
- [x] `cargo test --no-default-features --test callback_buffer --test callback_client`
- [x] `cargo test --no-default-features --test callback_primitives --test callback_state_mock`
- [x] `cargo test --all-features --test callback_buffer --test callback_client --test callback_primitives --test callback_state_mock`

## Plan Review

Completeness review:

- Covers the requested root `plan.md` handoff.
- Covers crate scaffold, shared protocol, LockData, blocking transport, async transport, database factories, all synchronization primitives, replset, Java parity tests, and docs.
- Covers the 2026-05-19 interchangeable-client requirement through a dedicated `ClientApi`/`ClientHandle` task for both blocking and async APIs.
- Covers the 2026-05-20 Java-compatible connection lifecycle requirement: reader starts only after initial connect/init success, then reconnects until explicit close.
- Covers the 2026-05-20 feature cleanup requirement: replset is part of the selected calling model, not its own feature.
- Covers the 2026-05-21 callback/Sans-IO requirement through dedicated split-buffer/client, cancellation, primitive facade, docs, and callback-only test tasks.
- Covers callback Java-compatibility details that are easy to miss: `ClientDisconnected`, clientId reuse across reconnect init, Java `payload_len + 4` extra-data raw layout, encoded-timeout deadline math, and callback-only dependency checks.
- Covers the full Java `ClientTest.java` test list, including benchmark as ignored-by-default.
- Covers both unit tests and integration/parity tests.
- Covers final verification commands.

Reasonableness review:

- The milestone order is reasonable: protocol first, then blocking single client, then async, then primitives, then replset and parity.
- The plan avoids using an async runtime under blocking APIs, matching `design.md`.
- The reconnect behavior is assigned to the transport layer while command replay semantics stay with replset, avoiding hidden duplicate execution in single-node clients.
- The callback work is separated after the existing TCP-owned facades so it can reuse stable protocol and primitive logic without destabilizing blocking/aio behavior.
- The callback tasks test half-packet, sticky-packet, callback reentrancy, cancellation, timeout, disconnect, max-frame protection, and multi-command continuation behavior without requiring a real TCP server.
- The scope is large for one uninterrupted implementation session, so execution should proceed task-by-task with verification after each commit.
- The only external runtime dependency is a local `slock` service for integration/parity tests; tests that require it should skip or be explicitly gated when the service is unavailable.

Assumptions:

- `replset` is part of v1 and not deferred.
- `Client` and `ReplsetClient` must remain business-code interchangeable through the same abstraction; runtime construction may vary by single or multiple endpoints, but downstream usage must not vary.
- Single-node automatic reconnect restores the socket/session but does not silently replay in-flight lock commands.
- Callback client is single-node in the first implementation; callback replset is a future extension requiring separate per-node buffers and connection lifecycle events.
- Callback APIs own no TCP socket and start no background reader or timer; the caller drives init, read, disconnect, and timeout.
- Callback cancellation does not retract bytes already written to `WriterBuffer` or already sent by the caller; it removes pending state and ignores the eventual response.
- Callback extra-data frame size is bounded by `ClientOptions::max_frame_size`, defaulting to 16 MiB unless implementation evidence suggests a different safe default.
- Tokio is the async runtime.
- Java compatibility is defined by `docs/Architecture.md`, `design.md`, and `D:\workspace\github\jaslock\src\test\java\io\github\snower\jaslock\ClientTest.java`.

# ruslock Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` for parallelizable phases or `superpowers:executing-plans` for inline execution. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Rust `slock` client library that provides both `ruslock::blocking` synchronous APIs and `ruslock::aio` async/await APIs, with behavior verified against the Java `jaslock` implementation.

**Architecture:** Reuse one shared protocol/data/primitive-logic core, then implement separate blocking and async transport layers. Blocking uses `std::net::TcpStream` plus a reader supervisor thread; async uses tokio reader supervisor tasks. Both transports connect/init before starting the reader and then auto-reconnect until explicit close.

**Tech Stack:** Rust, Cargo, tokio, bitflags, md-5, rand, socket2, thiserror, local `slock` service for integration/parity tests.

---

## Summary

The repository currently contains design documents but no Rust crate. This plan starts by saving the crate scaffold, then builds protocol compatibility, blocking transport, async transport, all synchronization primitives, replset support, and Java parity tests migrated from:

```text
D:\workspace\github\jaslock\src\test\java\io\github\snower\jaslock\ClientTest.java
```

The implementation must preserve the protocol details documented in `docs/Architecture.md` and `design.md`: 64-byte command frames, little-endian numeric fields, LockData framing, requestId response matching, key normalization, timeout/expired flags, single-connection auto-reconnect behavior, and replset retry behavior.

New requirement added on 2026-05-19: `Client` and `ReplsetClient` must implement the same public abstraction inside each calling model, so business code can switch between single IP and multi-IP deployment by changing construction/configuration only. After construction, usage must be identical through `ClientApi`/`ClientHandle`, shared `Database`, and shared primitive facade types.

New requirement added on 2026-05-20: `Client::open()` must start the reader only after the first TCP connect + Init succeeds. After that, the reader thread/task owns disconnect detection and automatic reconnect, reuses the same `clientId`, refreshes `init_type`, and keeps retrying by `ClientOptions::reconnect_interval` until `close()` or `auto_reconnect=false`. Single-node clients do not silently replay failed in-flight business commands; replset retry remains handled by the replset layer.

New requirement added on 2026-05-20: replset support must not be exposed as a separate Cargo feature. `blocking::ReplsetClient` is compiled with `blocking`, and `aio::ReplsetClient` is compiled with `aio`; default features are only `["blocking", "aio"]`.

## Execution Status

Last updated: 2026-05-20.

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

Partially completed and still in progress:

- [x] Plan commit steps are represented by one final implementation commit rather than rewritten as historical per-task commits.

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

## Verification Commands

- [x] `cargo fmt --check`
- [x] `cargo clippy --all-features --all-targets -- -D warnings`
- [x] `cargo test --lib --all-features`
- [x] `cargo test --features blocking --no-default-features`
- [x] `cargo test --features aio --no-default-features`
- [x] `cargo test --all-features`
- [x] `SLOCK_TEST_HOST=127.0.0.1 SLOCK_TEST_PORT=5658 cargo test --all-features --test java_parity_blocking --test java_parity_async --test java_parity_replset --test java_parity_lock_data --test java_parity_flow_tree`
- [x] `cargo test --all-features --test java_parity_benchmark -- --ignored`

## Plan Review

Completeness review:

- Covers the requested root `plan.md` handoff.
- Covers crate scaffold, shared protocol, LockData, blocking transport, async transport, database factories, all synchronization primitives, replset, Java parity tests, and docs.
- Covers the 2026-05-19 interchangeable-client requirement through a dedicated `ClientApi`/`ClientHandle` task for both blocking and async APIs.
- Covers the 2026-05-20 Java-compatible connection lifecycle requirement: reader starts only after initial connect/init success, then reconnects until explicit close.
- Covers the 2026-05-20 feature cleanup requirement: replset is part of the selected calling model, not its own feature.
- Covers the full Java `ClientTest.java` test list, including benchmark as ignored-by-default.
- Covers both unit tests and integration/parity tests.
- Covers final verification commands.

Reasonableness review:

- The milestone order is reasonable: protocol first, then blocking single client, then async, then primitives, then replset and parity.
- The plan avoids using an async runtime under blocking APIs, matching `design.md`.
- The reconnect behavior is assigned to the transport layer while command replay semantics stay with replset, avoiding hidden duplicate execution in single-node clients.
- The scope is large for one uninterrupted implementation session, so execution should proceed task-by-task with verification after each commit.
- The only external runtime dependency is a local `slock` service for integration/parity tests; tests that require it should skip or be explicitly gated when the service is unavailable.

Assumptions:

- `replset` is part of v1 and not deferred.
- `Client` and `ReplsetClient` must remain business-code interchangeable through the same abstraction; runtime construction may vary by single or multiple endpoints, but downstream usage must not vary.
- Single-node automatic reconnect restores the socket/session but does not silently replay in-flight lock commands.
- Tokio is the async runtime.
- Java compatibility is defined by `docs/Architecture.md`, `design.md`, and `D:\workspace\github\jaslock\src\test\java\io\github\snower\jaslock\ClientTest.java`.

# ruslock

Rust client for `slock`.

The crate exposes three independent APIs:

- `ruslock::blocking` uses `std::net::TcpStream` and a reader thread.
- `ruslock::aio` uses `tokio` and `async/await`.
- `ruslock::callback` is Sans-IO: it owns no socket and is driven by the caller.

The blocking API does not wrap a tokio runtime. Both APIs share the same protocol,
LockData, error, and primitive logic.

## Blocking Quickstart

```rust,no_run
use ruslock::{LockData, Result};

fn main() -> Result<()> {
    let client = ruslock::blocking::Client::connect("127.0.0.1:5658")?;
    let mut lock = client.lock("order:1001", 5, 10);

    lock.acquire_with_data(LockData::set("aaa"))?;
    lock.release()?;
    client.close();
    Ok(())
}
```

## Async Quickstart

```rust,no_run
use ruslock::{LockData, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let client = ruslock::aio::Client::connect("127.0.0.1:5658").await?;
    let mut lock = client.lock("order:1001", 5, 10);

    lock.acquire_with_data(LockData::set("aaa")).await?;
    lock.release().await?;
    client.close().await;
    Ok(())
}
```

## Replset

```rust,no_run
use ruslock::Result;

fn main() -> Result<()> {
    let client = ruslock::blocking::ReplsetClient::connect(
        "127.0.0.1:5658,127.0.0.1:5659",
    )?;
    let mut lock = client.lock("replset-key", 5, 10);
    lock.acquire()?;
    lock.release()?;
    client.close();
    Ok(())
}
```

## Callback / Sans-IO Quickstart

The callback API is for external schedulers such as Python asyncio, mio, or an
FFI host. The client never opens TCP. The caller sends bytes drained from
`WriterBuffer`, pushes received bytes into `ReaderBuffer`, and calls the handler
methods to advance the protocol state.

```rust,no_run
use ruslock::{LockData, Result};

fn drive_callback_client() -> Result<()> {
    let client = ruslock::callback::Client::new();
    let reader = client.reader_buffer();
    let writer = client.writer_buffer();

    client.handle_init()?;
    let init_bytes = writer.drain();
    // caller sends init_bytes through its own socket
    // caller receives bytes from that socket:
    # let init_response = vec![0u8; 64];
    reader.push(&init_response);
    let _ready = client.handle_init()?;

    let lock = client.lock("order:1001", 5, 10);
    let _request = lock.acquire_with_data(LockData::set("aaa"), |result| {
        let _ = result;
    })?;
    let command_bytes = writer.drain();
    // caller sends command_bytes and later pushes response bytes into reader
    # let response_bytes = vec![0u8; 64];
    reader.push(&response_bytes);
    let _callbacks = client.handle_read()?;

    Ok(())
}
```

If the caller's socket disconnects, call `handle_disconnect()`. `true` means the
caller should reconnect and then call `handle_init()` again. Pending callbacks
receive `ClientDisconnected`; cancelled request handles ignore late responses.

## Runtime Client Selection

Use `ClientHandle` when deployment is selected from configuration. A single
node creates a normal client backend, while multiple nodes create a replset
backend. The business code after construction is identical.

```rust,no_run
use ruslock::Result;

fn run(nodes: String) -> Result<()> {
    let client = ruslock::blocking::ClientHandle::connect(nodes)?;
    let mut lock = client.lock("order:1001", 5, 10);

    lock.acquire()?;
    lock.release()?;
    client.close();
    Ok(())
}
```

```rust,no_run
use ruslock::Result;

async fn run(nodes: String) -> Result<()> {
    let client = ruslock::aio::ClientHandle::connect(nodes).await?;
    let mut lock = client.lock("order:1001", 5, 10);

    lock.acquire().await?;
    lock.release().await?;
    client.close().await;
    Ok(())
}
```

## LockData

```rust
use ruslock::LockData;

let data = LockData::pipeline(vec![
    LockData::set("aaa"),
    LockData::append("bbb"),
]);
let encoded = data.encode().unwrap();
assert_eq!(encoded[4], ruslock::protocol::constants::LOCK_DATA_COMMAND_TYPE_PIPELINE);
```

## Feature Flags

- `blocking`: synchronous client facade.
- `aio`: async tokio client facade.
- `callback`: always compiled, no feature flag required.
- Replset support is included with each facade: `blocking::ReplsetClient` is available with `blocking`, and `aio::ReplsetClient` is available with `aio`.
- default: `["blocking", "aio"]`.
- With `default-features=false`, callback-only builds do not pull in tokio or socket2.

## Tests

Unit and mock transport tests do not require a server:

```powershell
cargo test --all-features
```

Java parity and integration tests use a local `slock` endpoint. Defaults match
the Java client tests:

```powershell
$env:SLOCK_TEST_HOST = "127.0.0.1"
$env:SLOCK_TEST_PORT = "5658"
cargo test --all-features --test java_parity_blocking --test java_parity_async --test java_parity_replset --test java_parity_lock_data --test java_parity_flow_tree
```

The benchmark parity test is ignored by default:

```powershell
cargo test --all-features --test java_parity_benchmark -- --ignored
```

# ruslock

Rust client for `slock`.

The crate exposes two independent APIs:

- `ruslock::blocking` uses `std::net::TcpStream` and a reader thread.
- `ruslock::aio` uses `tokio` and `async/await`.

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
- `replset`: replset facade.
- default: `["blocking", "aio", "replset"]`.

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

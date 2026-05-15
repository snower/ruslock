# ruslock Rust Driver Design

本文是在 `docs/Architecture.md` 的协议和 Java 实现梳理基础上，面向 `ruslock` 的完整 Rust 库设计。目标是实现一个兼容 `slock` 二进制协议的 Rust driver，并同时提供普通 blocking 同步调用接口和 `async/await` 异步调用接口。

## 1. 设计目标

1. 协议兼容：严格复刻 `jaslock` 的 64 字节命令头、LockData 编码、requestId 匹配、Init/Ping/Lock/Unlock 行为。
2. 双调用模型：同一 crate 同时提供 `ruslock::blocking` 和 `ruslock::aio` 两套 API。
3. 共享核心逻辑：协议、数据编码、错误映射、key 归一化、同步原语的命令构造逻辑只实现一份。
4. 传输层隔离：blocking 使用 `std::net::TcpStream` 和线程；async 使用 `tokio::net::TcpStream` 和 task。同步接口不依赖嵌套 tokio runtime。
5. 高级 API 完整：覆盖 `Lock`, `Event`, `GroupEvent`, `Semaphore`, `ReentrantLock`, `ReadWriteLock`, `PriorityLock`, `MaxConcurrentFlow`, `TokenBucketFlow`, `TreeLock`。
6. 可测试：协议编解码可脱离网络单测，传输和 API 可用本地 `slock` 做集成测试。

非目标：

- 不实现 `slock` server。
- 第一版不设计 TLS、认证和连接池，除非后续协议明确需要。
- 不逐字迁移 Java callback executor；Rust 以 `async/await` 和 blocking wait 为主。

## 2. 方案选择

### 2.1 候选方案

| 方案 | 描述 | 优点 | 缺点 |
| --- | --- | --- | --- |
| A. async-first + blocking `block_on` | 全部底层逻辑用 tokio，blocking API 内部创建 runtime 执行 async 方法 | 代码量少 | 在已有 runtime 中调用 blocking API 容易 panic 或死锁；blocking 用户也被迫引入 tokio 行为 |
| B. 共享协议 + 双传输层 | 协议/数据/原语状态共享，blocking 和 async 分别实现 transport | 同步和异步语义干净；运行时依赖清晰 | 传输层代码量略多 |
| C. 完全泛型 transport trait | 用统一 trait 抽象同步/异步 transport，所有上层泛型化 | 理论复用最大 | Rust 中同步/异步 trait 边界复杂，API 会变重 |

推荐方案：B。

理由：这个 driver 面向网络 IO，blocking 和 async 的调度模型差异很大。共享协议层和同步原语命令构造逻辑即可避免核心重复；传输层分开实现反而更清楚，也能保证 blocking 用户不需要关心 tokio runtime。

### 2.2 对外模块

```rust
use ruslock::blocking;
use ruslock::aio;
```

两套 API 命名尽量一致：

```rust
// blocking
let client = ruslock::blocking::Client::connect("127.0.0.1:5658")?;
let mut lock = client.lock("order:1001", 5, 5);
lock.acquire()?;
lock.release()?;

// async
let client = ruslock::aio::Client::connect("127.0.0.1:5658").await?;
let mut lock = client.lock("order:1001", 5, 5);
lock.acquire().await?;
lock.release().await?;
```

## 3. Crate 结构

```text
src/
  lib.rs
  error.rs
  options.rs
  time.rs
  key.rs
  protocol/
    mod.rs
    constants.rs
    id.rs
    command.rs
    result.rs
    codec.rs
  data/
    mod.rs
    lock_data.rs
    lock_result_data.rs
  primitive/
    mod.rs
    state.rs
    lock_logic.rs
    event_logic.rs
    group_event_logic.rs
    flow_logic.rs
    tree_lock_logic.rs
  blocking/
    mod.rs
    client.rs
    connection.rs
    replset.rs
    database.rs
    primitives.rs
  aio/
    mod.rs
    client.rs
    connection.rs
    replset.rs
    database.rs
    primitives.rs
```

### 3.1 共享模块职责

| 模块 | 职责 |
| --- | --- |
| `error` | `SlockError`, `LockErrorKind`, protocol/client/data 错误 |
| `options` | `ClientOptions`, reconnect 和 timeout 配置 |
| `time` | `PackedTime`, timeout/expired flag 合并 |
| `key` | 16 字节 key/lockId 归一化 |
| `protocol` | 常量、ID 生成、命令/响应结构体、二进制编解码 |
| `data` | `LockData` 构造、pipeline、响应 value 解析 |
| `primitive` | 与 IO 无关的同步原语命令构造和响应错误映射 |

### 3.2 blocking 模块职责

| 模块 | 职责 |
| --- | --- |
| `blocking::Client` | 单节点同步 client facade |
| `blocking::Connection` | `std::net::TcpStream`、reader thread、writer mutex、pending map |
| `blocking::ReplsetClient` | 多节点同步 client、leader 选择和 pending 重发 |
| `blocking::Database` | 同步 database facade |
| `blocking::primitives` | 同步 `Lock/Event/...` 包装器 |

### 3.3 aio 模块职责

| 模块 | 职责 |
| --- | --- |
| `aio::Client` | 单节点异步 client facade |
| `aio::ConnectionActor` | tokio task，管理连接、读写、pending map、重连 |
| `aio::ReplsetClient` | 多节点异步 client、leader 选择和 pending 重发 |
| `aio::Database` | 异步 database facade |
| `aio::primitives` | 异步 `Lock/Event/...` 包装器 |

## 4. Cargo feature 设计

```toml
[features]
default = ["blocking", "aio", "replset"]
blocking = []
aio = ["dep:tokio"]
replset = []

[dependencies]
bitflags = "2"
md-5 = "0.10"
rand = "0.8"
socket2 = "0.5"
thiserror = "1"
tokio = { version = "1", optional = true, features = ["net", "sync", "time", "rt", "macros"] }
```

说明：

- 默认同时启用 blocking 和 aio，满足“双接口”要求。
- `aio` 仅在异步接口启用时引入 tokio。
- 协议层不依赖 tokio。
- `socket2` 用于跨平台设置 TCP keepalive。
- 第一版尽量不用 `async-trait`，通过同步/异步 facade 分离避免 trait object 复杂度。

## 5. 公共类型设计

### 5.1 错误类型

```rust
pub type Result<T> = std::result::Result<T, SlockError>;

#[derive(Debug, thiserror::Error)]
pub enum SlockError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("client closed")]
    ClientClosed,

    #[error("client not connected")]
    NotConnected,

    #[error("command timeout")]
    CommandTimeout,

    #[error("lock locked")]
    LockLocked(Box<LockCommandResult>),

    #[error("lock unlocked")]
    LockUnlocked(Box<LockCommandResult>),

    #[error("lock not owned")]
    LockNotOwn(Box<LockCommandResult>),

    #[error("lock timeout")]
    LockTimeout(Box<LockCommandResult>),

    #[error("lock expired")]
    LockExpired(Box<LockCommandResult>),

    #[error("server state error")]
    StateError(Box<LockCommandResult>),

    #[error("server error result {result}")]
    Server { result: u8 },

    #[error("lock data error: {0}")]
    LockData(String),

    #[error("event wait timeout")]
    EventWaitTimeout,
}
```

`Lock.acquire/release` 将 `LockCommandResult.result` 转换为上面的 lock 专用错误。底层 `send_command` 可以返回原始 `CommandResult`，高级 API 负责业务错误映射。

### 5.2 ClientOptions

```rust
pub struct ClientOptions {
    pub connect_timeout: Duration,
    pub reconnect_interval: Duration,
    pub command_timeout_grace: Duration,
    pub auto_reconnect: bool,
    pub tcp_nodelay: bool,
    pub tcp_keepalive: bool,
}
```

默认值对齐 Java：

| 配置 | 默认值 |
| --- | --- |
| connect timeout | 5 秒 |
| reconnect interval | 2 秒 |
| command timeout grace | 120 秒 |
| auto reconnect | true |
| tcp nodelay | true |
| tcp keepalive | true |

命令等待时间：

- 普通命令：`command_timeout_grace`。
- Lock 命令：`timeout.low_16_seconds + command_timeout_grace`。

### 5.3 PackedTime

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PackedTime(u32);

impl PackedTime {
    pub fn new(value: u16) -> Self;
    pub fn with_flags(value: u16, flags: u16) -> Self;
    pub fn value(self) -> u16;
    pub fn flags(self) -> u16;
    pub fn bits(self) -> u32;
    pub fn merge_flags(self, flags: u16) -> Self;
}
```

`timeout` 和 Java `expried` 协议字段都使用该类型。对外 API 使用 `expired` 命名，但协议结构体字段注释标明对应 Java `expried`。

### 5.4 Id16 与 Key16

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Id16([u8; 16]);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Key16([u8; 16]);
```

规则：

- key 长度 `> 16`：MD5。
- key 长度 `<= 16`：左补 0，原始字节右对齐。
- requestId/lockId/clientId：6 字节毫秒时间戳高位 + 6 字节随机数高位 + 4 字节自增计数。

## 6. 协议层设计

### 6.1 常量

`protocol::constants` 完整迁移 Java `ICommand`：

- command type。
- result code。
- init flag。
- lock/unlock flag。
- timeout/expired flag。
- lock data stage/type/flag。

常量以 `pub const` 暴露，另提供 `bitflags!` 类型用于更易读的组合：

```rust
bitflags::bitflags! {
    pub struct LockFlags: u8 {
        const SHOW_WHEN_LOCKED = 0x01;
        const UPDATE_WHEN_LOCKED = 0x02;
        const CONTAINS_DATA = 0x20;
    }
}
```

### 6.2 Command

```rust
pub enum Command {
    Init(InitCommand),
    Ping(PingCommand),
    Lock(LockCommand),
}

pub struct LockCommand {
    pub command_type: CommandType, // Lock or Unlock
    pub request_id: Id16,
    pub flag: u8,
    pub db_id: u8,
    pub lock_id: Id16,
    pub lock_key: Key16,
    pub timeout: PackedTime,
    pub expired: PackedTime,
    pub count: u16,
    pub r_count: u8,
    pub data: Option<LockData>,
}
```

`Command::encode()` 返回：

```rust
pub struct EncodedCommand {
    pub request_id: Id16,
    pub command_type: CommandType,
    pub header: [u8; 64],
    pub extra: Option<Vec<u8>>,
    pub timeout: Duration,
    pub expects_response: bool,
}
```

`extra` 对应 Java `getExtraData()`，包含 4 字节长度前缀。

### 6.3 Result

```rust
pub enum CommandResult {
    Init(InitCommandResult),
    Ping(PingCommandResult),
    Lock(LockCommandResult),
}
```

解码流程：

1. 读取 64 字节 header。
2. 校验 magic/version。
3. 根据 command type 解析 result。
4. `LockCommandResult` 如果 flag 含 `CONTAINS_DATA`，调用 transport 继续读取 extra payload。
5. 用 requestId 匹配 pending request。

协议层只负责解析，不负责等待、重试或业务错误转换。

## 7. LockData 设计

### 7.1 构造 API

```rust
pub enum LockData {
    Set(Vec<u8>, DataFlags),
    Unset(DataFlags),
    Incr(i64, DataFlags),
    Append(Vec<u8>, DataFlags),
    Shift(u32, DataFlags),
    Execute(Box<LockCommand>, DataStage, DataFlags),
    Pipeline(Vec<LockData>, DataFlags),
    Push(Vec<u8>, DataFlags),
    Pop(u32, DataFlags),
}
```

便捷构造：

```rust
LockData::set("value");
LockData::unset();
LockData::incr(1);
LockData::append("tail");
LockData::pipeline([LockData::set("a"), LockData::append("b")]);
```

所有字符串输入统一使用 UTF-8。

### 7.2 响应读取

```rust
pub struct LockResultData {
    raw: Vec<u8>,
    stage: DataStage,
    command_type: DataCommandType,
    flags: DataFlags,
    value_offset: usize,
}

impl LockResultData {
    pub fn as_bytes(&self) -> Option<&[u8]>;
    pub fn as_string(&self) -> Result<String>;
    pub fn as_i64(&self) -> i64;
    pub fn as_list(&self) -> Result<Vec<Vec<u8>>>;
    pub fn as_string_list(&self) -> Result<Vec<String>>;
    pub fn as_map(&self) -> Result<HashMap<String, Vec<u8>>>;
    pub fn as_string_map(&self) -> Result<HashMap<String, String>>;
}
```

`value_offset` 按 Java 行为处理 property：

- 无 property：offset = 6。
- 有 property：读取 `raw[6..8]` 小端长度，offset = `8 + property_len`。

## 8. 共享 primitive 逻辑

高级同步原语分两层：

1. `primitive::*_logic`：纯逻辑层，只构造命令、解释结果、维护本地状态。
2. `blocking::*` / `aio::*`：调用对应 client 发送命令。

例如 `Lock`：

```rust
pub struct LockState {
    pub db_id: u8,
    pub lock_key: Key16,
    pub lock_id: Id16,
    pub timeout: PackedTime,
    pub expired: PackedTime,
    pub count: u16,
    pub r_count: u8,
    pub current_data: Option<LockResultData>,
}

impl LockState {
    pub fn acquire_command(&self, flags: LockFlags, data: Option<LockData>) -> LockCommand;
    pub fn release_command(&self, flags: UnlockFlags, data: Option<LockData>) -> LockCommand;
    pub fn apply_result(&mut self, result: LockCommandResult) -> Result<LockCommandResult>;
}
```

blocking 和 async 只差发送方式：

```rust
// blocking
pub fn acquire(&mut self) -> Result<LockCommandResult> {
    let command = self.state.acquire_command(LockFlags::empty(), None);
    let result = self.database.client.send_lock(command)?;
    self.state.apply_result(result)
}

// async
pub async fn acquire(&mut self) -> Result<LockCommandResult> {
    let command = self.state.acquire_command(LockFlags::empty(), None);
    let result = self.database.client.send_lock(command).await?;
    self.state.apply_result(result)
}
```

这样不会重复协议逻辑，也不会强行把同步/异步抽象进同一个复杂 trait。

## 9. blocking 传输逻辑

### 9.1 内部结构

```rust
pub struct BlockingConnection {
    address: Address,
    options: ClientOptions,
    client_id: Mutex<Option<Id16>>,
    init_type: AtomicU8,
    state: AtomicConnectionState,
    writer: Mutex<Option<BufWriter<TcpStream>>>,
    pending: Mutex<HashMap<Id16, SyncPending>>,
    reader_handle: Mutex<Option<JoinHandle<()>>>,
    close_notify: Arc<AtomicBool>,
}

struct SyncPending {
    command_type: CommandType,
    deadline: Instant,
    tx: std::sync::mpsc::Sender<Result<CommandResult>>,
}
```

### 9.2 open

`Client::open()`：

1. 如果 state 已是 `Connected`，直接返回。
2. 建立 `TcpStream`。
3. 设置 `nodelay/keepalive`。
4. 写 InitCommand，读取 InitCommandResult。
5. 校验 init result。
6. 保存 writer。
7. 启动 reader thread。
8. state 置为 `Connected`。

`Client::connect(addr)` 是 `Client::new(addr).open()?` 的便捷方法。

### 9.3 send_command

```text
build command
create mpsc one-shot
insert pending before write
lock writer and write header + extra
flush
recv_timeout(deadline)
remove pending on timeout
map result
```

注意点：

- pending 必须先于 write 插入，避免响应过快导致 reader 找不到 requestId。
- write 失败时移除 pending，并关闭当前 socket 触发重连。
- timeout 后必须移除 pending，避免后续迟到响应唤醒错误 waiter。

### 9.4 reader thread

reader thread 循环：

1. `read_exact(64)`。
2. `protocol::decode_header()`。
3. 如果是 lock result 且有 data，继续读 4 字节长度和 payload。
4. 根据 requestId 从 pending map 移除 waiter。
5. 发送 result 给 waiter。
6. 读失败时：
   - 清空或保留 pending，取决于是否由 replset 接管。
   - 关闭 writer。
   - 如果 `auto_reconnect` 且 client 未 close，进入 reconnect loop。

### 9.5 reconnect

单节点 client 的 reconnect 策略：

- 每 `reconnect_interval` 重试。
- reconnect 成功后发送 init。
- 对单节点未完成请求，默认返回 `NotConnected` 或 `ClientClosed`，不自动重发，避免重复执行风险。
- 对 replset 子 client，未完成请求交给 replset pending 逻辑判断是否重发。

## 10. async 传输逻辑

### 10.1 内部结构

```rust
pub struct AsyncConnection {
    tx: tokio::sync::mpsc::Sender<ClientOp>,
    state: Arc<AsyncConnectionState>,
}

enum ClientOp {
    Send {
        command: Command,
        response: tokio::sync::oneshot::Sender<Result<CommandResult>>,
    },
    Write {
        command: Command,
        response: tokio::sync::oneshot::Sender<Result<()>>,
    },
    Close,
}
```

### 10.2 ConnectionActor

Async client 使用一个 connection actor 管理所有网络状态：

```text
Client facade
  -> mpsc<ClientOp>
    -> ConnectionActor
       - current writer
       - pending HashMap<Id16, AsyncPending>
       - reader task result channel
       - reconnect timer
```

actor 生命周期：

1. `connect_and_init()`。
2. split stream。
3. spawn reader task，reader 解码后把 `DecodedFrame` 发回 actor。
4. `tokio::select!` 同时处理：
   - 用户发送命令。
   - reader 解码结果。
   - reader 失败。
   - pending timeout。
   - close。
5. 连接失败时进入 reconnect loop。

### 10.3 send_command

异步 `send_command`：

1. facade 创建 `oneshot`。
2. 发送 `ClientOp::Send` 到 actor。
3. actor 编码 command，插入 pending，写入 writer。
4. facade `await` oneshot，并叠加 `tokio::time::timeout`。
5. 超时后发送 cancel 或让 actor 根据 requestId 删除 pending。

actor 内也维护 deadline，避免 facade 被 drop 后 pending 泄漏。

### 10.4 async close

`Client::close().await`：

- 向 actor 发送 `Close`。
- actor 标记 closed。
- 关闭 socket。
- 对全部 pending 返回 `ClientClosed`。
- 停止 reader task。

`Drop` 只做 best-effort close signal，不等待网络释放。

## 11. blocking API 设计

### 11.1 Client

```rust
pub mod blocking {
    pub struct Client { inner: Arc<BlockingClientInner> }

    impl Client {
        pub fn new<A: ToAddress>(addr: A) -> Self;
        pub fn with_options<A: ToAddress>(addr: A, options: ClientOptions) -> Self;
        pub fn connect<A: ToAddress>(addr: A) -> Result<Self>;
        pub fn open(&self) -> Result<()>;
        pub fn close(&self);
        pub fn ping(&self) -> Result<bool>;
        pub fn select_database(&self, db_id: u8) -> Database;
        pub fn lock<K: AsRef<[u8]>>(&self, key: K, timeout: u16, expired: u16) -> Lock;
        pub fn event<K: AsRef<[u8]>>(&self, key: K, timeout: u16, expired: u16, default_set: bool) -> Event;
    }
}
```

### 11.2 Database

```rust
impl Database {
    pub fn set_default_timeout_flags(&self, flags: u16);
    pub fn set_default_expired_flags(&self, flags: u16);
    pub fn lock<K: AsRef<[u8]>>(&self, key: K, timeout: u16, expired: u16) -> Lock;
    pub fn semaphore<K: AsRef<[u8]>>(&self, key: K, count: u16, timeout: u16, expired: u16) -> Semaphore;
    pub fn max_concurrent_flow<K: AsRef<[u8]>>(...);
    pub fn token_bucket_flow<K: AsRef<[u8]>>(...);
}
```

### 11.3 Lock

```rust
impl Lock {
    pub fn acquire(&mut self) -> Result<LockCommandResult>;
    pub fn acquire_with_data(&mut self, data: LockData) -> Result<LockCommandResult>;
    pub fn release(&mut self) -> Result<LockCommandResult>;
    pub fn release_with_data(&mut self, data: LockData) -> Result<LockCommandResult>;
    pub fn show(&mut self) -> Result<Option<LockCommandResult>>;
    pub fn update(&mut self, data: Option<LockData>) -> Result<()>;
    pub fn release_head(&mut self, data: Option<LockData>) -> Result<()>;
    pub fn release_head_to_lock_wait(&mut self, data: Option<LockData>) -> Result<LockCommandResult>;
    pub fn current_data(&self) -> Option<&LockResultData>;
}
```

RAII：

```rust
let mut lock = client.lock("k", 5, 5);
let guard = lock.acquire_guard()?;
guard.release()?;
```

`Drop` 中可以 best-effort release，但不能把 Drop release 当作可靠业务语义。文档应鼓励显式 `release()`。

## 12. async API 设计

### 12.1 Client

```rust
pub mod aio {
    pub struct Client { inner: Arc<AsyncClientInner> }

    impl Client {
        pub fn new<A: ToAddress>(addr: A) -> Self;
        pub fn with_options<A: ToAddress>(addr: A, options: ClientOptions) -> Self;
        pub async fn connect<A: ToAddress>(addr: A) -> Result<Self>;
        pub async fn open(&self) -> Result<()>;
        pub async fn close(&self);
        pub async fn ping(&self) -> Result<bool>;
        pub fn select_database(&self, db_id: u8) -> Database;
        pub fn lock<K: AsRef<[u8]>>(&self, key: K, timeout: u16, expired: u16) -> Lock;
    }
}
```

### 12.2 Lock

```rust
impl Lock {
    pub async fn acquire(&mut self) -> Result<LockCommandResult>;
    pub async fn acquire_with_data(&mut self, data: LockData) -> Result<LockCommandResult>;
    pub async fn release(&mut self) -> Result<LockCommandResult>;
    pub async fn release_with_data(&mut self, data: LockData) -> Result<LockCommandResult>;
    pub async fn show(&mut self) -> Result<Option<LockCommandResult>>;
    pub async fn update(&mut self, data: Option<LockData>) -> Result<()>;
    pub async fn release_head(&mut self, data: Option<LockData>) -> Result<()>;
    pub async fn release_head_to_lock_wait(&mut self, data: Option<LockData>) -> Result<LockCommandResult>;
    pub fn current_data(&self) -> Option<&LockResultData>;
}
```

异步 guard 不依赖 async Drop：

```rust
let mut lock = client.lock("k", 5, 5);
let guard = lock.acquire_guard().await?;
guard.release().await?;
```

如果用户 drop guard 但没有 release，只能做本地 warning 或 best-effort fire-and-forget，不承诺可靠释放。

## 13. Replset 设计

### 13.1 公共行为

`ReplsetClient` 对外与 `Client` 保持一致：

```rust
let client = ruslock::blocking::ReplsetClient::connect(["127.0.0.1:5658", "127.0.0.1:5659"])?;
let mut lock = client.lock("k", 5, 5);
lock.acquire()?;
```

异步：

```rust
let client = ruslock::aio::ReplsetClient::connect(["127.0.0.1:5658"]).await?;
```

### 13.2 状态

```rust
struct ReplsetState {
    clients: Vec<ClientHandle>,
    lived: Vec<usize>,
    leader: Option<usize>,
    pending: VecDeque<PendingCommand>,
    closed: bool,
}

struct PendingCommand {
    command: Command,
    retry_type: RetryType,
    deadline: Instant,
    response: PendingResponse,
}
```

`RetryType` 对齐 Java：

| 状态 | 语义 |
| --- | --- |
| `Origin` | 原始请求 |
| `Redirected` | 已尝试转到其他 live client |
| `Pending` | 已进入 pending 队列 |
| `Woken` | pending 被 leader/live client 唤醒后重发 |

### 13.3 请求选择和重试

请求流程：

1. 优先选择 leader。
2. 没有 leader 时选择第一个 lived client。
3. 没有 lived client：
   - 如果命令允许重试，进入 pending。
   - 否则返回 `NotConnected`。
4. 写失败：
   - 优先尝试另一个 live client。
   - 仍失败则进入 pending。
5. 收到 `COMMAND_RESULT_STATE_ERROR` 且是 lock result：
   - 进入 replset retry 流程。
6. 新 leader 或 live client 出现：
   - wakeup pending 队列。
   - 保持原 requestId，避免调用方等待对象失配。

### 13.4 子 client 与 replset 通信

子 client 在 init 或后续 init command 中上报：

- 当前节点是否 leader。
- 是否已有 leader。
- 是否 shutdown。

replset 根据事件更新 `leader/lived` 并唤醒 pending。blocking 版本用 `Mutex + Condvar` 或 channel；async 版本用 `tokio::sync::Mutex + Notify`。

## 14. Database 与默认 flag

`Database` 是 facade，不拥有连接：

```rust
pub struct Database {
    client: ClientHandle,
    db_id: u8,
    default_timeout_flags: AtomicU16,
    default_expired_flags: AtomicU16,
}
```

创建 primitive 时合并 flag：

```text
timeout = PackedTime::with_flags(timeout_value, database.default_timeout_flags)
expired = PackedTime::with_flags(expired_value, database.default_expired_flags)
```

client 保存 256 个 database 槽位。Rust 侧可以用：

```rust
Mutex<Vec<Option<Database>>>
```

或者简单每次返回新的轻量 `Database`。为了避免复杂缓存，推荐第一版每次返回轻量 clone facade；它只包含 `Arc<ClientInner>` 和 db_id，不需要强缓存。

## 15. 同步原语逻辑

### 15.1 Lock

基础命令：

- acquire -> `COMMAND_TYPE_LOCK`。
- release -> `COMMAND_TYPE_UNLOCK`。
- with data -> 自动设置 `CONTAINS_DATA` flag。
- show -> acquire + `LOCK_FLAG_SHOW_WHEN_LOCKED`，把 `LockNotOwn` 视为查询成功结果。
- update -> acquire + `LOCK_FLAG_UPDATE_WHEN_LOCKED`，把 `LockLocked` 视为更新成功路径。
- release head -> 全 0 lockId + `UNLOCK_FLAG_UNLOCK_FIRST_LOCK_WHEN_UNLOCKED`。
- release head to lock wait -> 再加 `UNLOCK_FLAG_SUCCED_TO_LOCK_WAIT`。

### 15.2 Event

内部用三类 lock：

- `event_lock`：set/clear 操作。
- `check_lock`：is_set。
- `wait_lock`：wait。

`default_set = true`：

- clear：`event_lock.acquire(UPDATE_WHEN_LOCKED)`，忽略 `LockLocked`。
- set：`event_lock.release()`，忽略 `LockUnlocked`。
- wait：`wait_lock.acquire()`，超时转换为 `EventWaitTimeout`。

`default_set = false`：

- clear：`event_lock.release()`，忽略 `LockUnlocked`。
- set：`event_lock.acquire(UPDATE_WHEN_LOCKED)`，忽略 `LockLocked`。
- wait：timeout 加 `TIMEOUT_FLAG_LOCK_WAIT_WHEN_UNLOCK` 后 acquire。

### 15.3 ReentrantLock

- 固定一个 lockId。
- `r_count = 0xff`。
- acquire/release 复用同一个底层 Lock。

### 15.4 ReadWriteLock

- write lock：`count = 0`。
- read lock：每次 acquire 生成新 lockId，`count = 0xffff`。
- release_read 释放本地队列中最早的 read lock。

### 15.5 Semaphore

- 构造时 `count = max(count - 1, 0)`。
- acquire 每次生成新 lockId。
- release 使用全 0 lockId + `UNLOCK_FIRST_LOCK_WHEN_UNLOCKED`。

### 15.6 PriorityLock

- timeout flags 加 `TIMEOUT_FLAG_RCOUNT_IS_PRIORITY`。
- `r_count = priority`。
- acquire/release 复用同一个 lock。

### 15.7 MaxConcurrentFlow

- 与 Semaphore 类似，`count = max - 1`。
- 缓存一个 flow lock。
- priority > 0 时启用 `TIMEOUT_FLAG_RCOUNT_IS_PRIORITY`。

### 15.8 TokenBucketFlow

- 无 release。
- period < 3 秒：expired = `ceil(period * 1000)` + `EXPRIED_FLAG_MILLISECOND_TIME`。
- period >= 3 秒：先尝试对齐到下一个周期边界，超时后用完整周期重试。

### 15.9 GroupEvent

- lockId 编码：前 8 字节 versionId 小端，后 8 字节 clientId 小端。
- wait/wakeup 根据返回 lockId 更新 versionId。
- 使用 `TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED`。

### 15.10 TreeLock

- root 无 parent。
- child 保存 parentKey。
- leaf lock 使用 `count = 0xffff`, `r_count = 1`, `TIMEOUT_FLAG_RCOUNT_IS_PRIORITY`。
- child leaf acquire 时先做 child check lock 和 parent check lock，再 acquire leaf。
- release 使用 `UNLOCK_FLAG_UNLOCK_TREE_LOCK`。

## 16. 并发与线程安全

### 16.1 blocking

- `Client`, `Database` 可 clone，内部是 `Arc`。
- `send_command` 可多线程并发调用。
- 写 socket 由 `Mutex<BufWriter<TcpStream>>` 串行化。
- 读 socket 由唯一 reader thread 负责。
- pending map 用 `Mutex<HashMap<Id16, SyncPending>>`。
- 每个高级 primitive 是否 `Send` 取决于内部是否保存可变 `current_data`；推荐 primitive 本身不实现内部锁，用户需要 `&mut self` 调用。

### 16.2 async

- `Client`, `Database` 可 clone，内部是 `Arc` + `mpsc`。
- 所有网络 IO 在 actor 中串行处理。
- facade 方法只发送 operation 并 await response。
- 高级 primitive 使用 `&mut self` 更新 `current_data`，避免额外 mutex。

## 17. 超时策略

| 类型 | 策略 |
| --- | --- |
| connect timeout | `ClientOptions.connect_timeout` |
| command timeout | 普通命令 120 秒 grace |
| lock command timeout | `(timeout.value + grace)` 秒 |
| event wait timeout | 底层 lock timeout 映射为 `EventWaitTimeout` |
| replset pending timeout | 沿用原 command deadline，不因重试重置 |

`timeout.value = 0` 时仍有 120 秒 command grace，这是 Java 行为。后续可以提供 `ClientOptions` 覆盖 grace。

## 18. close 与资源释放

`close()` 必须保证：

1. 标记 closed。
2. 停止自动重连。
3. 关闭 socket。
4. 清空 pending，并让等待者收到 `ClientClosed`。
5. blocking 等 reader thread 退出。
6. async 停止 actor 和 reader task。

`Drop`：

- blocking `Client` 可以 best-effort 调用 close signal。
- async `Client` 的 Drop 不能 await，只发送 close signal，不保证完成。
- 业务锁释放不能依赖 Drop。

## 19. 兼容性边界

必须与 Java 保持一致：

- key/lockId 16 字节归一化。
- requestId/lockId/clientId 生成布局。
- 64 字节命令头 offset。
- lock data 长度前缀和 stage/type packing。
- timeout/expired 高 16 位 flag。
- response requestId 匹配。
- replset leader 优先和 pending retry 语义。

可以有 Rust 化差异：

- 异步接口使用 `async/await`，不做 Java callback executor。
- blocking 和 async 使用不同 transport 实现。
- 高级 API 用 `Result<T, SlockError>`，不用异常层级。
- async guard 不承诺 Drop 自动 release。

## 20. 测试设计

### 20.1 协议单测

- Init/Ping/Lock 请求编码长度为 64。
- LockCommand 每个 offset 精确匹配 Java。
- LockData set/unset/incr/append/shift/pipeline/push/pop 编码正确。
- LockResultData bytes/string/long/list/map 解码正确。
- key 归一化与 Java 兼容。
- result code 到 `SlockError` 映射正确。

### 20.2 transport 单测

使用 mock TCP server：

- open 先发送 Init。
- ping requestId 能匹配响应。
- extra data 响应能读取完整 payload。
- 短读/断线会触发错误和 pending 清理。
- command timeout 后 pending map 清理。

### 20.3 blocking 集成测试

需要本地 `slock`：

- lock acquire/release。
- lock data set/incr/append/pipeline。
- event default set/clear/wait 两种模式。
- semaphore 并发限制。
- read/write lock。
- reentrant lock。
- flow。

### 20.4 async 集成测试

与 blocking 覆盖相同语义，额外验证：

- 多 task 并发 acquire。
- timeout future cancellation 后 actor pending 清理。
- close 后所有 pending await 返回 `ClientClosed`。

### 20.5 replset 集成测试

- leader 节点优先。
- leader 断开后 fallback。
- 新 leader 出现后 pending 被唤醒。
- `STATE_ERROR` lock result 触发 retry。

### 20.6 Java `ClientTest.java` 对等迁移测试

除基础协议单测、mock transport 单测和 Rust 自身集成测试外，必须完整迁移 Java 项目中的功能测试：

```text
D:\workspace\github\jaslock\src\test\java\io\github\snower\jaslock\ClientTest.java
```

该文件是 Java driver 对每个同步原语行为的回归测试集。Rust 实现需要为其中每个 `@Test` 建立对应测试，确保 Java driver 与 Rust driver 在同一 `slock` 服务上的功能结果一致。默认测试服务地址与 Java 保持一致：`127.0.0.1:5658`；Rust 测试允许通过环境变量覆盖，例如 `SLOCK_TEST_HOST`、`SLOCK_TEST_PORT`、`SLOCK_REPLSET_NODES`。

建议测试文件布局：

```text
tests/
  java_parity_blocking.rs
  java_parity_async.rs
  java_parity_replset.rs
  java_parity_lock_data.rs
  java_parity_flow_tree.rs
  java_parity_benchmark.rs
```

迁移映射：

| Java 测试 | Rust 测试目标 | 覆盖要求 |
| --- | --- | --- |
| `testClientLock` | `blocking_client_lock_parity` | 单节点 lock acquire/release、guard、count=10、多 lock 数据传递顺序 |
| `testReplsetClientLock` | `blocking_replset_lock_parity` | replset facade 与单节点 lock 行为一致 |
| `testClientAsyncLock` | `async_client_lock_parity` | Java callback lock 行为迁移为 Rust `async/await` acquire/release |
| `testReplsetClientAsyncLock` | `async_replset_lock_parity` | replset async lock acquire/release |
| `testEventDefaultSeted` | `blocking_event_default_set_parity` | `default_set=true` 的 is_set/clear/set/wait/data |
| `testEventDefaultUnseted` | `blocking_event_default_unset_parity` | `default_set=false` 的 is_set/set/clear/wait/data |
| `testEventAsyncDefaultSeted` | `async_event_default_set_parity` | async event wait 和 data 回填 |
| `testEventAsyncDefaultUnseted` | `async_event_default_unset_parity` | async event future 语义迁移为 await |
| `testGroupEvent` | `blocking_group_event_parity` | versionId 更新、wakeup payload、wait data、set/is_set |
| `testGroupEventAsync` | `async_group_event_parity` | async group event version 与 data 行为 |
| `testReadWriteLock` | `blocking_read_write_lock_parity` | 多读共享、读阻塞写、写阻塞读、timeout error |
| `testReadWriteLockAsync` | `async_read_write_lock_parity` | async read/write lock timeout error 映射 |
| `testReentrantLock` | `blocking_reentrant_lock_parity` | 10 次重入 acquire/release，释放后普通 lock 可用 |
| `testReentrantLockAsync` | `async_reentrant_lock_parity` | async 重入行为 |
| `testSemaphore` | `blocking_semaphore_parity` | 10 个许可、额外 acquire timeout、release 后恢复 |
| `testSemaphoreAsync` | `async_semaphore_parity` | async semaphore 限流与 timeout |
| `testTreeLock` | `blocking_tree_lock_parity` | root/child/leaf lock、递归 child check、tree unlock flag |
| `testMaxConcurrentFlow` | `blocking_max_concurrent_flow_parity` | 最大并发数、timeout、毫秒 expired flag 自动释放 |
| `testMaxConcurrentFlowAsync` | `async_max_concurrent_flow_parity` | async acquire/release 基础行为 |
| `testTokenBucketFlow` | `blocking_token_bucket_flow_parity` | period < 3 秒毫秒过期、批量 acquire 后恢复 |
| `testTokenBucketFlowAsync` | `async_token_bucket_flow_parity` | async token acquire 基础行为 |
| `testPriorityLock` | `async_priority_lock_parity` | 1000 个随机 priority 请求，完成顺序按 priority 非递减 |
| `testLockData` | `blocking_lock_data_parity` | set/incr/append/shift/execute/pipeline/push/pop 及 data 解码 |
| `testBenchmark` | `benchmark_parity_ignored_by_default` | 迁移为 `#[ignore]` 性能/压力测试，默认 CI 不运行 |

迁移原则：

- Java 中同步测试迁移为 blocking integration test。
- Java 中 callback/future 测试迁移为 async integration test，用 `.await` 表达同一行为。
- Java `Assert` 中的具体值必须原样保留，包括 `"aaa"`, `"bbb"`, `"ccc"`、versionId `2/3`、semaphore 数量 `10`、priority 顺序、LockData list 长度和内容。
- Java 期望抛出的 `LockTimeoutException`, `LockUnlockedException` 等，在 Rust 中必须断言为对应 `SlockError` variant。
- `testBenchmark` 也要迁移，但标记 `#[ignore]` 或 feature-gated，避免常规 CI 运行百万次压力请求。
- 这些 parity 测试是 M4/M5 完成的验收门槛；缺任一功能测试时，不得声称同步原语与 Java driver 行为一致。

## 21. 实现里程碑

### M1: 协议和数据层

- constants。
- Id16/Key16/PackedTime。
- Init/Ping/Lock command encode。
- response decode。
- LockData/LockResultData。
- 协议单测。

### M2: blocking 单节点

- Client open/init/close。
- reader thread。
- send/write/ping。
- blocking Database 和 Lock。
- mock server 测试。

### M3: async 单节点

- ConnectionActor。
- async Client open/close/send/ping。
- async Database 和 Lock。
- cancellation 和 timeout 测试。

### M4: 高级同步原语

- Event。
- Semaphore。
- ReentrantLock。
- ReadWriteLock。
- PriorityLock。
- MaxConcurrentFlow。
- TokenBucketFlow。
- GroupEvent。
- TreeLock。

### M5: replset

- blocking ReplsetClient。
- async ReplsetClient。
- leader 状态更新。
- pending retry。
- replset 集成测试。

### M6: API 收尾

- README 示例。
- rustdoc。
- feature 文档。
- 与 Java 行为差异说明。

## 22. 文件生成建议

第一轮代码落地时推荐先创建：

```text
Cargo.toml
src/lib.rs
src/error.rs
src/options.rs
src/time.rs
src/key.rs
src/protocol/constants.rs
src/protocol/id.rs
src/protocol/command.rs
src/protocol/result.rs
src/protocol/codec.rs
src/data/mod.rs
```

先让协议单测通过，再进入 blocking/async transport。这样可以把最容易出错的二进制兼容问题压到最小范围内解决。

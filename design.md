# ruslock Rust Driver Design

> 2026-05-19 新增硬性约束：同一调用模型内，`Client` 和 `ReplsetClient` 必须实现相同抽象接口，并且业务代码能在单 IP 和多 IP 部署之间无修改切换。构造阶段可以根据程序参数选择单节点或 replset，但构造完成后必须通过同一套 client/database/primitive API 使用。

> 2026-05-20 新增连接生命周期约束：`Client::open()` 必须在首次 TCP connect + Init 成功后才启动 reader 线程或 task；reader 负责断线后的自动重连，复用同一 `clientId` 并刷新 `init_type`。除非主动 `close()` 或 `auto_reconnect=false`，否则持续按 `ClientOptions::reconnect_interval` 重试。单节点 client 不静默重发已写失败或断线中的业务命令，replset 仍由自身 pending/retry 层负责重发语义。

> 2026-05-21 新增 callback/Sans-IO 集成约束：新增无 TCP 所有权的 callback API，client 只暴露分权的 `ReaderBuffer` 和 `WriterBuffer`，命令只写入 `writer_buffer`，响应解析只从 `reader_buffer` 读取。调用方负责创建 TCP、发送 writer buffer、接收数据并写入 reader buffer，再调用 `handle_init` / `handle_read` / `handle_disconnect` 驱动状态机；lock、event 等业务结果全部通过 callback 回传。callback 断线使用 `ClientDisconnected`，请求句柄支持取消，`current_data` 返回快照，extra data 保留 Java `len + 4` 布局，deadline 使用 `encoded.timeout + command_timeout_grace`，重连复用同一 `clientId`。

本文是在 `docs/Architecture.md` 的协议和 Java 实现梳理基础上，面向 `ruslock` 的完整 Rust 库设计。目标是实现一个兼容 `slock` 二进制协议的 Rust driver，并同时提供普通 blocking 同步调用接口、`async/await` 异步调用接口，以及可接入 Python asyncio 等第三方调度器的 callback/Sans-IO 接口。

## 1. 设计目标

1. 协议兼容：严格复刻 `jaslock` 的 64 字节命令头、LockData 编码、requestId 匹配、Init/Ping/Lock/Unlock 行为。
2. 多调用模型：同一 crate 同时提供 `ruslock::blocking`、`ruslock::aio` 和 `ruslock::callback` 三套 API。
3. 共享核心逻辑：协议、数据编码、错误映射、key 归一化、同步原语的命令构造逻辑只实现一份。
4. 传输层隔离：blocking 使用 `std::net::TcpStream` 和线程；async 使用 `tokio::net::TcpStream` 和 task；callback 不创建 TCP，只维护 reader/writer buffer 和 request 状态。同步接口不依赖嵌套 tokio runtime。
5. 高级 API 完整：覆盖 `Lock`, `Event`, `GroupEvent`, `Semaphore`, `ReentrantLock`, `ReadWriteLock`, `PriorityLock`, `MaxConcurrentFlow`, `TokenBucketFlow`, `TreeLock`。
6. 可测试：协议编解码可脱离网络单测，传输和 API 可用本地 `slock` 做集成测试。

非目标：

- 不实现 `slock` server。
- 第一版不设计 TLS、认证和连接池，除非后续协议明确需要。
- 不逐字迁移 Java callback executor 线程池；Rust callback API 是 Sans-IO 状态机，不拥有网络连接、线程或 runtime。

## 2. 方案选择

### 2.1 候选方案

| 方案 | 描述 | 优点 | 缺点 |
| --- | --- | --- | --- |
| A. async-first + blocking `block_on` | 全部底层逻辑用 tokio，blocking API 内部创建 runtime 执行 async 方法 | 代码量少 | 在已有 runtime 中调用 blocking API 容易 panic 或死锁；blocking 用户也被迫引入 tokio 行为 |
| B. 共享协议 + 双传输层 | 协议/数据/原语状态共享，blocking 和 async 分别实现 transport | 同步和异步语义干净；运行时依赖清晰 | 传输层代码量略多 |
| C. 完全泛型 transport trait | 用统一 trait 抽象同步/异步 transport，所有上层泛型化 | 理论复用最大 | Rust 中同步/异步 trait 边界复杂，API 会变重 |
| D. 共享协议 + blocking/aio/callback 三传输 facade | 保留 blocking/aio 的 TCP 所有权，同时新增 callback/Sans-IO facade | 可直接接入 Python asyncio、mio、嵌入式调度器等外部 IO 循环；不引入 tokio 依赖 | 需要单独维护 pending callback 和 buffer 状态机 |

推荐方案：D，作为 B 的扩展。

理由：这个 driver 面向网络 IO，blocking、async 和外部调度器集成的调度模型差异很大。共享协议层和同步原语命令构造逻辑即可避免核心重复；传输层分开实现反而更清楚，也能保证 blocking 用户不需要关心 tokio runtime，callback 用户不需要让 ruslock 拥有 TCP socket。

### 2.2 对外模块

```rust
use ruslock::blocking;
use ruslock::aio;
use ruslock::callback;
```

三套 API 命名尽量一致，其中 callback API 使用回调回传业务结果：

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

// callback / Sans-IO
let client = ruslock::callback::Client::new();
let mut lock = client.lock("order:1001", 5, 5);
lock.acquire(|result| {
    // Result<LockCommandResult>
})?;
// caller drains client.writer_buffer() and sends bytes through its own socket
```

### 2.3 Client / ReplsetClient 可替换抽象

`Client` 和 `ReplsetClient` 不能只是“方法名相似”，而是必须在 public API 层可替换：

1. blocking 与 async 分别定义自己的公共抽象，避免把同步和异步方法强行合并到同一个 trait。
2. `blocking::Client`、`blocking::ReplsetClient`、`blocking::ClientHandle` 都实现同一 `blocking::ClientApi`。
3. `aio::Client`、`aio::ReplsetClient`、`aio::ClientHandle` 都实现同一 `aio::ClientApi`。
4. 业务代码依赖 `ClientApi` 或 `ClientHandle`，不依赖单节点/replset 具体类型。
5. `select_database`、`lock`、`event`、`semaphore`、flow、tree lock 等工厂方法必须返回同一套 public facade 类型；不向业务层暴露 `ReplsetLock` 这类行为不同的 primitive 类型。
6. replset 的 leader 选择、write failure retry、`STATE_ERROR` retry、pending wakeup 只存在于底层 command sender，不改变上层 primitive 的调用方式。

blocking 推荐形态：

```rust
pub trait ClientApi: Clone + Send + Sync + 'static {
    fn open(&self) -> Result<()>;
    fn close(&self) -> Result<()>;
    fn ping(&self) -> Result<()>;
    fn select_database(&self, db_id: u8) -> Database;
    fn lock<K: Into<Key16>>(&self, key: K, timeout: u32, expired: u32) -> Lock;
}

pub enum ClientHandle {
    Single(Client),
    Replset(ReplsetClient),
}

impl ClientApi for ClientHandle { /* enum dispatch */ }
```

async 推荐形态：

```rust
use core::future::Future;

pub trait ClientApi: Clone + Send + Sync + 'static {
    type OpenFuture<'a>: Future<Output = Result<()>> + Send + 'a where Self: 'a;
    type CloseFuture<'a>: Future<Output = Result<()>> + Send + 'a where Self: 'a;
    type PingFuture<'a>: Future<Output = Result<()>> + Send + 'a where Self: 'a;

    fn open(&self) -> Self::OpenFuture<'_>;
    fn close(&self) -> Self::CloseFuture<'_>;
    fn ping(&self) -> Self::PingFuture<'_>;
    fn select_database(&self, db_id: u8) -> Database;
    fn lock<K: Into<Key16>>(&self, key: K, timeout: u32, expired: u32) -> Lock;
}

pub enum ClientHandle {
    Single(Client),
    Replset(ReplsetClient),
}
```

运行时根据参数选择部署形态时，推荐返回 `ClientHandle`：

```rust
let client = ruslock::blocking::ClientHandle::connect(nodes)?;
let mut lock = client.lock("order:1001", 5, 5);
lock.acquire()?;
```

如果调用方更喜欢泛型，也可以写成：

```rust
fn run<C: ruslock::blocking::ClientApi>(client: &C) -> ruslock::Result<()> {
    let mut lock = client.lock("order:1001", 5, 5);
    lock.acquire()?;
    lock.release()
}
```

### 2.4 callback / Sans-IO 外部调度接口

callback API 的目标不是替代 `aio`，而是把 ruslock 变成可被第三方调度器驱动的 Sans-IO 协议状态机。典型调用方包括 Python asyncio、mio、自研 reactor、嵌入式 runtime 或 FFI 外层。

硬性约束：

1. `callback::Client` 不创建 TCP 连接，不持有 socket，不启动线程；`default-features=false` 时不依赖 tokio 或 `socket2`。
2. client 只暴露 `ReaderBuffer` 和 `WriterBuffer` 分权句柄；业务命令只追加到 `writer_buffer`，协议解析只消费 `reader_buffer`。
3. 调用方负责把 `writer_buffer` 中的数据发送到真实连接，并把连接收到的 bytes 写入 `reader_buffer`。
4. `handle_init()`、`handle_read()`、`handle_disconnect()` 只推进本地协议状态机，不能直接做网络 IO。
5. `lock.acquire`、`event.wait`、`semaphore.acquire` 等操作不直接返回业务结果，只注册 callback；收到响应后由 `handle_read()` 触发 callback。
6. callback 触发前必须先从 pending map 移除 requestId，并完成 primitive 本地状态更新，避免 callback 内再次发起请求时发生重入死锁。
7. 连接断开后 `handle_disconnect()` 负责以 `ClientDisconnected` fail 当前 pending callback，并根据 `auto_reconnect` 和 closed 状态返回是否建议调用方重连。

推荐流程：

```text
caller creates callback::Client
caller gets ReaderBuffer and WriterBuffer
caller calls handle_init()
caller drains WriterBuffer and sends Init bytes
caller receives Init response bytes and pushes into ReaderBuffer
caller calls handle_init() again until it returns true
business method writes command bytes into writer_buffer and registers callback
caller drains WriterBuffer and sends command bytes
caller receives response bytes and pushes into ReaderBuffer
caller calls handle_read(), which parses frames and invokes callbacks
caller detects socket close and calls handle_disconnect()
if handle_disconnect() returns true, caller creates a new socket and repeats init
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
    api.rs
    handle.rs
    client.rs
    connection.rs
    replset.rs
    database.rs
    primitives.rs
  aio/
    mod.rs
    api.rs
    handle.rs
    client.rs
    connection.rs
    replset.rs
    database.rs
    primitives.rs
  callback/
    mod.rs
    buffer.rs
    client.rs
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
| `blocking::ClientApi` | 同步 client 抽象接口，单节点、replset、统一 handle 都必须实现 |
| `blocking::ClientHandle` | 运行时可替换 facade，根据节点数量持有 `Client` 或 `ReplsetClient` |
| `blocking::Client` | 单节点同步 client facade，实现 `ClientApi` |
| `blocking::Connection` | `std::net::TcpStream`、reader supervisor thread、writer mutex、pending map、自动重连 |
| `blocking::ReplsetClient` | 多节点同步 client、leader 选择和 pending 重发，实现 `ClientApi` |
| `blocking::Database` | 同步 database facade |
| `blocking::primitives` | 同步 `Lock/Event/...` 包装器 |

### 3.3 aio 模块职责

| 模块 | 职责 |
| --- | --- |
| `aio::ClientApi` | 异步 client 抽象接口，单节点、replset、统一 handle 都必须实现 |
| `aio::ClientHandle` | 运行时可替换 facade，根据节点数量持有 `Client` 或 `ReplsetClient` |
| `aio::Client` | 单节点异步 client facade，实现 `ClientApi` |
| `aio::Connection` | tokio reader supervisor task，管理连接、读写、pending map、自动重连 |
| `aio::ReplsetClient` | 多节点异步 client、leader 选择和 pending 重发，实现 `ClientApi` |
| `aio::Database` | 异步 database facade |
| `aio::primitives` | 异步 `Lock/Event/...` 包装器 |

### 3.4 callback 模块职责

| 模块 | 职责 |
| --- | --- |
| `callback::Client` | Sans-IO client facade，管理 init/read/disconnect 状态机、pending callback 和 reader/writer buffer |
| `callback::ReaderBuffer` | 调用方只可 `push/clear` 的接收缓冲；保存半包和粘包响应，解析消费由 client 内部完成 |
| `callback::WriterBuffer` | 调用方只可 `drain/drain_into/clear` 的发送缓冲；保存待发送 Init/业务命令帧 |
| `callback::SharedBuffer` | 内部共享缓冲实现，不作为对称 public IO 能力暴露，避免调用方误 drain reader 或向 writer 手工 push |
| `callback::Database` | callback database facade，db id 和默认 flag 管理 |
| `callback::primitives` | callback `Lock/Event/...` 包装器，业务操作注册 callback 并写入 writer buffer |

## 4. Cargo feature 设计

```toml
[features]
default = ["blocking", "aio"]
blocking = ["dep:socket2"]
aio = ["dep:tokio"]

[dependencies]
bitflags = "2"
md-5 = "0.10"
rand = "0.8"
socket2 = { version = "0.5", optional = true }
thiserror = "1"
tokio = { version = "1", optional = true, features = ["net", "sync", "time", "rt", "macros", "io-util"] }
```

说明：

- 默认同时启用 blocking 和 aio，满足“双接口”要求。
- `aio` 仅在异步接口启用时引入 tokio。
- replset 不是独立 feature：`blocking::ReplsetClient` 随 `blocking` 编译，`aio::ReplsetClient` 随 `aio` 编译，避免业务在单节点和多节点部署之间切换时还需要额外 Cargo feature。
- callback 不是独立 feature，默认始终编译；它只依赖共享协议/数据/primitive 逻辑，在 `default-features=false` 时不引入 tokio、`socket2` 或 TCP 拥有权依赖。
- 协议层不依赖 tokio。
- `socket2` 仅随 `blocking` feature 引入，用于跨平台设置 TCP keepalive。
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

    #[error("client disconnected")]
    ClientDisconnected,

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

`Lock.acquire/release` 将 `LockCommandResult.result` 转换为上面的 lock 专用错误。底层 `send_command` 可以返回原始 `CommandResult`，高级 API 负责业务错误映射。`ClientDisconnected` 专门表示已经开始使用的连接意外断开，区别于从未完成 init 的 `NotConnected` 和用户主动 `close()` 后的 `ClientClosed`。

### 5.2 ClientOptions

```rust
pub struct ClientOptions {
    pub connect_timeout: Duration,
    pub reconnect_interval: Duration,
    pub command_timeout_grace: Duration,
    pub max_frame_size: usize,
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
| max frame size | 16 MiB |
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
pub(crate) struct Connection {
    address: String,
    options: ClientOptions,
    client_id: Mutex<Option<Id16>>,
    init_type: Arc<Mutex<u8>>,
    writer: Arc<Mutex<Option<TcpStream>>>,
    pending: Arc<Mutex<HashMap<Id16, SyncPending>>>,
    closed: Arc<AtomicBool>,
    reader_running: Arc<AtomicBool>,
}

struct SyncPending {
    command_type: CommandType,
    deadline: Instant,
    tx: std::sync::mpsc::Sender<Result<CommandResult>>,
}
```

### 9.2 open

`Client::open()`：

1. 如果 reader supervisor 已运行，直接返回。
2. 清除 closed 标记。
3. 建立 `TcpStream`。
4. 设置 `nodelay/keepalive`。
5. 写 InitCommand，读取 InitCommandResult。
6. 校验 init result，并记录返回的 `init_type`。
7. 保存 writer。
8. 在首次连接和 Init 成功后启动 reader supervisor thread。

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

reader supervisor thread 循环：

1. 从当前 reader stream 执行 `read_exact(64)`。
2. `protocol::decode_header()`。
3. 如果是 lock result 且有 data，继续读 4 字节长度和 payload。
4. 根据 requestId 从 pending map 移除 waiter。
5. 发送 result 给 waiter。
6. 读失败时：
   - 关闭 writer。
   - 唤醒当前 pending，使调用方得到 IO/NotConnected 类错误。
   - 如果 `auto_reconnect` 且 client 未 close，进入 reconnect loop。
   - reconnect 成功后保存新 writer、刷新 `init_type`，并继续读新 stream。

### 9.5 reconnect

单节点 client 的 reconnect 策略：

- 每 `reconnect_interval` 重试。
- reconnect 成功后使用同一 `clientId` 发送 init。
- Init 成功后刷新本地 `init_type`，使 leader 等连接状态与服务端最新返回保持一致。
- `auto_reconnect=false` 或 `close()` 后停止 supervisor，不再继续重连。
- 对单节点未完成请求，默认返回 IO/`NotConnected` 或 `ClientClosed`，不自动重发，避免重复执行风险。
- 对 replset 子 client，未完成请求交给 replset pending 逻辑判断是否重发。

## 10. async 传输逻辑

### 10.1 内部结构

```rust
pub(crate) struct Connection {
    address: String,
    options: ClientOptions,
    client_id: Mutex<Option<Id16>>,
    init_type: Arc<Mutex<u8>>,
    writer: Arc<AsyncMutex<Option<OwnedWriteHalf>>>,
    pending: Arc<AsyncMutex<HashMap<Id16, PendingSender>>>,
    closed: Arc<AtomicBool>,
    reader_running: Arc<AtomicBool>,
}
```

### 10.2 Connection reader supervisor

Async client 使用 `aio::Connection` 和 reader supervisor task 管理网络状态：

```text
Client facade
  -> aio::Connection
     - current writer
     - pending HashMap<Id16, PendingSender>
     - reader supervisor task
     - reconnect timer
```

async 连接生命周期：

1. `open().await` 先执行 `connect_once()`，完成 TCP connect + Init。
2. Init 成功后保存 writer、刷新 `init_type`。
3. 只有首次连接成功后才 spawn reader supervisor task。
4. reader task 持有当前 reader stream，循环解码响应并按 requestId 唤醒 pending。
5. reader 失败时关闭 writer、唤醒 pending；如果 `auto_reconnect` 且未 close，按 `reconnect_interval` 重试。
6. reconnect 成功后继续使用同一 `clientId` Init，保存新 writer 并继续读取。
7. `close().await` 标记 closed，关闭 writer，停止后续重连，并唤醒所有 pending。

### 10.3 send_command

异步 `send_command`：

1. facade 创建 `oneshot`。
2. connection 编码 command，并在写入前插入 pending。
3. connection 锁定 writer，写入 header + extra 并 flush。
4. facade `await` oneshot，并叠加 `tokio::time::timeout`。
5. 超时或 future 被 drop 时通过 pending cleanup 按 requestId 删除 pending。

connection 的 pending cleanup 必须避免 facade 被 drop 后 pending 泄漏。

### 10.4 async close

`Client::close().await`：

- 标记 closed。
- 关闭 writer/socket。
- 对全部 pending 返回 `ClientClosed`。
- 停止 reader supervisor task，且不再触发重连。

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

`ReplsetClient` 对外必须与 `Client` 完全可替换，而不是只提供一组同名方法。两者都实现同一 `ClientApi`，并且通过 `ClientHandle` 支持运行时选择：

```rust
let client = ruslock::blocking::ClientHandle::connect(["127.0.0.1:5658", "127.0.0.1:5659"])?;
let mut lock = client.lock("k", 5, 5);
lock.acquire()?;
```

异步：

```rust
let client = ruslock::aio::ClientHandle::connect(["127.0.0.1:5658"]).await?;
```

具体要求：

- `Client` 和 `ReplsetClient` 的业务入口由 `ClientApi` 约束。
- `ClientHandle::connect(nodes)` 负责按节点数量选择单节点或 replset backend。
- `Database` 和所有 primitive facade 类型保持一致；replset 不暴露独立的 `ReplsetLock` / `ReplsetEvent` 业务类型。
- `ReplsetClient` 的 retry、leader 切换和 pending wakeup 必须封装在发送命令层，primitive 逻辑只感知普通 `LockCommandResult`。

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

## 13A. callback / Sans-IO API 设计

### 13A.1 设计定位

`ruslock::callback` 是第三套 public facade。它与 `blocking` / `aio` 共享协议编解码、LockData、错误映射和 primitive 命令构造，但不拥有任何真实 IO。调用方可以把它嵌入 Python asyncio、mio、自研事件循环或 FFI 外层，由外部调度器负责 TCP connect/read/write。

与 `aio` 的区别：

- `aio::Client` 拥有 tokio TCP 连接，方法通过 `.await` 返回结果。
- `callback::Client` 不拥有连接，方法只写入 `writer_buffer` 并注册 callback；结果由调用方写入 `reader_buffer` 后调用 `handle_read()` 触发。
- callback API 不启动后台任务，所以 timeout 也需要调用方通过 `next_deadline()` / `handle_timeout(now)` 驱动。

### 13A.2 内部结构

```rust
pub struct Client {
    inner: Arc<ClientInner>,
}

struct ClientInner {
    options: ClientOptions,
    client_id: Mutex<Option<Id16>>,
    init_type: AtomicU8,
    state: Mutex<CallbackState>,
    reader_buffer: SharedBuffer,
    writer_buffer: SharedBuffer,
    pending: Mutex<HashMap<Id16, PendingCallback>>,
    cancelled: Mutex<HashSet<Id16>>,
    closed: AtomicBool,
}

enum CallbackState {
    New,
    InitSent,
    Inited,
    Disconnected,
    Closed,
}

struct PendingCallback {
    command_type: CommandType,
    deadline: Instant,
    complete: Box<dyn FnOnce(Result<CommandResult>) + Send + 'static>,
}
```

buffer 必须支持半包和粘包，但 public API 要按方向分权：

```rust
struct SharedBuffer { /* Arc<Mutex<VecDeque<u8>>> 或 BytesMut */ }

pub struct ReaderBuffer { inner: SharedBuffer }
pub struct WriterBuffer { inner: SharedBuffer }

impl ReaderBuffer {
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
    pub fn push(&self, bytes: &[u8]);
    pub fn clear(&self);
}

impl WriterBuffer {
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
    pub fn drain(&self) -> Vec<u8>;
    pub fn drain_into(&self, out: &mut Vec<u8>);
    pub fn clear(&self);
}
```

`ReaderBuffer` 不暴露 `drain`，避免调用方误删未解析半包；`WriterBuffer` 不暴露 `push`，避免调用方把非 ruslock 协议字节混入待发送帧。client 内部通过 `SharedBuffer` 的私有方法消费 reader、追加 writer。

### 13A.3 Client API

Rust public API 使用 snake_case；用户描述中的 `handlerInit` 对应 `handle_init()`。

```rust
impl Client {
    pub fn new() -> Self;
    pub fn with_options(options: ClientOptions) -> Self;

    pub fn reader_buffer(&self) -> ReaderBuffer;
    pub fn writer_buffer(&self) -> WriterBuffer;

    pub fn handle_init(&self) -> Result<bool>;
    pub fn handle_read(&self) -> Result<usize>;
    pub fn handle_disconnect(&self) -> Result<bool>;
    pub fn handle_timeout(&self, now: Instant) -> Result<usize>;
    pub fn next_deadline(&self) -> Option<Instant>;
    pub fn cancel_request(&self, request_id: Id16) -> Result<bool>;
    pub fn is_inited(&self) -> bool;
    pub fn init_type(&self) -> u8;
    pub fn pending_len(&self) -> usize;
    pub fn close(&self) -> Result<()>;

    pub fn ping<F>(&self, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<PingCommandResult>) + Send + 'static;

    pub fn select_database(&self, db_id: u8) -> Database;
    pub fn lock<K: AsRef<[u8]>>(&self, key: K, timeout: u16, expired: u16) -> Lock;
}
```

返回语义：

| 方法 | 返回 |
| --- | --- |
| `handle_init()` | `Ok(false)` 表示已写 Init 或等待更多 bytes；`Ok(true)` 表示 Init 完成 |
| `handle_read()` | 返回本次解析并触发 callback 的响应数量；半包保留在 reader buffer |
| `handle_disconnect()` | fail 当前 pending；返回 `true` 表示调用方应重连，`false` 表示已 close 或禁止 auto reconnect |
| `handle_timeout(now)` | 触发已超时 callback，返回超时数量 |

### 13A.4 Init 状态机

`handle_init()` 必须可重复调用：

1. `New`：
   - 生成或复用 `clientId`。
   - 编码 `InitCommand` 写入 `writer_buffer`。
   - 状态变为 `InitSent`。
   - 返回 `Ok(false)`。
2. `InitSent`：
   - 从 `reader_buffer` 尝试读取 64 字节 Init response。
   - 数据不足时保留 reader buffer，返回 `Ok(false)`。
   - 解码成功后校验 result，保存 `init_type`。
   - 状态变为 `Inited`，返回 `Ok(true)`。
3. `Inited`：
   - 幂等返回 `Ok(true)`。
4. `Disconnected`：
   - 清理旧 reader buffer，复用原 `clientId` 重新写 Init 到 writer buffer，进入 `InitSent`。
5. `Closed`：
   - 返回 `ClientClosed`。

调用方负责在每次 `handle_init()` 写出 bytes 后 drain `writer_buffer` 并发送。

### 13A.5 业务命令和 callback

callback primitive 的业务方法不等待响应：

```rust
impl Lock {
    pub fn acquire<F>(&self, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<LockCommandResult>) + Send + 'static;

    pub fn release<F>(&self, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<LockCommandResult>) + Send + 'static;
}

impl Event {
    pub fn wait<F>(&self, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<LockCommandResult>) + Send + 'static;
}

pub struct RequestHandle {
    request_id: Id16,
    client: Weak<ClientInner>,
}

impl RequestHandle {
    pub fn request_id(&self) -> Id16;
    pub fn cancel(&self) -> Result<bool>;
}
```

callback primitive 的本地状态通过内部 `Arc<Mutex<LockState>>` 保存，`current_data()` 必须返回 `Option<LockResultData>` 快照 clone，而不是 `Option<&LockResultData>` 引用；这样调用方不会持有内部锁引用，也不会被 callback 并发更新破坏生命周期。

发送流程：

1. primitive 构造 `Command`。
2. client 编码 64 字节 header 和可选 extra data。
3. 在写入 `writer_buffer` 前计算 deadline 并把 `requestId` 插入 pending map；deadline 必须等于 `now + encoded.timeout.low_16_seconds + ClientOptions::command_timeout_grace`，`timeout=0` 时也保留 Java 的 grace 行为。
4. 将编码 bytes 追加到 `writer_buffer`。
5. 返回 `RequestHandle { request_id, client }`。
6. 调用方 drain `writer_buffer` 并通过自己的 socket 发送。

如果当前状态不是 `Inited`，业务命令返回 `NotConnected`，不写入 writer buffer，也不注册 pending callback。

取消语义：

- `RequestHandle::cancel()` 与 `Client::cancel_request(request_id)` 等价。
- 取消只移除 pending 并把 requestId 记录到 `cancelled` 集合；已经写入 `writer_buffer` 或已经由调用方发送到 socket 的 bytes 不会被回滚。
- 取消成功时不触发用户 callback；之后如果同一 requestId 的响应到达，`handle_read()` 静默忽略该响应并从 `cancelled` 集合移除。
- 取消不存在或已经完成的 requestId 返回 `Ok(false)`。

### 13A.6 响应解析和 callback 触发

`handle_read()` 循环解析 `reader_buffer`：

```text
while reader_buffer contains a complete frame:
    decode 64-byte response header
    if response has extra data:
        require 4-byte little-endian payload length + payload
        reject payload length > ClientOptions::max_frame_size as Protocol
        if not enough bytes, keep partial bytes and stop
        build LockResultData raw bytes as vec![0; payload_len + 4]
        copy payload into raw[4..], preserving Java read_extra_data layout
    remove pending by requestId
    map CommandResult to primitive-specific Result
    update primitive shared state
    invoke callback outside pending/state locks
```

callback 触发规则：

- 必须按响应到达顺序触发。
- requestId 如果存在于 `cancelled` 集合，移除该记录并忽略响应；其他未知 requestId 返回 `Protocol`，避免静默吞包。
- callback 内允许继续调用 client 发起新命令，因此 `handle_read()` 不能在持有 pending mutex 时调用用户 callback。
- `Lock.current_data()`、`Event.current_data()` 等本地状态必须在 callback 前完成更新。

### 13A.7 多命令 primitive continuation

部分高级 primitive 不是单个 request 即可完成，例如 `TreeLeafLock::acquire()` 需要先做 child/parent check，再 acquire leaf；部分 flow 也可能在内部重试或串行执行多个命令。callback facade 不能把这些操作拆成多个用户可见 callback，而要把它们建模成内部 continuation：

```rust
struct PendingCallback {
    command_type: CommandType,
    deadline: Instant,
    continuation: CallbackContinuation,
}

enum CallbackContinuation {
    Single(Box<dyn FnOnce(Result<CommandResult>) + Send + 'static>),
    PrimitiveStep(Box<dyn FnOnce(Result<CommandResult>, &Client) -> ContinuationAction + Send + 'static>),
}

enum ContinuationAction {
    Complete(Box<dyn FnOnce() + Send + 'static>),
    SendNext(EncodedCommand, PendingCallback),
}
```

规则：

- 一个 public primitive 方法只触发一次用户 callback，无论内部发送多少条命令。
- 任一步失败时立即完成用户 callback，后续步骤不再发送。
- 每个内部 request 仍使用自己的 requestId、pending 项和 deadline；整组操作的外部 `RequestHandle` 记录 root requestId，并在取消时取消当前活动 request。
- continuation 发送下一条命令时仍必须先插入 pending，再追加 `writer_buffer`，并且不能在持有 pending/state/buffer 锁时调用用户代码。

### 13A.8 disconnect、reconnect 和 timeout

`handle_disconnect()`：

- 清空 reader buffer 和 writer buffer。
- 将 state 置为 `Disconnected`，除非已经 `Closed`。
- drain pending map，并以 `ClientDisconnected` 触发 callback。
- 清空 `cancelled` 集合；下一次 `handle_init()` 复用同一 `clientId`。
- 如果 `options.auto_reconnect && !closed`，返回 `Ok(true)`；否则返回 `Ok(false)`。
- 不静默重放已经写出或 pending 中的业务命令，避免 lock/release 重复执行。

`handle_timeout(now)`：

- 遍历 pending map，找出 `deadline <= now` 的请求。
- 移除这些 pending。
- 按 command 类型映射为 `CommandTimeout` 或更具体的 wait timeout。
- 在锁外触发 callback。

`next_deadline()` 用于外部调度器注册 timer。callback API 没有后台线程，不会自动超时。

### 13A.9 与 blocking/aio/replset 的边界

- callback 第一版只设计单连接 client；不把 replset retry/pending queue 强行叠入 Sans-IO 层。
- 后续如需要 callback replset，应新增 `callback::ReplsetClient`，由调用方为每个 node 提供独立 reader/writer buffer 和连接生命周期事件。
- callback API 不实现 blocking wait，也不返回 Future；需要 Future 的外层可在 Python/Rust 调度器里用 callback 转换成 promise/oneshot。
- callback 与 blocking/aio 共享同一套 primitive 命令构造，避免 Lock/Event/Semaphore 行为分叉。

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
- 写 socket 由 `Mutex<Option<TcpStream>>` 串行化。
- 读 socket 由唯一 reader supervisor thread 负责，并负责断线重连。
- pending map 用 `Mutex<HashMap<Id16, SyncPending>>`。
- 每个高级 primitive 是否 `Send` 取决于内部是否保存可变 `current_data`；推荐 primitive 本身不实现内部锁，用户需要 `&mut self` 调用。

### 16.2 async

- `Client`, `Database` 可 clone，内部是 `Arc`。
- 写 socket 由 `tokio::sync::Mutex<Option<OwnedWriteHalf>>` 串行化。
- 读 socket 由唯一 reader supervisor task 负责，并负责断线重连。
- facade 方法直接调用 connection 并 await response。
- 高级 primitive 使用 `&mut self` 更新 `current_data`，避免额外 mutex。

### 16.3 callback

- `Client`, `Database`, `ReaderBuffer`, `WriterBuffer` 可 clone，内部是 `Arc`。
- `reader_buffer` 和 `writer_buffer` 使用内部锁保护，但 public 方法按方向分权：reader 只允许外部 push，writer 只允许外部 drain。
- `handle_read()`、`handle_init()`、`handle_disconnect()` 与业务发起方法可以被不同线程调用，但同一 client 内必须通过 state mutex 串行推进协议状态。
- callback 触发前必须释放 pending/state/buffer 锁，允许 callback 内发起新命令。
- callback primitive 的 `current_data` 等状态使用共享内部状态保存，不依赖 `&mut self`，并通过 clone 快照返回。

## 17. 超时策略

| 类型 | 策略 |
| --- | --- |
| connect timeout | `ClientOptions.connect_timeout` |
| command timeout | 普通命令 120 秒 grace |
| lock command timeout | `(timeout.value + grace)` 秒 |
| event wait timeout | 底层 lock timeout 映射为 `EventWaitTimeout` |
| replset pending timeout | 沿用原 command deadline，不因重试重置 |
| callback timeout | 不启动后台任务；pending deadline = `encoded.timeout.low_16_seconds + command_timeout_grace`，调用方按 `next_deadline()` 调度并调用 `handle_timeout(now)` |
| callback max frame | extra data payload length 超过 `ClientOptions.max_frame_size` 时返回 `Protocol`，默认 16 MiB |

`timeout.value = 0` 时仍有 120 秒 command grace，这是 Java 行为。后续可以提供 `ClientOptions` 覆盖 grace。

## 18. close 与资源释放

`close()` 必须保证：

1. 标记 closed。
2. 停止自动重连。
3. 对拥有 socket 的 transport 关闭 socket；callback transport 清空 reader/writer buffer。
4. 清空 pending 和 callback cancelled 集合，并让等待者收到 `ClientClosed`。
5. blocking 等 reader thread 退出。
6. async 停止 reader supervisor task。
7. callback 不触发网络 IO，只在本地 fail pending callback。

`Drop`：

- blocking `Client` 可以 best-effort 调用 close signal。
- async `Client` 的 Drop 不能 await，只发送 close signal，不保证完成。
- callback `Client` 的 Drop 不能可靠触发用户 callback，调用方应显式 `close()` 或 `handle_disconnect()`。
- 业务锁释放不能依赖 Drop。

## 19. 兼容性边界

必须与 Java 保持一致：

- key/lockId 16 字节归一化。
- requestId/lockId/clientId 生成布局。
- 64 字节命令头 offset。
- lock data 长度前缀和 stage/type packing。
- Lock response extra data 解码时保留 Java `payload_len + 4` raw 布局，payload 从 offset 4 开始。
- timeout/expired 高 16 位 flag。
- response requestId 匹配。
- replset leader 优先和 pending retry 语义。

可以有 Rust 化差异：

- 异步接口使用 `async/await`，不做 Java callback executor。
- callback 接口使用 Rust callback + Sans-IO buffer，不迁移 Java callback executor 线程池和内部 TCP 管理。
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
- reader thread/task 会在断线后自动重连，并在 `close()` 后停止重试。
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
- timeout future cancellation 后 connection pending 清理。
- close 后所有 pending await 返回 `ClientClosed`。

### 20.5 replset 集成测试

- leader 节点优先。
- leader 断开后 fallback。
- 新 leader 出现后 pending 被唤醒。
- `STATE_ERROR` lock result 触发 retry。

### 20.6 callback / Sans-IO 单测

callback 测试不需要真实 TCP server，只通过 reader/writer buffer 驱动：

- `ReaderBuffer` 只暴露 push/clear，`WriterBuffer` 只暴露 drain/drain_into/clear，clone 句柄共享同一底层缓冲。
- `handle_init()` 首次调用写出 64 字节 Init，返回 `false`。
- Init response 半包时返回 `false` 并保留 reader buffer；完整响应后返回 `true`，断线重连后再次 Init 复用同一 `clientId`。
- `ping(callback)` 写入 writer buffer 并按 requestId 触发 callback。
- acquire/release/event/semaphore 等业务方法只写 writer buffer 并注册 callback，不直接返回业务结果。
- `handle_read()` 能解析单个响应、半包、粘包、多响应，并按 requestId 触发正确 callback。
- Lock result 带 extra data 时，能等待完整长度前缀和 payload 后再触发 callback，并验证 `LockResultData.raw` 保留 Java `payload_len + 4` 布局。
- callback 触发前先更新 `current_data` 快照，且 callback 内可再次发起命令。
- `RequestHandle::cancel()` 和 `Client::cancel_request()` 会移除 pending，迟到响应被忽略且不触发 callback。
- 未知 requestId、非法 magic/version、非法 extra data 长度或超出 `max_frame_size` 返回 `SlockError::Protocol`。
- `handle_disconnect()` 会以 `ClientDisconnected` fail 当前 pending callback，清空 buffer，并按 `auto_reconnect` 返回是否需要重连。
- `handle_timeout(now)` 会按 `encoded.timeout + command_timeout_grace` 触发超时 callback 并清理 pending；`next_deadline()` 返回最近 deadline。
- 多命令 primitive continuation 成功或失败都只触发一次用户 callback，取消时不会继续发送后续命令。

### 20.7 Java `ClientTest.java` 对等迁移测试

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
- reader supervisor thread 和自动重连。
- send/write/ping。
- blocking Database 和 Lock。
- mock server 测试。

### M3: async 单节点

- async connection reader supervisor task 和自动重连。
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

### M5A: callback / Sans-IO

- callback Client reader/writer buffer。
- `handle_init` / `handle_read` / `handle_disconnect` / `handle_timeout` 状态机。
- callback Database 和 Lock/Event/Semaphore 等 primitive facade。
- 半包、粘包、多响应、extra data、disconnect、timeout 单测。
- 外部调度器接入示例。

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
src/callback/mod.rs
src/callback/buffer.rs
```

先让协议单测通过，再进入 blocking/async transport。callback 的 buffer 和 Sans-IO 状态机可以在协议层稳定后独立实现，这样可以把最容易出错的二进制兼容问题压到最小范围内解决。

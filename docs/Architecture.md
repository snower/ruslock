# ruslock Architecture Draft from jaslock

## Connection Auto-Reconnect Update (2026-05-20)

This section records the Java `SlockClient` reconnect behavior that the Rust
driver must preserve.

### Java behavior

`SlockClient.open()` first calls `connect()` and completes the Init handshake.
Only after that succeeds does it create the daemon IO thread named
`jaslock-io-{host}:{port}`. `tryOpen()` differs only in that it still starts
the IO thread after an initial connect failure, so the thread can keep trying
to reconnect in the background.

`SlockClient.run()` owns the long-lived read/reconnect lifecycle:

1. It loops while `closed == false`.
2. If the input stream is missing, it calls `reconnect()` and continues.
3. It reads exactly one 64-byte response header at a time, then reads optional
   Lock result extra data when the response flag says data is present.
4. It dispatches responses by `requestId`, not by send order.
5. Any read failure, short read, or parse-loop exception closes the current
   socket, sleeps for the reconnect interval, and re-enters the outer loop.

`reconnect()` first wakes/removes pending requests for the current single
client when there is no other lived replset node that can own them. It then
calls `connect()` repeatedly every 2 seconds until either a new Init handshake
succeeds or `close()` sets `closed = true`.

`close()` is the only normal stop signal. It sets `closed = true`, closes the
socket, wakes pending waiters, closes single-client databases, and stops the
callback executor. After this point no background reconnect attempt is allowed.

### Rust behavior

Both `ruslock::blocking::Connection` and `ruslock::aio::Connection` follow the
same lifecycle:

1. `open()` performs the first TCP connect and Init handshake synchronously
   with the caller. A reader thread/task is created only after that first
   handshake succeeds.
2. The reader thread/task acts as a connection supervisor. It reads responses
   until a read failure occurs, then removes the current writer, wakes pending
   commands with an IO error, and starts reconnect attempts.
3. Reconnect attempts reuse the same `clientId`, repeat the Init handshake, and
   update `init_type` from the new Init response. Attempts sleep for
   `ClientOptions::reconnect_interval` between failures.
4. `ClientOptions::auto_reconnect = false` disables reconnect after the first
   disconnect; the reader exits instead.
5. `close()` sets the shared closed flag, shuts down the current writer, wakes
   pending commands with `ClientClosed`, and prevents further reconnect loops.

New commands sent while the connection is between sockets may observe
`NotConnected` or a write IO error; they are not silently queued by the
single-node client. `ReplsetClient` owns the higher-level pending/retry queue
for commands that should wait for another node or a later reconnect.

## Callback / Sans-IO Update (2026-05-21)

`ruslock::callback` is a third facade for schedulers that must own TCP
themselves. It shares command encoding, requestId matching, LockData parsing,
and primitive command construction with the blocking and async facades, but it
does not open sockets, spawn reader threads, or create timers.

### Buffer ownership

The public buffers are split by capability:

| Buffer | Caller capability | Client capability |
| --- | --- | --- |
| `ReaderBuffer` | `push(bytes)`, `clear()` | consume parsed response frames |
| `WriterBuffer` | `drain()`, `drain_into(out)`, `clear()` | append Init and command frames |

The caller must never be able to drain `ReaderBuffer` or push arbitrary bytes
into `WriterBuffer`. This preserves half-packet state and prevents non-protocol
bytes from being mixed into outgoing frames.

### Init and command flow

1. Caller creates `callback::Client`.
2. Caller gets `ReaderBuffer` and `WriterBuffer`.
3. Caller calls `handle_init()`.
4. Client writes one 64-byte Init frame into `WriterBuffer` and returns
   `Ok(false)`.
5. Caller drains `WriterBuffer`, sends those bytes through its own socket, and
   pushes received bytes into `ReaderBuffer`.
6. Caller calls `handle_init()` again until it returns `Ok(true)`.
7. Business methods such as `lock.acquire(callback)` encode commands, insert
   pending callbacks by requestId, append frames to `WriterBuffer`, and return
   `RequestHandle`.
8. Caller sends drained command bytes, receives response bytes, pushes them into
   `ReaderBuffer`, and calls `handle_read()`.

`handle_read()` parses all complete frames in arrival order. User callbacks are
invoked only after the pending entry has been removed and primitive local state
has been updated, so callbacks may issue another command on the same client.

### Extra data framing

Lock result extra data follows the Java layout exactly:

1. Read the 4-byte little-endian payload length.
2. Reject lengths greater than `ClientOptions::max_frame_size`.
3. Wait until the full payload is available.
4. Build `LockResultData` raw bytes with length `payload_len + 4`, leave
   `raw[0..4]` as zeroes, and copy payload into `raw[4..]`.

This keeps Java's `stage/type` at `raw[4]` and `commandFlag` at `raw[5]`.

### Disconnect, timeout, and cancellation

- `handle_disconnect()` clears both buffers, fails all pending callbacks with
  `ClientDisconnected`, clears cancelled request ids, moves to `Disconnected`,
  and returns whether the caller should reconnect. Re-init after disconnect
  reuses the same `clientId`.
- `next_deadline()` returns the earliest pending command deadline. The deadline
  is `encoded.timeout.low_16_seconds + ClientOptions::command_timeout_grace`,
  including timeout `0`.
- `handle_timeout(now)` removes expired pending callbacks and completes them
  with `CommandTimeout` or primitive-specific mapping.
- `RequestHandle::cancel()` removes the current pending request and records its
  requestId as cancelled. Already written bytes are not retracted; a late
  response for a cancelled id is ignored.

本文基于 `D:\workspace\github\jaslock` 当前 Java 实现整理，目标是为 `slock` 的 Rust driver 提供实现蓝图。重点覆盖命令实现、协议编解码、连接管理、database 管理和 API 接口实现。

## 1. 源码分层

`jaslock` 的核心代码在 `src/main/java/io/github/snower/jaslock`：

| Java 包/类 | 职责 | Rust 建议模块 |
| --- | --- | --- |
| `commands` | 二进制协议常量、命令头、请求/响应编解码 | `protocol` |
| `datas` | 锁数据操作编码和响应数据解码 | `data` |
| `SlockClient` | 单连接 TCP client、握手、读写循环、请求匹配 | `client`, `transport` |
| `SlockReplsetClient` | 多节点副本集 client、leader 选择、重试和 pending 队列 | `replset` |
| `SlockDatabase` | DB 选择、默认 flag 合并、同步原语工厂 | `database` |
| `Lock`, `Event`, `Semaphore` 等 | 面向用户的同步原语 API | `primitive` |
| `callback` | Java async callback/future 管理 | `async_support` |
| `exceptions` | client/lock/data 异常映射 | `error` |

Rust 侧可以先按下面的模块边界拆：

```text
src/
  lib.rs
  error.rs
  protocol/
    mod.rs
    constants.rs
    command.rs
    codec.rs
    id.rs
  transport/
    mod.rs
    connection.rs
    request_map.rs
  client.rs
  replset.rs
  database.rs
  data.rs
  primitive/
    mod.rs
    lock.rs
    event.rs
    group_event.rs
    semaphore.rs
    read_write_lock.rs
    reentrant_lock.rs
    priority_lock.rs
    flow.rs
    tree_lock.rs
```

## 2. 协议基础

### 2.1 固定命令头

所有基础命令头都是 64 字节。`ICommand` 中定义：

| 名称 | 值 |
| --- | --- |
| `MAGIC` | `0x56` |
| `VERSION` | `0x01` |
| `COMMAND_TYPE_INIT` | `0x00` |
| `COMMAND_TYPE_LOCK` | `0x01` |
| `COMMAND_TYPE_UNLOCK` | `0x02` |
| `COMMAND_TYPE_PING` | `0x05` |

通用请求头：

| Offset | 长度 | 字段 |
| --- | --- | --- |
| 0 | 1 | magic |
| 1 | 1 | version |
| 2 | 1 | commandType |
| 3 | 16 | requestId |
| 19 | 45 | command-specific payload or zero padding |

通用响应头在 requestId 之后多一个 `result`：

| Offset | 长度 | 字段 |
| --- | --- | --- |
| 0 | 1 | magic |
| 1 | 1 | version |
| 2 | 1 | commandType |
| 3 | 16 | requestId |
| 19 | 1 | result |
| 20 | 44 | command-specific payload or zero padding |

Java 的 `dumpCommand()` 只真正用于 client 请求。部分 `CommandResult.dumpCommand()` 没有按 parse 布局写 result，Rust driver 不需要复刻这一点，只要正确解析服务端响应即可。

### 2.2 请求 ID / lock ID / client ID

`Command.genRequestId()`, `LockCommand.genLockId()`, `InitCommand.genClientId()` 使用相同 16 字节布局：

| 字节 | 内容 |
| --- | --- |
| 0..5 | 当前毫秒时间戳的高 6 字节，大端顺序 |
| 6..11 | `Random.nextLong()` 的高 6 字节，大端顺序 |
| 12..15 | 自增计数低 31 bit，大端顺序 |

Rust 侧建议实现一个 `Id16([u8; 16])`，提供 `new_request_id()`, `new_lock_id()`, `new_client_id()`。随机数可以用 `rand`，计数器用 `AtomicU32`。

### 2.3 数字编码

协议里的业务数字字段使用小端：

| 字段 | 宽度 |
| --- | --- |
| `timeout` | `u32`/`i32` 小端 |
| `expried` | `u32`/`i32` 小端 |
| `count`, `lCount` | `u16` 小端 |
| `rCount`, `lrCount` | `u8` |
| lock data 长度 | `u32` 小端 |

Java 字段名拼写为 `expried`，Rust API 可以对外使用 `expired`，但协议层建议保留注释说明它对应 Java 的 `expried` 字段，避免对照源码时混乱。

## 3. 命令编解码

### 3.1 InitCommand

Init 请求：

| Offset | 长度 | 字段 |
| --- | --- | --- |
| 0 | 1 | magic |
| 1 | 1 | version |
| 2 | 1 | `COMMAND_TYPE_INIT` |
| 3 | 16 | requestId |
| 19 | 16 | clientId |
| 35 | 29 | zero padding |

Init 响应：

| Offset | 长度 | 字段 |
| --- | --- | --- |
| 19 | 1 | result |
| 20 | 1 | initType |

`initType` flag：

| Flag | 值 | 含义 |
| --- | --- | --- |
| `INIT_TYPE_FLAG_HA_CLIENT` | `0x01` | HA client |
| `INIT_TYPE_FLAG_IS_TRANSPARENCY` | `0x02` | transparency |
| `INIT_TYPE_FLAG_IS_LEADER` | `0x04` | 当前节点为 leader |
| `INIT_TYPE_FLAG_HAS_LEADER` | `0x08` | 集群已有 leader |
| `INIT_TYPE_FLAG_IS_SHUTDOWN` | `0x10` | 节点 shutdown |

连接建立后必须先发送 Init 请求并等待 64 字节响应，`result == 0x00` 才算握手成功。副本集 client 根据 `IS_LEADER` 和后续 init 状态更新 leader。

### 3.2 PingCommand

Ping 请求只包含通用请求头，offset 19..63 全 0。Ping 响应只使用 offset 19 的 `result`。`ping()` 返回 `result == COMMAND_RESULT_SUCCED`。

### 3.3 LockCommand / UnlockCommand 请求

Lock 和 Unlock 共用 `LockCommand`，差异只在 `commandType` 和 flag 含义。

| Offset | 长度 | 字段 |
| --- | --- | --- |
| 0 | 1 | magic |
| 1 | 1 | version |
| 2 | 1 | `COMMAND_TYPE_LOCK` 或 `COMMAND_TYPE_UNLOCK` |
| 3 | 16 | requestId |
| 19 | 1 | flag |
| 20 | 1 | dbId |
| 21 | 16 | lockId |
| 37 | 16 | lockKey |
| 53 | 4 | timeout 小端 |
| 57 | 4 | expried 小端 |
| 61 | 2 | count 小端 |
| 63 | 1 | rCount |

请求携带 `LockData` 时需要在 flag 上加：

| 命令 | data flag |
| --- | --- |
| Lock | `LOCK_FLAG_CONTAINS_DATA` (`0x20`) |
| Unlock | `UNLOCK_FLAG_CONTAINS_DATA` (`0x20`) |

`getExtraData()` 返回完整的 lock data bytes，包含 4 字节长度前缀。发送帧顺序为 `64-byte header` + optional `extraData`。

### 3.4 LockCommandResult 响应

| Offset | 长度 | 字段 |
| --- | --- | --- |
| 19 | 1 | result |
| 20 | 1 | flag |
| 21 | 1 | dbId |
| 22 | 16 | lockId |
| 38 | 16 | lockKey |
| 54 | 2 | lCount 小端 |
| 56 | 2 | count 小端 |
| 58 | 1 | lrCount |
| 59 | 1 | rCount |
| 60 | 4 | reserved |

如果响应 flag 包含 `LOCK_FLAG_CONTAINS_DATA`，Java client 会继续读取：

1. 4 字节小端 `dataLen`。
2. 再读取 `dataLen` 字节 payload。
3. 将 payload 放入 `LockResultData`，其中 Java 为了复用索引保留了前 4 个空字节，实际 `stage/type` 在 `data[4]`，`commandFlag` 在 `data[5]`。

Rust 可以直接把响应数据建模为：

```rust
pub struct LockResultData {
    pub stage: DataStage,
    pub command_type: DataCommandType,
    pub flags: DataFlags,
    pub value: Vec<u8>,
}
```

同时保留一个 `raw` 方法用于兼容调试即可。

### 3.5 result 到错误映射

| result | Java 常量 | Rust error 建议 |
| --- | --- | --- |
| `0x00` | `COMMAND_RESULT_SUCCED` | `Ok` |
| `0x01` | `UNKNOWN_MAGIC` | `Protocol` |
| `0x02` | `UNKNOWN_VERSION` | `Protocol` |
| `0x03` | `UNKNOWN_DB` | `Server` |
| `0x04` | `UNKNOWN_COMMAND` | `Server` |
| `0x05` | `LOCKED_ERROR` | `LockLocked` |
| `0x06` | `UNLOCK_ERROR` | `LockUnlocked` |
| `0x07` | `UNOWN_ERROR` | `LockNotOwn` |
| `0x08` | `TIMEOUT` | `LockTimeout` |
| `0x09` | `EXPRIED` | `Expired` |
| `0x0a` | `STATE_ERROR` | `StateError` |
| `0x0b` | `ERROR` | `Server` |
| `0x0c` | `LOCK_ACK_WAITING` | `AckWaiting` |

`Lock.acquire()` 和 `Lock.release()` 都会将 `0x05/0x06/0x07/0x08` 转换成具体异常，其他非成功结果转成通用 `LockException`。Rust 建议让 lock API 返回 `Result<LockCommandResult, SlockError>`。

## 4. LockData 编码

`LockData.dumpData()` 输出：

| Offset | 长度 | 字段 |
| --- | --- | --- |
| 0 | 4 | `value.len() + 2`，小端 |
| 4 | 1 | `(stage << 6) | (commandType & 0x3f)` |
| 5 | 1 | commandFlag |
| 6 | N | value |

`stage`：

| 名称 | 值 |
| --- | --- |
| `CURRENT` | `0` |
| `UNLOCK` | `1` |
| `TIMEOUT` | `2` |
| `EXPRIED` | `3` |

`commandType`：

| 名称 | 值 | Java 类 |
| --- | --- | --- |
| `SET` | `0` | `LockSetData` |
| `UNSET` | `1` | `LockUnsetData` |
| `INCR` | `2` | `LockIncrData` |
| `APPEND` | `3` | `LockAppendData` |
| `SHIFT` | `4` | `LockShiftData` |
| `EXECUTE` | `5` | `LockExecuteData` |
| `PIPELINE` | `6` | `LockPipelineData` |
| `PUSH` | `7` | `LockPushData` |
| `POP` | `8` | `LockPopData` |

`commandFlag`：

| Flag | 值 | 用途 |
| --- | --- | --- |
| `VALUE_TYPE_NUMBER` | `0x01` | value 是数字，小端 |
| `VALUE_TYPE_ARRAY` | `0x02` | value 是 length-prefixed array |
| `VALUE_TYPE_KV` | `0x04` | value 是 key/value map |
| `CONTAINS_PROPERTY` | `0x10` | value 前带 property 区 |
| `PROCESS_FIRST_OR_LAST` | `0x20` | push/pop 等按首尾处理 |

数据类行为：

| 类 | value 编码 |
| --- | --- |
| `LockSetData` | bytes 或 UTF-8 string |
| `LockUnsetData` | 空数组 |
| `LockIncrData` | 8 字节小端有符号整数，默认 `1` |
| `LockAppendData` | bytes 或 UTF-8 string |
| `LockShiftData` | 4 字节小端长度 |
| `LockExecuteData` | 内嵌一个 `LockCommand.dumpCommand()` 的 64 字节命令 |
| `LockPipelineData` | 多个 `LockData.dumpData()` 直接拼接，并重新计算总长度 |
| `LockPushData` | bytes 或 UTF-8 string |
| `LockPopData` | 4 字节小端 count |

`LockResultData` 的读取方式：

| 方法语义 | Java 行为 |
| --- | --- |
| bytes/string | 跳过 header 和可选 property，读取剩余 value |
| long | 从 value offset 开始最多读取 8 字节小端 |
| list | value 中每项是 4 字节小端长度 + bytes |
| map | key 长度 + key UTF-8 + value 长度 + value bytes |

如果 `CONTAINS_PROPERTY` 被设置，Java 读取 `data[6..8]` 的小端 property 长度，然后把 value offset 移到 `8 + property_len`。

## 5. key 与 flag 管理

### 5.1 lockKey / lockId 归一化

`AbstractExecution` 对 lock key 做 16 字节归一化：

1. 输入长度大于 16：取 MD5 digest。
2. 输入长度小于等于 16：左侧补 0，原始字节右对齐。

`Lock` 对外部传入的 `lockId` 使用同样策略；没有传入时生成新的 16 字节 lockId。Rust 必须保持这一点，否则与 Java driver 对同一个 string key 的寻址不一致。

### 5.2 timeout / expried 的高 16 位 flag

Java 把时间值和 flag 打包在同一个 `int`：

```text
low  16 bits: timeout or expried value
high 16 bits: timeoutFlag or expriedFlag
```

`SlockDatabase` 的默认 flag 会在创建同步原语时合并到高 16 位。Rust 建议实现：

```rust
pub struct PackedTime(u32);

impl PackedTime {
    pub fn new(value: u16, flags: u16) -> Self;
    pub fn with_flags(self, flags: u16) -> Self;
}
```

重点 flag：

| Flag | 值 | 用途 |
| --- | --- | --- |
| `TIMEOUT_FLAG_RCOUNT_IS_PRIORITY` | `0x0010` | `rCount` 表示 priority |
| `TIMEOUT_FLAG_LOCK_WAIT_WHEN_UNLOCK` | `0x0200` | event 反向等待 |
| `TIMEOUT_FLAG_MILLISECOND_TIME` | `0x0400` | 毫秒单位 |
| `TIMEOUT_FLAG_REQUIRE_ACKED` | `0x1000` | 需要 ack |
| `TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED` | `0x4000` | group event version 比较 |
| `TIMEOUT_FLAG_KEEPLIVED` | `0x8000` | 保活 |
| `EXPRIED_FLAG_MILLISECOND_TIME` | `0x0400` | expired 毫秒单位 |
| `EXPRIED_FLAG_UNLIMITED_EXPRIED_TIME` | `0x4000` | 不限制过期 |
| `EXPRIED_FLAG_KEEPLIVED` | `0x8000` | 保活 |

## 6. 连接管理

### 6.1 单节点 SlockClient

`SlockClient.open()` 流程：

1. 如果 IO 线程已存在，直接返回。
2. `connect()`：
   - 创建 TCP socket。
   - `setKeepAlive(true)`。
   - `setTcpNoDelay(true)`。
   - 连接超时 5000ms。
   - 包装 4096 字节输入/输出 buffer。
   - 调用 `initClient()` 握手。
3. 启动 daemon IO 线程 `jaslock-io-{host}:{port}`。
4. 如果启用 async callback 且不是 replset 子 client，则启动 callback executor。

`initClient()`：

1. 如果没有 clientId，生成 16 字节 clientId。
2. 发送 `InitCommand` 的 64 字节请求。
3. 读取 64 字节响应。
4. 校验 `COMMAND_RESULT_SUCCED`。
5. 记录 `initType`。
6. 如果不是 HA client，清理单 client 的未完成请求。

Rust 实现建议：

| Java 机制 | Rust 建议 |
| --- | --- |
| daemon read thread | `tokio` task 或 blocking thread |
| `ConcurrentHashMap<BytesKey, Command>` | `DashMap<Id16, PendingRequest>` 或 `Mutex<HashMap<...>>` |
| `Semaphore` waiter | `oneshot::Sender<Result<...>>` |
| `ReentrantLock` 串行写 | 一个 writer task 或 `Mutex<WriteHalf>` |
| `pendingWriteCount` 合并 flush | 先实现每帧 flush，性能阶段再批量 flush |

### 6.2 读循环

`SlockClient.run()` 持续执行：

1. 如果 input stream 为空，调用 `reconnect()`。
2. 循环读取完整 64 字节头。
3. 按 `buf[2]` 判断命令类型：
   - `LOCK`/`UNLOCK`：解析 `LockCommandResult`，如有 extra data 继续读取数据。
   - `PING`：解析 `PingCommandResult`。
   - `INIT`：解析 `InitCommandResult`，更新副本集状态。
4. `handleCommand()` 用 `requestId` 从 requests map 找 pending command，写入结果并唤醒 waiter。
5. 读取失败或异常时关闭 socket，sleep 2 秒后重连。

Rust 侧要保证：

- `read_exact(64)`，不要接受短读。
- 对 extra data 先读 4 字节长度，再读完整 payload。
- 响应匹配必须按 `requestId`，不是按发送顺序。
- 关闭连接时要完成所有 pending waiter，返回 `ClientClosed` 或 `ClientDisconnected`。

### 6.3 写路径

Java 同步 `sendCommand()`：

1. 检查 client 是否关闭。
2. `dumpCommand()` 生成 64 字节头。
3. `createWaiter()` 创建等待信号。
4. 将 command 放入 requests map。
5. 在写锁内写 header + optional extraData。
6. 等待结果：
   - 普通 command 最多 120 秒。
   - `LockCommand` 最多 `(timeout & 0xffff) + 120` 秒。
7. 超时后移除 requests 并报 `ClientCommandTimeoutException`。

Java 异步 `sendCommand(command, callback)`：

1. 必须先 `enableAsyncCallback()`。
2. `CallbackExecutorManager.addCommand()` 给 command 设置 waiter callback。
3. 写入请求。
4. IO 线程收到响应时触发 command callback，再投递到 callback executor。
5. 定时任务按秒扫描 timeout queue，超时则回调 timeout error。

Rust API 可以同时提供：

```rust
impl Client {
    pub async fn send(&self, command: Command) -> Result<CommandResult, SlockError>;
    pub async fn write(&self, command: Command) -> Result<(), SlockError>;
}
```

其中 `write()` 对应 Java `writeCommand()`，只发送请求，不创建 waiter；主要供 replset retry/pending 重发使用。对普通用户 API，优先使用 `send()`。

### 6.4 close / reconnect

`close()`：

- 标记 `closed = true`。
- 关闭 socket。
- 如果是单 client，关闭所有 databases。
- 如果没有其他 lived replset client，清理所有 pending requests。
- 停止 callback executor。

`reconnect()`：

- 如果没有可用副本集节点，清理本 client 的 pending requests。
- 每 2 秒尝试 `connect()`。
- 成功后重新进入读循环。

Rust 实现时要把 `closed` 和 `reconnecting` 状态显式化，避免 close 后后台任务继续自动重连。

## 7. 副本集管理

`SlockReplsetClient` 实现同一个 `ISlockClient` 接口，但内部管理多个 `SlockClient`：

| 字段 | 作用 |
| --- | --- |
| `hosts` | `host:port` 列表，支持逗号分隔字符串 |
| `clients` | 所有子 client |
| `livedClients` | 当前连接可用节点 |
| `livedLeaderClient` | 当前 leader |
| `requests` | 所有子 client 共享的 request map |
| `pendingRequests` | 等 leader/可用节点后重发的 command 队列 |
| `databases` | 与子 client 共享的 256 个 database facade |

打开流程：

1. 解析每个 host。
2. 为每个 host 创建 `SlockClient(host, port, replsetClient, databases)`。
3. 子 client 共享 `requests` 和 `databases`。
4. 调用子 client `tryOpen()`，即使首次连接失败也启动后台重连。
5. 至少创建了一个 client 就认为 replset open 成功。

请求选择：

1. 优先使用 `livedLeaderClient`。
2. 没有 leader 时使用 `livedClients.getFirst()`。
3. 没有 lived client 则报 `ClientUnconnectException`。

重试逻辑：

| retryType | 含义 |
| --- | --- |
| `0` | 原始请求 |
| `1` | 已尝试转发到另一个当前可用 client |
| `2` | 已进入 pendingRequests |
| `3` | 从 pendingRequests 被唤醒重发 |

触发重试的场景：

- 写 socket 失败。
- 响应为 `COMMAND_RESULT_STATE_ERROR` 且是 `LockCommandResult`。
- 节点重连或 leader 出现时唤醒 pending 队列。

Rust `ReplsetClient` 建议用一个共享 inner：

```rust
pub struct ReplsetInner {
    clients: Vec<Arc<Client>>,
    lived: Mutex<Vec<usize>>,
    leader: AtomicUsize, // usize::MAX means none
    pending: Mutex<VecDeque<Command>>,
}
```

子 client 的 init 状态变化回调到 replset，更新 leader 并唤醒 pending。

## 8. Database 管理

`SlockDatabase` 是轻量 facade：

- 保存 `client`, `dbId`, `defaultTimeoutFlag`, `defaultExpriedFlag`。
- `getClient()` 在 database 已 close 时抛 `ClientClosedException`。
- 创建所有同步原语，并把默认 flag 合并进 timeout/expried 高 16 位。

`SlockClient` 和 `SlockReplsetClient` 都维护 `SlockDatabase[256]`，`selectDatabase(dbId)` 懒加载。Java 参数类型是 `byte`，但 Rust 必须使用 `u8` 并转换为 `usize` 索引，完整覆盖 0..255。

Rust API 建议：

```rust
pub struct Database<C> {
    client: Arc<C>,
    db_id: u8,
    default_timeout_flags: u16,
    default_expired_flags: u16,
}

impl<C: SlockTransport> Database<C> {
    pub fn lock<K: AsRef<[u8]>>(&self, key: K, timeout: u16, expired: u16) -> Lock<C>;
}
```

client 顶层的 `new_lock/new_event/...` 都只是 `select_database(0)` 的快捷方法。

## 9. 用户 API 与同步原语

### 9.1 统一 client trait

Java `ISlockClient` 统一了单节点和副本集：

- `open`, `tryOpen`, `close`
- `enableAsyncCallback`
- `sendCommand`, `writeCommand`
- `ping`
- `selectDatabase`
- `newLock`, `newEvent`, `newSemaphore`, `newMaxConcurrentFlow`, `newTokenBucketFlow`, `newGroupEvent`, `newTreeLock`, `newPriorityLock`

Rust 建议定义 trait：

```rust
#[async_trait::async_trait]
pub trait SlockClientLike: Send + Sync {
    async fn open(&self) -> Result<(), SlockError>;
    async fn close(&self);
    async fn send_command(&self, command: Command) -> Result<CommandResult, SlockError>;
    async fn write_command(&self, command: Command) -> Result<(), SlockError>;
    fn select_database(&self, db_id: u8) -> Database<Self>
    where
        Self: Sized;
}
```

如果先做 blocking driver，也可以把 async trait 换成同步方法，但内部协议对象和错误类型最好保持一致。

### 9.2 AbstractExecution 共同状态

所有同步原语都继承 `AbstractExecution`：

| 字段 | 含义 |
| --- | --- |
| `database` | 所属 DB |
| `lockKey` | 归一化后的 16 字节 key |
| `timeout` | 低 16 位值 + 高 16 位 flag |
| `expried` | 低 16 位值 + 高 16 位 flag |
| `count` | 并发数量控制 |
| `rCount` | 重入计数或 priority 等扩展值 |
| `currentLockData` | 最近一次响应中的数据 |

Rust 可用组合替代继承：

```rust
pub struct Execution<C> {
    database: Database<C>,
    lock_key: [u8; 16],
    timeout: PackedTime,
    expired: PackedTime,
    count: u16,
    r_count: u8,
    current_data: Option<LockResultData>,
}
```

### 9.3 Lock

`Lock` 是所有高级原语的基础。

核心方法：

| 方法 | 行为 |
| --- | --- |
| `acquire(flag, lockData)` | 发送 `COMMAND_TYPE_LOCK` |
| `release(flag, lockData)` | 发送 `COMMAND_TYPE_UNLOCK` |
| `show(lockData)` | 用 `LOCK_FLAG_SHOW_WHEN_LOCKED` 查询锁数据 |
| `update(lockData)` | 用 `LOCK_FLAG_UPDATE_WHEN_LOCKED` 更新数据 |
| `releaseHead(lockData)` | lockId 全 0，`UNLOCK_FIRST_LOCK_WHEN_UNLOCKED` |
| `releaseHeadRetoLockWait(lockData)` | release head 后让等待者立即获得锁 |
| `with()` | acquire 后返回 AutoCloseable，关闭时 release |

错误映射：

- `LOCKED_ERROR` -> `LockLocked`
- `UNLOCK_ERROR` -> `LockUnlocked`
- `UNOWN_ERROR` -> `LockNotOwn`
- `TIMEOUT` -> `LockTimeout`
- 其他非成功 -> `Lock`

Rust `Lock` 需要保存自己的 `lock_id`，否则 release 无法释放 acquire 获得的同一把锁。

### 9.4 Event

`Event` 通过底层 lock 模拟 set/clear/wait。它有一个重要参数 `defaultSeted`：

| `defaultSeted` | clear | set | isSet/wait 语义 |
| --- | --- | --- | --- |
| `true` | acquire/update 事件锁 | release 事件锁 | acquire 成功表示 set，timeout 表示未 set |
| `false` | release 事件锁 | acquire/update 事件锁 | 使用 `TIMEOUT_FLAG_LOCK_WAIT_WHEN_UNLOCK` 反向等待 |

`wait(timeout)` 把底层 `LockTimeoutException` 或 `ClientCommandTimeoutException` 包装为 `EventWaitTimeoutException`。`waitAndTimeoutRetryClear()` 在超时场景下会尝试补偿性 clear。

Rust 侧建议把 Event 实现为 Lock 的组合，不暴露内部 eventLock/checkLock/waitLock。

### 9.5 GroupEvent

`GroupEvent` 额外维护：

| 字段 | 含义 |
| --- | --- |
| `clientId` | 当前等待者/客户端 id |
| `versionId` | 事件版本 |

`encodeLockId(clientId, versionId)` 输出 16 字节：

| 字节 | 内容 |
| --- | --- |
| 0..7 | versionId 小端 |
| 8..15 | clientId 小端 |

`wakeup()` 和 `wait()` 会根据返回的 lockId 更新 `versionId`。它大量使用 `TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED`，用于版本小于当前锁版本时也视为成功。

### 9.6 ReentrantLock

`ReentrantLock` 持有一个固定 lockId 的 `Lock`，并设置 `rCount = 0xff`。重复 acquire/release 都复用同一个底层 lock。

### 9.7 ReadWriteLock

| 操作 | 底层行为 |
| --- | --- |
| `acquireWrite` | `count = 0`, `rCount = 0` |
| `releaseWrite` | 释放 writeLock |
| `acquireRead` | 新建 lock，`count = 0xffff` |
| `releaseRead` | 从 readLocks 队列取一个并释放 |

读锁每次 acquire 都生成新的 lockId，写锁复用同一个 lockId。

### 9.8 Semaphore

构造时把用户传入的 `count` 转为 `count - 1`。`acquire()` 每次新建 lockId 获取一个名额。`release()` 使用全 0 lockId，并带 `UNLOCK_FLAG_UNLOCK_FIRST_LOCK_WHEN_UNLOCKED`，释放队列头部锁。

### 9.9 MaxConcurrentFlow

与 semaphore 类似，`count = max - 1`。它缓存一个 `flowLock`，acquire/release 复用同一个 lockId。带 priority 时把 timeout 高 16 位加上 `TIMEOUT_FLAG_RCOUNT_IS_PRIORITY`，并用 `rCount` 保存 priority。

### 9.10 TokenBucketFlow

Token bucket 只提供 acquire，没有 release：

| period | 行为 |
| --- | --- |
| `< 3` 秒 | expired 使用 `ceil(period * 1000)` 并加 `EXPRIED_FLAG_MILLISECOND_TIME` |
| `>= 3` 秒 | 先按当前时间对齐到下一个 period 边界，timeout 为 0；如果 timeout，再用完整 period 和用户 timeout 重试 |

`with()` 返回空 close 操作，因为 token 获取后由过期时间自动释放。

### 9.11 PriorityLock

`PriorityLock` 是普通 lock 的 priority 包装：

- timeout 加 `TIMEOUT_FLAG_RCOUNT_IS_PRIORITY`。
- `rCount = priority`。
- acquire/release 复用同一个底层 lock。

### 9.12 TreeLock

`TreeLock` 用 parent/child 关系组合多把 lock：

- root 没有 parent。
- `newChild()` 用当前 `lockKey` 作为 child 的 parentKey。
- leaf lock 使用 `count = 0xffff`, `rCount = 1`, `TIMEOUT_FLAG_RCOUNT_IS_PRIORITY`。
- 子节点 leaf acquire 时先尝试 child check lock 和 parent check lock，再 acquire leaf。
- leaf release 使用 `UNLOCK_FLAG_UNLOCK_TREE_LOCK`。

Rust 侧建议先实现普通 `Lock` 后再实现 TreeLock，因为它依赖多个底层 lock 的补偿释放。

## 10. 异步接口设计

Java 同时支持 callback 和 `CallbackFuture`。Rust 更自然的接口是 `async fn`：

```rust
impl<C: SlockClientLike> Lock<C> {
    pub async fn acquire(&mut self) -> Result<LockCommandResult, SlockError>;
    pub async fn release(&mut self) -> Result<LockCommandResult, SlockError>;
}
```

如需 callback 风格，可以在 async API 上薄封装：

```rust
pub fn acquire_with_callback<F>(&self, callback: F)
where
    F: FnOnce(Result<LockCommandResult, SlockError>) + Send + 'static;
```

不要把 Java 的 callback executor 逐字搬到 Rust；协议请求匹配、timeout、连接重试才是必须保持的语义。

## 11. Rust 实现顺序

建议按下面顺序落地，风险最低：

1. `protocol/constants.rs`：完整迁移 `ICommand` 常量。
2. `protocol/id.rs`：实现 16 字节 ID 和 key 归一化。
3. `protocol/command.rs`：实现 Init/Ping/Lock 请求编码和响应解码。
4. `data.rs`：实现 LockData 编码和 LockResultData 解码。
5. `transport/connection.rs`：单节点 TCP connect/init/read loop/write/send。
6. `client.rs`：公开 `Client` facade 和 database 0 快捷方法。
7. `database.rs`：DB 懒加载、默认 flag 合并、primitive 工厂。
8. `primitive/lock.rs`：实现 Lock 的 acquire/release/show/update。
9. 其他 primitive：Event, Semaphore, ReentrantLock, ReadWriteLock, Flow, GroupEvent, TreeLock。
10. `replset.rs`：leader 选择、pending queue、retryType 语义。
11. 集成测试：用本地 slock 服务验证 Java/Rust 编码兼容。

## 12. 必备测试清单

协议单测：

- Init/Ping/Lock 请求编码长度必须是 64。
- LockCommand 各字段 offset 和小端编码正确。
- LockData 长度、stage/type、flag、value 编码正确。
- key 长度小于 16 时左补 0，大于 16 时 MD5。
- requestId/lockId/clientId 长度和组成符合 Java。

传输单测或集成测试：

- open 后必须先 init，init result 非 0 返回错误。
- send 后能按 requestId 匹配响应。
- 响应 extra data 能正确读取完整 payload。
- command timeout 后清理 pending map。
- close 后 pending request 全部返回错误。

API 集成测试：

- `Lock.acquire/release`。
- `LockData` 的 set/incr/append/shift/pipeline/push/pop。
- `Event` 的 default set/clear/wait 两种模式。
- `Semaphore` 最大并发数。
- `MaxConcurrentFlow` 和 `TokenBucketFlow` 限流语义。
- `ReadWriteLock` 读共享、写互斥。
- `ReentrantLock` 重入。
- `GroupEvent` version 更新。
- `ReplsetClient` leader 选择和断线重试。

## 13. 实现注意点

- Rust 不要照搬 Java 自定义 `BufferedOutputStream`，只需保证一个完整命令帧的 header 和 extra data 按顺序写入，并在需要时 flush。
- requests map 的 key 必须按 requestId 的字节内容比较，Java 用 `BytesKey` 包装 byte array。
- Java `selectDatabase(byte dbId)` 受 signed byte 限制，Rust 应用 `u8`。
- `LockCommandResult` 的 response data 读取和 request data 写入不是完全对称的；request extra data 包含长度前缀，response 解码时长度前缀用于 framing，数据对象只需要 stage/type、flag、value。
- 高级同步原语大多是 `Lock` 的组合，不需要协议新命令。
- `TIMEOUT_FLAG_RCOUNT_IS_PRIORITY` 在 Java 常被写成 `timeout | 0x00100000`，本质是把 `0x0010` 放入 timeout 高 16 位。
- `expried` 拼写来自 Java 协议字段，Rust 文档和代码注释要标明对应关系。

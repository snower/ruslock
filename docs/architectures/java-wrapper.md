# Java 包装架构：用 JDK 8 验证 RusLock 原语

## 1. 目标与兼容基线

在 `wrapper/java` 提供 Maven 模块 **`io.github.snower:ruslock`**，最低运行环境为 **JDK 8**。它是 Rust 原语的 Java 绑定，而不是在原来的 Java 锁算法下替换网络传输。Java 侧发起一次 `Event.wait()` 或 `Semaphore.acquire()`，应执行与未来其他语言绑定相同的 Rust 原语实现。

兼容基准是 `D:\workspace\github\jaslock` 的 `85c2d2b`（Maven 版本 `1.1.5`）。保留 Java 包名 `io.github.snower.jaslock`、公开类与接口、构造器、重载、常量、受检异常、回调及 `with()` 等用法。Maven 坐标变化不要求用户改 import；同一个 classpath 不应同时引入原 `jaslock` 和新 `ruslock`，两者提供相同的类名。只将稳定 Java 源码用作 API 与行为基准，不引入原版 JAR 作为运行时依赖。迁移源码时保留原版权和 MIT 许可声明。

兼容性以**可观察行为**为准：参数含义、协议结果、状态变化、异常类型、回调完成及生命周期都要对照原版。不要求复刻线程实现、缓冲实现或同步机制。旧 Java 实现中发现的行为差异要先对照验证；不为方便包装而擅自修改契约。本文是目标设计，不代表 JNI 接口、平台产物或测试已经实现。

## 2. 选型与分层

选择 **JNI + Rust `aio` 原语 + 单一连接管理路径**。JNI 在 Java 8 可用，不依赖 Panama/FFM。同步和异步 Java API 调用同一个 Rust `aio` 对象；同步方法等待完成，不另起 `blocking` 客户端。

| 方案 | 结论 |
| --- | --- |
| 保留 Java 原语，只替换 `sendCommand` / `writeCommand` | 接入较快，但只能验证 Rust 传输，无法验证 Rust 原语。 |
| 每类 Java 原语委托给对应 Rust 原语 | 采用。兼容成本在构造、对象生命周期与并发，需要显式设计。 |
| Java 管 socket，Rust 用 Sans-IO 驱动 | 不作为默认路径。Java 还要实现传输生命周期，也增加跨 JNI 的读写交互。 |

```text
应用 / 原版 Jaslock 测试
  → Java 8 兼容外观：公开类、重载、结果/异常、CallbackFuture、执行器
    → JNI cdylib：类型转换、对象句柄、一次性完成通知
      → Rust 绑定接口：不依赖 JNI，管理对象、操作与完成
        → ruslock::aio：原语状态与操作、连接与副本集、协议
          → Slock 服务端
```

建议将 Maven 模块、Java 源码及 JNI crate 放在 `wrapper/java`（JNI crate 可置于 `wrapper/java/native`）。通用绑定接口放在 Rust 核心侧的明确模块中，不能依赖 `JNIEnv`、Java 类型或 Java 执行器。后续语言接入同一 Rust 接口与原语；需要稳定外部 ABI 时再增加版本化 C 接口，不把 JNI 或 Rust 内存布局当作多语言 ABI。现有 `blocking` / `callback` Rust API 不在本项目重写范围内；若未来绑定使用不同执行后端，须复用同一份原语逻辑，不能仅凭类名相同宣称语义相同。

## 3. 原语映射与职责

Java 对象保存 Rust 句柄与必要的 Java 侧依赖（如执行器、父对象引用）；禁止在 Java 中维护第二份锁状态或在 Rust 缺方法时回退到纯 Java 实现。`SlockDatabase` 保留公开工厂入口，DB 选择、默认 flags 及原语构造走 Rust。`with()` 只负责 Java 的作用域适配：对同一个原语调用 Rust 获取和释放。

| Java 类型 | Rust 必须拥有的行为 |
| --- | --- |
| `Lock` | 构造与显式 lock ID、获取/释放、show/update/release-head、count/rCount、当前锁数据及 flags。 |
| `Event` / `GroupEvent` | 默认置位模式、set/clear/wait 与超时路径；GroupEvent 的版本、唤醒与重试规则。 |
| `ReentrantLock` / `ReadWriteLock` | 重入、读写获取与释放的规则和内部状态。 |
| `Semaphore` | 容量、获取/释放、计数查询与释放相关操作。 |
| `PriorityLock` / `MaxConcurrentFlow` / `TokenBucketFlow` | 优先级、并发上限、速率周期与相应超时语义。 |
| `TreeLock` / `TreeLeafLock` | 父子关系、叶锁生命周期与层级操作。 |

落地前按 Jar 公共签名清单对照现有 Rust 方法，而不是按类型同名假定功能齐全。[`aio/primitives.rs`](../../src/aio/primitives.rs) 的部分构造器接受 `u16` timeout/expired；Java 公共 API 传递 `int` 打包值。绑定接口必须保留完整 32 位位型，在 Rust 中使用 `PackedTime` 并按基准行为合并默认 flags，不能先截为 `u16`。同样逐项核对 key 归一化、显式 ID、无符号位型、`GroupEvent` 版本和 `TreeLock` 父子构造器。Rust 缺少用来表达 Java 行为的操作时，补 Rust 原语和 Rust 回归测试，再接 JNI。

`LockData`、`Command` 和 `CommandResult` 等公开 Java 类型保留其构造、编码和读取用法；绑定须明确区分 `null`、空 payload 与实际数据，并保留 request ID、result code、flags 和返回数据。正常原语调用由 Rust 创建与执行协议命令；Java 的 `sendCommand` / `writeCommand` 则是独立的公开低层入口。后者应验证公开 `Command` 子类和附加帧，不可只匹配 Rust 内置 enum 变体后丢弃 Java 调用方的数据。需要原始帧能力时由 Rust 传输层提供受约束的入口；只写操作不等待服务端响应，也不伪造成功响应。

## 4. 请求、对象与并发生命周期

- **连接与运行时。** 每个 Java 客户端拥有一个 Rust 逻辑客户端及连接管理域；副本集可以有多个节点连接。绑定可共享受控的 Tokio runtime，不为每个原语或同步调用建线程池。Rust 拥有连接、握手、路由、请求关联与超时。区分 `open()` 和 `tryOpen()`：后者须按基准语义在初次连接失败后继续后台重连。现有 [`aio/connection.rs`](../../src/aio/connection.rs) 在握手成功后才启动 reader，不能只靠 Java 吞异常来实现 `tryOpen()`。
- **句柄与关闭。** JNI 使用有类型且可识别过期值的 opaque `jlong` token，由 Rust 句柄表管理；不将可解引用裸指针交给 Java。提交时取得对象的强引用并检查客户端状态，避免请求与句柄释放竞争。`close()` 停止接收新请求、结束连接与未完成请求、释放客户端所属句柄；清理应幂等。原语被回收时只释放本地句柄，不能隐式执行未定义的远端解锁。若需要 GC 兜底，使用 Java 8 可用的 `PhantomReference` / `ReferenceQueue`，不依赖 Java 9 的 `Cleaner`；显式关闭仍是主路径。是否允许关闭后重开以及相应句柄有效期，以原版实测为准。
- **同一原语上的并发。** 对象状态锁只保护短暂的读取和提交/完成更新，不跨网络 `.await`，也不在持锁时调用 Java。现有部分 `aio` 方法用 `&mut self` 跨越等待；不能简单包一层 `Mutex` 后等待请求结束。应在 Rust 中拆分状态快照、网络执行和结果应用，根据操作为冲突的状态更新定义顺序，测定迟到结果如何处理。特别要验证同一 `Event` 上无论哪个 API 发起 `wait()`，另一个线程发起的 `set()` 都不会被挡在其后。
- **完成与调度。** 同步 Java 方法等待 Rust 请求的结果；异步提交保留 `enableAsyncCallback` 门控、`Consumer` 与 `CallbackFuture` 的既有行为。区分立即抛出的提交失败与已受理请求的异步完成。响应、超时、断线和关闭竞争时，已受理请求只完成一次；由同一个完成路径释放请求资源和 JNI 全局引用。用户回调通过 Java 兼容执行器分发，不在 Rust I/O 任务中运行；验证回调里再次调用客户端或抛异常不会阻塞 I/O、丢失完成或发生自等待。Java Future 的取消与远端操作取消不是同一件事，必须按照基准验证，不假设取消可撤销已发出的锁命令。
- **错误与边界安全。** Rust 传回结构化错误和可用响应，由 Java 还原 Jaslock 异常类型与携带的数据；区分服务端锁超时和客户端命令等待超时，不能统一转成 `RuntimeException`。JNI 校验句柄类型、数组长度、值域与空值；跨线程通知时正确附着 JVM，不跨线程保存 `JNIEnv`。禁止 Rust unwind 跨 JNI 边界，并保证异常路径释放引用和资源。
- **副本集。** 对照基准的 leader/`STATE_ERROR`、等待队列、切换与重放条件补齐 Rust 连接层。将“发送前明确失败”和“已写入但响应未知”分开处理；不能把所有 I/O 异常一律重试，尤其是有副作用的锁与数据命令。同步、回调和低层命令入口须共享同一副本集路由和状态。

## 5. JDK 8、构建与发布

1. **Java 构建**：Maven POM 使用 `io.github.snower:ruslock`，在真实 JDK 8 上编译和运行测试（Java class 文件主版本 52）。加跑较新 JDK 可以发现兼容问题，但不能替代 JDK 8。原测试保留 JUnit 4，不迁移测试框架。
2. **Native 构建**：JNI crate 产出 `cdylib`，构建锁定 Rust 依赖；在声明支持的 OS/架构上构建和运行 JNI 冒烟测试。第一轮至少在开发环境及 Linux CI 验证，其他平台仅在对应产物和实测齐备后列入支持范围。发布应明确 JVM 架构及 Linux libc 等约束，不把开发机上的 native 路径当作正式分发方案。
3. **分发与装载**：主发布产物应让受支持平台的消费者只声明一个 Maven 依赖即可运行。可将已验证的 native 库资源随主 JAR 发布，按 OS/架构选择，提取到权限受控且与版本隔离的位置，用 `System.load` 加载；资源缺失、平台不支持或装载失败时给出可定位的错误。不要静默回退到纯 Java，也不从网络临时下载库。验证并行初始化、Windows DLL 占用与多 classloader 行为；若首版不支持某场景，明确报错和限制。
4. **产物验收**：用独立 Maven 项目和隔离本地仓库消费安装后的 JAR，不依赖源码树、`target` 或预设的 `java.library.path`。实际加载 native、连接测试服务并执行锁操作；仅在 reactor 中 `mvn test` 成功不能证明发行包可用。

## 6. 落地顺序与验收

| 阶段 | 交付门槛 |
| --- | --- |
| 基线 | 固定 Jaslock 提交与 Slock 服务镜像版本；原版在 JDK 8 上运行完整测试。产出公开签名清单、原测试文件哈希与 Surefire 报告。 |
| 最小闭环 | 单节点 `Lock` 和 `Event` 已从 Java 调到对应 Rust 原语；同步、回调、携带数据的结果、异常映射、`wait` 与 `set` 并发及关闭竞态通过对照。用 mock/事件探针验证经过 Rust 原语路径，不靠源码中的 JNI 调用数量推断。 |
| 完整 API | 补齐剩余原语、低层命令及副本集；对照公开签名（含泛型、受检异常与公开嵌套类型）和关键行为；运行 RusLock 原有 Rust 回归，确认核心补差未破坏其他入口。 |
| 发行验证 | 原样测试全绿，独立消费项目通过 JDK 8 加载及连接验证；只发布已测平台。 |

**原样测试是硬门槛。** 当前本地基准测试树只有 `ClientTest.java`，含 **24 个 `@Test`**，包括 `testBenchmark`；仓库旧设计中的 22 不是迁移清单。逐字节复制测试并用 SHA-256 校验，不改 package、断言、端口、等待时间或用例内容，不将 benchmark 排除。需要 Slock 服务监听固定 `127.0.0.1:5658`；原版与包装版分别使用干净服务状态、顺序运行，以 Surefire 报告核对测试名和计数，要求 `24` 执行、`0` 失败、`0` 错误、`0` 跳过。若服务缺失导致连接被拒绝，应先修复环境，不把编译通过当作集成通过。若出现失败，应对照原版和协议行为，调整 Rust 或绑定实现，不改迁入的测试。

**另建测试，不修改原测试。** 至少补充：同对象并发 wait/set；响应/超时/关闭的恰好一次完成；首次失败后的重连；多节点 leader 切换与重放边界；未启用回调、重入回调及退出；高级 flags 和锁数据；`sendCommand` / `writeCommand` 对公开命令的行为。原版和包装版因包名相同，应放在独立 JVM 或分离构建环境做对照。原测试的副本集用例只连接单节点，优先级用例也不足以证明排序；需要独立多节点和排序断言来补证据。

建议验证入口（实现后执行）：原版目录在 JDK 8 下运行 `mvn -B clean test`；本仓库运行 `mvn -B -f wrapper/java/pom.xml clean verify`、`cargo test --all-features --locked`，并单独运行 native crate 的检查（若纳入同一 Cargo workspace，可由 workspace 验证覆盖）。记录服务版本、JDK、运行平台、命令退出码和测试报告；Maven、Rust 与产物消费三项证据互不替代。

## 7. 参考

- [现有 RusLock/Jaslock 协议与 API 对照](../Architecture.md)
- [Rust aio 客户端与低层发送入口](../../src/aio/client.rs)、[副本集实现](../../src/aio/replset.rs)
- [仓库现有 Slock 服务测试配置](../../.github/workflows/ci.yml)

# RusLock Java Wrapper Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 提供在 JDK 8 上以 Jaslock API 调用 RusLock 原语的 Maven 绑定。

**Architecture:** Java 外观通过 JNI 调用无 JVM 依赖的 Rust 绑定接口；Rust `aio` 状态和连接是唯一执行路径。先以 Lock/Event 穿透各层，再覆盖全部原语、低层命令和发布产物。

**Tech Stack:** Maven、Java 8、JUnit 4、JNI cdylib、Rust/Tokio、真实 Slock 服务。

**Spec:** [JDK 8 绑定规格](../specs/2026-11-26-java-wrapper-design.md)；[架构细节](../../architectures/java-wrapper.md)。

## Global Constraints

- `wrapper/java`，`io.github.snower:ruslock`，Java 包 `io.github.snower.jaslock`，class major 52。
- 以 Jaslock `85c2d2b` 为准；保留版权；原 24 项测试逐字节复制，不修改、不跳过。
- 原语无 Java 回退；同步/回调共用 Rust `aio`；不改现有 Rust 公开 API 签名。
- 测试 Slock 固定 `127.0.0.1:5658`；不自动提交、发布或推送。

## Review Focus

- 任务 2：同一个 Rust Event 的 `wait` 跨网络挂起时还能 `set`。
- 任务 2：Java 打包时间的高 16 位合并后不丢失。
- 任务 3：无节点启动后恢复，关闭不会遗留重连任务。
- 任务 4：响应与关闭竞争及回调重入均不重复/死锁。
- 任务 5：不确定是否写成功的命令不得被盲目重放；低层只写不等待应答。

### 任务 1：建立原样测试和 API 基准

**文件：** `wrapper/java/src/test/java/io/github/snower/jaslock/ClientTest.java`、`wrapper/java/tools/`（校验和签名对照脚本）。输入为外部 Jaslock checkout，输出为基准 SHA-256、签名清单及 Surefire 结果，不修改原 checkout 的受版本控制文件。

- [x] 确认基准 HEAD `85c2d2b`，复制测试与相应许可，校验哈希相等和 24 个 `@Test`。
- [x] 固定 Slock `v2.1.14`（`1669b53`）。JDK 8 运行基准 `mvn -B clean test`：24/0/0/0；使用原有本机进程，测试前后均无活跃锁，未重启服务。
- [x] 从基准 JAR 生成类与成员签名比较输入，过滤合成实现细节，保留可见扩展面。

### 任务 2：Rust Lock/Event 原语与绑定对象

**文件：** `src/aio/primitives.rs`、`src/aio/database.rs`、`src/binding/`、`tests/binding_primitives.rs`。Rust 对外提供按类型构造对象、**提交操作（返回 Rust completion）** 与释放句柄；传递 `PackedTime` 和结构化 `SlockError` / `LockCommandResult`，不依赖 JNI。

- [x] 先写 `event_wait_allows_set_on_same_handle` 和 `packed_time_keeps_caller_and_database_flags`；让前者在有界时间内收到 wait 成功，让后者直接断言实体命令的 32 位值。
- [x] 运行 `cargo test --all-features --test binding_primitives`，确认新增契约尚未满足。
- [x] 为 `Lock` / `Event` 增加兼容构造与操作入口；拆分挂起操作的状态读取与完成更新，而不是跨 `.await` 持对象锁。绑定对象共用现有 Rust 原语，不另写 Event 算法。
- [x] 运行同一 Rust 测试至通过：2 项通过。
- [ ] 随 JNI 句柄表验证跨类型、过期 token 拒绝以及关闭不隐式解锁。

### 任务 3：Rust 连接生命周期

**文件：** `src/aio/connection.rs`、`src/aio/client.rs`、`src/aio/handle.rs`，测试 `tests/binding_lifecycle.rs`。为绑定提供 strict open、try-open 与 close，后续任务扩到 replset。

- [x] 添加 `try_open_recovers_after_initial_refusal`、`close_stops_initial_reconnect`；使用可控监听器，在首次失败后启动服务并等待握手，在关闭后断言不会建立新连接。
- [x] 运行 `cargo test --all-features --test binding_lifecycle` 观察新契约失败，再实现生命周期入口与取消；保留现有普通 `open` 行为。
- [x] 重跑 focused 测试：4 项通过；另跑 `aio_mock` 14 项和 `client_api` 6 项通过。后续收发代码变更后需重验。

### 任务 4：JNI 与 Java 最小闭环

**文件：** `wrapper/java/pom.xml`、`wrapper/java/native/{Cargo.toml,src/lib.rs}`、`wrapper/java/src/main/java/io/github/snower/jaslock/`、`wrapper/java/src/test/java/io/github/snower/jaslock/NativeLifecycleTest.java`。Java facade 链接 `NativeBridge` 私有入口，入口传 opaque `long`、Java 8 类型和只完成一次的通知对象。POM 生成 JNI 可加载测试产物。

- [ ] 增加 `NativeLifecycleTest`：真实打开、同步 Lock、Future/Consumer Event、并发 wait/set、回调重入、关闭与响应竞争且回调计数为 1。先运行指定测试，确认缺少绑定。
- [ ] 引入保留 MIT 的兼容类型；实现 JNI 加载、对象句柄和结构化错误映射，Java 只做参数、等待和回调调度。未接完的原语不能使用旧 Java 算法作为过渡测试路径。
- [x] 在 JDK 8 运行 `mvn -B -f wrapper/java/pom.xml -Dtest=ClientTest,NativeLifecycleTest test`：原样测试 24/0/0/0、JNI 生命周期 5/0/0/0。使用现有本机 Slock；最终改动落地后重验。

### 任务 5：全原语、低层命令及副本集

**文件：** `src/aio/{primitives,database,replset,connection}.rs`、`src/binding/`、`wrapper/java/native/src/`、`wrapper/java/src/main/java/io/github/snower/jaslock/`。同一类型化绑定层承载剩余构造、状态与操作；独立低层传输入口接受受验证的公开命令帧。

- [ ] 加失败用例固定显式 lock ID、GroupEvent 版本、TreeLock 父键和返回数据；再补 Rust 原语方法并接 JNI，以原样 `ClientTest` 对应方法逐组回归。
- [ ] 加失败用例固定只写命令服务端不回包仍完成、公开自定义命令附带数据完整、leader 切换后未发出请求按基准处理且不会无差别重放结果未知的请求。
- [ ] 在 Rust 补低层发送、写入和副本集的 lifecycle/routing 行为；用可控节点验证故障切换，另用真实 Slock 跑原样测试。
- [ ] 比较公开签名、protected 成员和 checked exceptions；运行 `cargo test --all-features --locked`，按 JDK 8 基准在独立干净实例跑 `ClientTest` 全 24 项。
  - 本地验收：`cargo test --all-features --locked` 重跑 172 通过、0 失败、1 项 Rust benchmark 按原配置 ignored；最终 JAR 的 61 个 API 类型签名与基准相同。JDK 8 对现有 Slock 服务运行原样 Java 24 项并全部通过，包含 `testBenchmark`。独立干净 Slock 实例尚未验证。

### 任务 6：打包与独立消费验证

**文件：** `wrapper/java/pom.xml`、`wrapper/java/README.md`、`wrapper/java/tools/`、`.github/workflows/` 中单独的 Java 验证 job。输出是包含已验证平台 JNI 资源的 JAR。

- [ ] 用独立项目先验证未打包资源时加载失败，再配置按 OS/架构打包和安全提取；缺失或错误 native 产物不得回退。
- [x] Windows x86-64 / JDK 8：`mvn -B -f wrapper/java/pom.xml clean verify` 66/0/0/0；原样 `ClientTest` 24/0/0/0 且 SHA-256 与基准相同；JAR 的 78 个 class 都是 major 52。
- [x] 最终 Windows x86-64 产物通过独立 Maven 消费验收：只声明 `io.github.snower:ruslock:1.1.5`，空 `java.library.path` 下验证真实锁获取、互斥和释放；剥除 DLL 的负对照按预期失败。
- [ ] 汇总平台、JDK、服务、命令退出码与报告。只对实际验证的平台和契约作通过声明。

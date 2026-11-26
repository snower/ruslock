# RusLock Java 8 绑定规格

本规格将[已确认的整体架构](../../architectures/java-wrapper.md)转成可验收的交付要求；实施顺序见[计划](../plans/2026-11-26-java-wrapper.md)。

## 交付范围

- `wrapper/java` 构建 Maven 工件 `io.github.snower:ruslock`，运行和编译基线 JDK 8（class major 52），Java 包名保持 `io.github.snower.jaslock`。兼容参照为 `D:\workspace\github\jaslock` 提交 `85c2d2b`（POM `1.1.5`）；镜像的 Java 类保留 MIT 原版权。原版与包装版不同时放入一个 classpath。
- 保留客户端、数据库、所有公开原语、命令、数据、异常、回调、构造器和重载。对照公开及受保护扩展面、泛型和 checked exceptions；需要行为相同，不要求内部线程或私有方法相同。
- `Lock`、`Event`、`GroupEvent`、`Semaphore`、`ReentrantLock`、`ReadWriteLock`、`PriorityLock`、`MaxConcurrentFlow`、`TokenBucketFlow`、`TreeLock`/叶锁均由 Rust 原语实现；Java 只封装对象、结果、错误、回调及作用域。无纯 Java 原语回退。使用同一 Rust `aio` 实现作为未来其他语言的入口；首版不承诺对外稳定 C ABI。

## 接口行为

- Java 同步、Future 和 Consumer 操作通过同一 Rust 客户端、同一原语身份。JNI 只传有类型且可判定过期的句柄、参数和结果。Rust 负责状态、I/O、请求关联、重连和副本集；只写命令不等待协议响应。低层 `sendCommand`、`writeCommand` 独立保留，不是 Java 原语的实现路径。
- 保留完整 timeout/expired 位型、数据库 flags、显式 lock ID、key 归一化、原语计数/版本/父子关系及空值与空 payload 的区别。Java 从结构化错误还原 Jaslock 异常及可用的命令结果；服务端锁超时与本地等待超时不能合并。
- `tryOpen` 首次连接失败后继续后台重连；副本集按基准区分未发出与写入后结果未知的请求，不无条件重放有副作用的操作。回调保留门控和提交失败的同步异常，已受理请求在响应、超时、断线、关闭竞争下只能完成一次；用户回调不占用 native I/O 任务。
- 同一 `Event` 上等待不能阻塞另一个线程的 `set`。句柄关闭和已发请求竞争不能造成悬空引用；关闭后不再接受请求，显式释放优先，GC 回收仅释放本地资源。Rust panic 不得越过 JNI 边界。
- 支持平台的 Maven 消费者只声明一个依赖即可加载 native 库；缺平台产物或加载失败须明确报错。仅将真实运行验证过的平台列入支持范围。本项目不自动发布 Maven 工件。

## 验收证据

1. `ClientTest.java` 按原包路径逐字节复制，SHA-256 与基准一致；真实 Slock 监听 `127.0.0.1:5658`，分别在独立、干净状态下用 JDK 8 运行原版与包装版。两份报告各有 24 项执行，0 失败、0 错误、0 跳过，包括 `testBenchmark`。任何失败修实现或环境，不改迁入测试。
2. 单独验证 JAR 签名兼容（包括基准测试未触达的入口），用 Rust 原语层回归和 JNI 集成证明确实在执行 Rust 原语；单独验证 wait/set 并发、完成竞态、回调重入、首次连接失败恢复、低层命令和多节点故障切换。
3. Rust 原有测试无回归；独立 Maven 示例用 JDK 8 只引入发行 JAR 即可加载 native 并完成真实锁操作。记录 JDK、平台、服务版本、命令与报告。环境阻塞时如实报告，不把编译或 mock 测试当作真实服务验收。

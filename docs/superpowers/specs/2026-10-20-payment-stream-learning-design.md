# 支付实时统计学习项目设计

日期：2026-10-20

## 1. 目标与已确认的运行方式

面向熟悉 Java / Spring、初学 Flink 的开发者，通过可运行的支付统计案例理解：

- Source、算子、keyBy、托管状态与 Sink。
- 业务重复事件去重与 Exactly-once 的区别。
- Checkpoint、输入重放、Kafka 事务及 read_committed 可见性。
- Spring 消费者的 PostgreSQL 本地事务、幂等消费和结果版本控制。

用户已确认：Docker 运行 Kafka、PostgreSQL、Flink；Spring Boot 在 IDEA 或本机 Maven 中运行。

已验证本机 Java 21、Maven、Docker Engine 和 Docker Compose 可用。当前目录未初始化 Git，
已有 IDEA 工程配置，不覆盖或调整该配置。

交付是一套仅供本机教学的项目，不包含真实支付、退款、登录权限系统、生产集群或页面开发。
第一阶段不加入窗口，避免将时间语义与故障恢复同时引入；教程明确后续可在此基础上学习窗口。

## 2. 架构与模块

```text
HTTP 请求
  → payment-service 支付事件接口
  → Kafka payment-events
  → flink-job：keyBy(shopId)、去重、累计金额和订单数
  → Kafka shop-payment-totals（事务输出）
  → payment-service 结果消费者
  → PostgreSQL processed_event + shop_payment_stat
  → HTTP 查询接口
```

使用 Maven 父工程与两个子模块：

| 模块 | 职责 |
| --- | --- |
| payment-service | HTTP 接口、Kafka 生产和消费、数据库事务、查询、教学故障注入 |
| flink-job | 独立提交到 Flink 的 Java DataStream 作业 |

不把 Flink 集群嵌入 Spring Boot，也不为两个 JSON 消息对象增加公共代码模块。
消息契约在文档中集中定义，并通过契约测试检查两侧序列化兼容性。

使用 Spring Boot 3.5、Flink 1.20 LTS、Kafka 3.9 和 PostgreSQL 17 系列；
实现时锁定经过依赖与镜像校验的具体发布版本，不使用 latest 或 SNAPSHOT。
代码以 Java 17 字节码为目标；本机 Java 21 运行 Spring，Flink 容器使用 Java 17。

Docker Compose 提供单 Kafka broker（KRaft）、PostgreSQL、Flink JobManager 和 TaskManager。
固定项目资源名称并只将必要端口绑定到本机回环地址。提供容器内外各自可达的 Kafka listener。
使用持久化卷保存数据库、Kafka 数据与 Checkpoint；常规停止不删除卷。

## 3. 消息契约与业务边界

金额一律用 long 表示“分”，要求正整数，并检查累计溢出。
Kafka 两个 Topic 均使用 shopId 作为消息 key；各使用两个分区，Flink 并行度设为 2。
实验期间不调整分区数量，同一店铺输入严格保持 Kafka 分区内顺序。

### 3.1 支付事件

字段：eventId、shopId、amountCents、occurredAt。

- eventId：调用者提供的稳定支付事件标识；重试不能生成新标识。
- shopId：店铺标识；两个 ID 都限定长度与字符范围，便于日志和结果 ID 生成。
- occurredAt：业务发生时间，当前累计实验不根据它排序或划窗。
- 同一事件的重试必须保留 shopId、amountCents 和 occurredAt。
- HTTP 发布接口等待 Kafka 发送确认后才返回成功；成功只代表事件已接收，不代表统计已经落库。
- 发送确认超时表示结果不确定，调用方应使用原事件重试。
- 接口不同时写支付数据库，不引入数据库与 Kafka 双写问题。

Flink 按 shopId 分组，并用 MapState 记录该店铺已处理的 eventId 及关键载荷。
完全相同的重复事件直接跳过，不增加累计值或版本，不输出新的统计事件。
同一店铺下相同 eventId 携带不同载荷时明确报错，不悄悄采用某一个金额。
本实验的去重身份为 (shopId, eventId)，不声称跨店铺校验 eventId 全局唯一。

教学数据量很小，去重状态不设置 TTL，避免 TTL 过期改变实验结论；
README 说明这不是无限期保存生产事件的容量方案。

### 3.2 店铺累计结果

字段：resultId、shopId、totalAmountCents、paymentCount、version、triggerEventId。

每处理一笔新支付，更新托管累计状态，并令该店铺 version 增加 1。
version 同时对应该店铺的唯一支付计数；resultId 由 shopId 与 version 确定性生成。
重放同一输入前缀时，恢复同一状态后产生相同的版本和结果 ID，不使用当前时间生成去重身份。

结果语义是“截至 version 的累计值”，数据库覆盖写入 totalAmountCents，禁止再次累加累计值。
version 只在同一店铺、同一连续作业状态历史内比较。
不支持丢弃 Flink 状态后沿用旧的 Topic 和结果表重新开始版本计数。
重建实验必须同步重建整套教学数据，或从持久化 Checkpoint 恢复。

## 4. Flink 状态、Checkpoint 与 Kafka 事务

Flink Source 从 payment-events 读取，初次启动使用 earliest，
故障恢复优先使用 Checkpoint 中的读取位置。
算子配置稳定的 UID，使用托管状态保存去重信息、累计金额和版本。

默认每 10 秒触发 Checkpoint，最大并发数 1，超时 60 秒；
Checkpoint 保存到持久化卷，配置可通过作业参数调整，并设置重启策略。
示例日志记录 shopId、输入事件 ID、当前版本及累计结果，不把内存状态误称为已提交数据库结果。

Kafka Sink 显式启用 EXACTLY_ONCE，使用本项目独立的 transactionalIdPrefix。
事务超时设置为 10 分钟，并确认 broker 上限允许此值；说明其需覆盖快照和预计恢复耗时。
Spring 结果消费者显式配置 read_committed。

教程分别解释：

1. 算子输出：新支付到来后即产生累计结果。
2. Kafka 已接收：Producer 刷出消息并取得确认，事务可能仍未提交。
3. 消费者可见：对应 Checkpoint 成功且事务提交后才能读取。

核心故障实验只重启 TaskManager，保留 JobManager、Checkpoint 和 Kafka，
观察运行中的作业自动恢复。整个集群重新启动需显式找到持久化 Checkpoint 并恢复作业；
脚本不把“重新提交一个全新作业”伪装成状态恢复。

## 5. Spring 消费与 PostgreSQL 一致性

采用 Spring JDBC 和参数化 SQL，不引入 ORM。
用可重复执行的迁移管理两张表：

| 表 | 作用 |
| --- | --- |
| processed_event | 保存处理器名、resultId、shopId、版本与处理时间；(handler_name, event_id) 唯一 |
| shop_payment_stat | shop_id 主键，累计金额、支付数、version 和修改时间 |

结果消费流程：

1. Listener 同步调用独立事务服务，避免同类调用导致事务代理失效。
2. 同一 PostgreSQL 事务内，尝试 INSERT processed_event，指定唯一冲突键并 DO NOTHING。
3. 只有真正插入时才执行结果 Upsert；已有事件直接作为重复处理返回。
4. Upsert 用 PostgreSQL ON CONFLICT DO UPDATE，并原子判断新 version 大于当前 version。
5. 旧版本事件可以被登记为已处理，但不能覆盖新的统计值。
6. 本地事务提交后 Listener 才正常返回，由容器提交已处理的 Kafka 进度。

禁止自动提前提交 offset；单分区不异步乱序处理，也不提交越过失败记录的进度。
可重试数据库错误必须重投；教学应用不配置“重试耗尽后静默跳过并提交”。
非法消息明确记录并阻止其分区继续确认，教程说明如何定位；
不额外引入死信 Topic 和补偿平台。

本地事务仅覆盖这两张表，不声称 Kafka offset 与 PostgreSQL 组成同一个原子事务。
数据库已提交但消费进度未提交时允许重投，通过唯一约束消除重复业务效果。
去重表在实验生命周期内保留，不自动清理。

Flink 的业务输入去重、Spring 的输出重投去重、结果版本检查分别解决三个问题；
教程不将它们混为一谈。

## 6. 接口与教学控制

提供三个业务接口：

- POST /api/payments：校验并发布支付事件。
- GET /api/shops/{shopId}/stats：查询金额、支付数和版本。
- GET /api/processed-events/count：查询教学处理器的已处理事件数，供核对去重效果。

不存在的店铺统计返回 404；输入不合法返回 400；
Kafka 不可用或发送超时返回清晰错误，避免报告虚假成功。

故障注入仅在 lab profile 下开启，服务绑定回环地址：

- 可指定某个 resultId，设置“一次性在数据库提交成功后、Listener 返回前抛异常”。
- 一次性标志先消耗再抛出；随后重投可以完成，避免无限重复注入。
- 不使用远程杀进程接口，也不对系统中的其他 Java 进程操作。
- 通过 lab 模式结果注入入口，将指定的历史累计结果发送到结果 Topic；
  只允许测试消息并保持 shopId 为 Kafka key，用于验证去重与防旧值覆盖。

数据库后置故障使用可控异常复现提交间隙，不冒充真实进程崩溃；
Flink 实验则通过重启本项目 TaskManager 容器演示实际运行时中断。

## 7. 学习章节与验收结果

每章包含业务问题、阅读代码位置、PowerShell 操作、观察位置、期望值和原理解释。
所有实验使用明确的独立 shopId，脚本有超时轮询，避免依赖固定睡眠或手工点击时机。

| 章节 | 操作 | 验收 |
| --- | --- | --- |
| 1. 跑通链路 | A 支付 10000 分、B 支付 5000 分、A 支付 8000 分 | A=18000/2笔/version2，B=5000/1笔/version1 |
| 2. 业务重复 | 原样重发 A 的第一笔支付 | 金额、支付数、版本均不变 |
| 3. 事务可见性 | 将间隔调整为 30 秒，发新支付，观察计算日志和消费日志 | 解释结果先算出、后提交可见；不保证固定在第30秒完成 |
| 4. Flink 恢复 | 确认成功 Checkpoint，持续发送有限事件，重启 TaskManager | 状态恢复后最终金额与唯一事件总和一致 |
| 5. 消费幂等 | 为下一结果设置数据库提交后的一次性故障 | 出现重投，但同一结果只登记一次，统计保持正确 |
| 6. 旧结果 | 独立店铺先注入version3，再注入此前未处理的version2 | version2被登记但统计保持version3；区别于重复ID被拦截 |

第三章调整配置时从已有状态恢复，或使用独立的干净实验环境；
不通过无状态重启现有作业来改变间隔。
第四章只断言最终一致和恢复证据，不宣称每次重启必然命中同一个内部故障窗口。

## 8. 测试与交付

自动化测试覆盖：

- 输入校验与消息序列化契约。
- Flink 去重、累计、版本及状态快照恢复后的输出。
- PostgreSQL 真实唯一约束和条件 Upsert，不用 H2 替代 PostgreSQL 方言。
- 同一数据库事务中登记与业务写入的回滚。
- 数据库提交后的消费者失败与重投。
- 旧版本、重复结果、首次结果的区别。

端到端脚本连接真实 Docker 服务，验证第一章、第二章和故障注入结果。
Flink 故障恢复实验记录实际运行的证据；环境或网络限制导致未运行的步骤必须明确标注。

交付包含根 POM、两个模块、Compose、数据库迁移、PowerShell 启动/验证脚本、
中文 README 与逐章实验文档。
脚本检查外部命令退出码并提供超时失败信息，不静默继续。
构建依赖、Flink 作业提交、查看 Web UI 和 IDEA 启动参数都写明。
停止脚本默认保留数据，清空数据作为单独的显式操作，且只作用于本项目资源。

设计审核通过后，下一步编写实施计划；计划确认执行方式后开始实现和验证。

# 支付实时统计学习项目 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 构建并实际验证 Spring Boot → Kafka → Flink → Kafka → PostgreSQL 的本机学习项目，通过六个实验理解状态、事务、恢复和幂等。

**Architecture:** 一个 Maven 父工程包含 payment-service 与 flink-job 两个子模块。Docker Compose 承载基础设施和独立 Flink 集群，Spring Boot 在本机运行；JSON 事件连接各组件，PostgreSQL 保存可查询的累计结果。

**执行说明：** 用户选择当前会话顺序实现。下方保留原始步骤设计；最终执行结果见 `docs/verification.md`，逐项调整与验证证据见 `.superpowers/sdd/2026-10-20-payment-stream-learning/progress.md`。集成测试合并到 `PaymentFlowIT`；PostgreSQL实际使用同系列17.10固定摘要；故障注入新增只读观察接口用于脚本验收。

**交付验收（2026-10-20）：**

- [x] Task 1–2：工程、JSON契约与Docker基础设施。
- [x] Task 3–4：Flink有状态聚合与Spring支付接口。
- [x] Task 5–6：PostgreSQL事务、消费去重和故障注入。
- [x] Task 7：六章实际实验、重复启动保护及带状态恢复。
- [x] Task 8：教程、独立审查修正、最终18项测试全部通过。

**Tech Stack:** Java 17 字节码、本机 Java 21、Spring Boot 3.5.7、Flink 1.20.3、Flink Kafka Connector 3.4.0-1.20、Kafka 3.9.1、PostgreSQL 17.6、JUnit 5、Testcontainers 2.0.2、PowerShell。

**Spec:** `docs/superpowers/specs/2026-10-20-payment-stream-learning-design.md`，已获用户确认。

## Global Constraints

- 用户已确认：Docker 运行 Kafka、PostgreSQL、Flink；Spring Boot 在 IDEA 或本机 Maven 中运行。
- 当前目录未初始化 Git，已有 IDEA 工程配置，不覆盖或调整该配置。
- 代码以 Java 17 字节码为目标；本机 Java 21 运行 Spring，Flink 容器使用 Java 17。
- 金额一律用 long 表示“分”，要求正整数，并检查累计溢出。
- Kafka 两个 Topic 均使用 shopId 作为消息 key；各使用两个分区，Flink 并行度设为 2。
- 默认每 10 秒触发 Checkpoint，最大并发数 1，超时 60 秒。
- Kafka Sink 显式启用 EXACTLY_ONCE；事务超时设置为 10 分钟。
- Spring 结果消费者显式配置 read_committed；禁止自动提前提交 offset。
- 同一 PostgreSQL 事务内完成事件登记和统计更新；Upsert 原子判断新 version 大于当前 version。
- 业务去重身份为 (shopId, eventId)，累计结果身份为 (shopId, version)，不使用当前时间生成去重身份。
- 故障注入仅在 lab profile 下开启，服务绑定回环地址。
- 不使用 H2 替代 PostgreSQL 方言；不加入窗口、退款、登录、UI、死信平台或公共代码模块。
- Java 使用中文注释、SLF4J、参数化 SQL，遵循用户的最小实现原则；不为单一实现机械增加接口。
- 此目录不是 Git 仓库，因此不执行 git init 或伪称提交完成；若用户另行初始化，才在通过验证的任务边界提交。

## Review Focus

1. HTTP 或 Kafka 输入包含缺失 ID、非法字符、零/负金额或累计溢出：不能产生无效统计；Task 1、3、4 验证。
2. 相同事件 ID 载荷变化或重试改变店铺：去重范围和冲突行为必须明确；Task 3 测试同店冲突与跨店独立。
3. 数据库成功而 offset 未提交：重复投递必须跳过业务修改，且错误处理不能耗尽重试后静默跳过；Task 6 验证。
4. 旧累计值首次到达而非重复 ID：也不能覆盖新版本；Task 5、7 分别验证数据库和完整消费链路。
5. 重启脚本造成新旧两个作业同时输出，或在旧结果库上无状态重跑：启动脚本必须检测并拒绝危险组合；Task 2、7 验证。

## 文件职责与约定

Java 根包分别为 `com.example.payment` 和 `com.example.flink`。
下文路径缩写：

- `S` = `payment-service/src/main/java/com/example/payment`
- `ST` = `payment-service/src/test/java/com/example/payment`
- `F` = `flink-job/src/main/java/com/example/flink`
- `FT` = `flink-job/src/test/java/com/example/flink`

每个 Java 类型一个文件。两个模块各自定义两种消息 DTO，通过根目录 contracts 的 JSON 样例验证契约。
Flink 状态对象使用显式 POJO/基础类型，避免将不可变 record 直接作为需依赖反射的托管状态。
Spring DTO 可使用 record，但必须满足本计划字段和校验要求。

固定协议：

```text
PaymentEventDTO(eventId:String, shopId:String, amountCents:Long, occurredAt:String)
ShopTotalDTO(resultId:String, shopId:String, totalAmountCents:Long,
             paymentCount:Long, version:Long, triggerEventId:String)
```

occurredAt 为 ISO-8601 UTC instant 字符串，入站解析校验后保留一致规范形式。
ID 正则 `[A-Za-z0-9_-]{1,64}`；resultId 为 `shopId + ":" + version`。
累计结果必须 version > 0、paymentCount = version、金额 > 0，字段缺失拒绝处理。

脚本统一通过 `$PSScriptRoot` 定位工程，不依赖调用者工作目录；
使用 native exit code 检查与有限超时轮询，后台进程不得弹出可见窗口。

---

## Task 1: 建立可编译的工程和消息契约

**Files:**
- Create: `pom.xml`, `.gitignore`, `payment-service/pom.xml`, `flink-job/pom.xml`
- Create: `contracts/payment-event.json`, `contracts/shop-total.json`
- Create: `S/PaymentApplication.java`, `S/message/PaymentEventDTO.java`, `S/message/ShopTotalDTO.java`
- Create: `F/message/PaymentEventDTO.java`, `F/message/ShopTotalDTO.java`, `F/message/EventJson.java`
- Test: `ST/message/MessageContractTest.java`, `FT/message/MessageContractTest.java`

**Interfaces:**
- Produces: 两个模块的上述 DTO；`EventJson.readPayment(String): PaymentEventDTO`、
  `EventJson.writeTotal(ShopTotalDTO): String`。
- Fixtures: A 的 payment-001 金额 10000、occurredAt 为 `2026-10-20T10:00:00Z`；
  A 的结果 ID 为 `A:1`、金额10000、paymentCount=version=1、triggerEventId=payment-001。

- **Step 1:** 创建最少的 POM 与契约测试，测试读取同一根目录 fixtures 并断言每个字段；DTO 尚不存在时先记录编译失败。
- **Step 2:** 执行 `mvn -B test`，确认失败原因是待实现类型，不是下载失败。
- **Step 3:** 实现 DTO 和 JSON 编解码；测试非法时间、缺失金额和未知消息结构被拒绝。
- **Step 4:** 配置父 POM release=17、UTF-8、JUnit5/Surefire；Spring 模块导入 Boot BOM，
  Flink 模块独立管理 Jackson/Flink 依赖，避免 Boot BOM 覆盖 Flink 自带运行时。
  Flink framework 依赖 provided；Kafka connector 打入 shaded jar，排除签名文件并保留服务描述。
- **Step 5:** 执行 `mvn -B test`、`mvn -B dependency:tree`。
  通过条件：契约测试通过，Flink 部署包不包含 Flink 核心框架或 Spring Boot，两个模块不相互依赖。

仅选择稳定发布依赖。若网络或镜像可用性需要替换补丁版本，保持已确认的系列并记录实际版本和兼容性证据，
不得因为下载困难静默跨大版本升级。

## Task 2: 可启动的本地基础设施

**Files:**
- Create: `compose.yaml`, `.env.example`
- Create: `scripts/common.ps1`, `scripts/start-infra.ps1`, `scripts/stop-infra.ps1`
- Create: `infra/init-topics.sh`

**Interfaces:**
- Compose project: `flink-payment-lab`
- 服务名：`kafka`, `postgres`, `jobmanager`, `taskmanager`，以及一次性初始化 Topic 的 `init-topics`。
- 主机端口：Kafka `127.0.0.1:19092`、PG `127.0.0.1:15432`、Flink REST `127.0.0.1:18081`。
- 容器网络 Kafka 地址 `kafka:9092`；PG 库 `payment_lab`；用户名/密码仅为本地教学配置。
- 镜像：`apache/kafka:3.9.1`, `postgres:17.6`, `flink:1.20.3-scala_2.12-java17`。
- Named volumes：Kafka 数据、PG 数据、Flink Checkpoint（JM/TM 统一挂载 `/opt/flink/checkpoints`）。
- `Invoke-LabCommand` 检查 native 退出码，`Wait-LabCondition` 有超时和最后一次失败信息。

- **Step 1:** 检查三个精确镜像 tag 和已占用端口；不停止任何非本项目容器。
- **Step 2:** 编写 Compose，Kafka 单节点 KRaft、事务状态日志副本数/minISR=1、
  broker 事务超时上限至少600000ms，设置内外 listener；JM/TM设置2个slot和可写持久化卷。
- **Step 3:** `docker compose -p flink-payment-lab config --quiet` 必须成功；
  初始化脚本用 LF，幂等创建 `payment-events` 与 `shop-payment-totals`，分区数2、副本数1。
- **Step 4:** 实现 `start-infra.ps1`：检查依赖、启动、轮询健康、检查 Topic 分区数与注册slot数；
  缺失镜像或端口冲突返回非零且保留原始错误。
- **Step 5:** 实现 `stop-infra.ps1`：只停止本项目，不带 `--volumes`；
  连续两次启动不会重复创建资源或改分区数。
- **Step 6:** 实际运行启动脚本，验证 Kafka Topic 描述、`pg_isready`、
  `GET http://127.0.0.1:18081/overview`；停止再启动后持久化卷仍在。

Task 7 负责作业级的重复启动及恢复检查，此任务不自动提交 Flink 作业。

## Task 3: 可恢复的 Flink 支付聚合

**Files:**
- Create: `F/PaymentAggregationJob.java`, `F/JobOptions.java`
- Create: `F/aggregate/PaymentAggregateFunction.java`, `F/aggregate/ShopAggregateState.java`
- Test: `FT/aggregate/PaymentAggregateFunctionTest.java`, `FT/JobOptionsTest.java`
- Modify: `flink-job/pom.xml`

**Interfaces:**
- `PaymentAggregateFunction extends KeyedProcessFunction<String, PaymentEventDTO, ShopTotalDTO>`。
- `processElement(PaymentEventDTO, Context, Collector<ShopTotalDTO>)` 按事件更新或跳过。
- 作业参数：`--bootstrap-servers`, `--checkpoint-interval-ms`（默认10000）、
  `--checkpoint-dir`（默认file:///opt/flink/checkpoints）、`--transactional-id-prefix`（默认payment-lab-totals-）。
- 作业名 `payment-aggregation`；source/aggregate/sink UID 分别 `payment-source`、`shop-aggregate`、`totals-sink`。

- **Step 1:** 写 KeyedOneInputStreamOperatorTestHarness 测试：
  A10000、B5000、A8000得到A18000/version2和B5000/version1；
  同事件重发无输出；A相同eventId变金额或时间抛明确异常；B相同eventId视为独立事件。
- **Step 2:** 先运行 `mvn -B -pl flink-job -am test` 记录缺失实现失败。
- **Step 3:** 实现 `MapState<String, PaymentEventDTO>` 去重和 `ValueState<ShopAggregateState>` 累计，
  先校验和 `Math.addExact` 计算成功再写状态；不设置TTL；生成确定性resultId。
- **Step 4:** 增加恢复测试：保存A18000时的快照，再处理20和30元；重新创建算子并载入快照，
  重放后输出版本和值与第一次一致，A最终23000；快照前重复事件在恢复后仍被过滤。
  增加溢出测试，拒绝溢出且不污染当前状态。
- **Step 5:** 实现 KafkaSource（earliest初始位置）、JSON转换、keyBy、KafkaSink。
  显式设置EXACTLY_ONCE、事务超时600000、parallelism2、checkpoint timeout60000/max concurrent1、
  retained checkpoints及固定延时重启策略（10次、间隔3秒）。
  Sink key使用shopId；Checkpoint目录与UID稳定。
- **Step 6:** 执行单元测试和 `mvn -B -pl flink-job -am package`；
  检查生成的 `flink-job/target/flink-job.jar`，记录后续提交入口类。

如宿主Java21对测试harness有模块访问限制，只给测试JVM添加所需open参数；
真正作业运行仍使用Java17，不用忽略测试来规避。

## Task 4: Spring 支付事件发布接口

**Files:**
- Create: `S/payment/PaymentController.java`, `S/payment/PaymentPublisher.java`
- Create: `S/payment/PublishReceiptDTO.java`, `S/web/ApiExceptionHandler.java`
- Create: `payment-service/src/main/resources/application.yml`
- Test: `ST/payment/PaymentControllerTest.java`, `ST/payment/PaymentPublisherTest.java`

**Interfaces:**
- `PaymentPublisher.publish(PaymentEventDTO): PublishReceiptDTO`。
- `POST /api/payments`：200响应含eventId、topic、partition、offset和状态`ACCEPTED`；
  表示Kafka确认接收，不表示Flink或数据库已完成。
- 默认HTTP `127.0.0.1:18080`；Kafka `127.0.0.1:19092`。

- **Step 1:** 编写MVC参数测试：空ID、ID含冒号、过长ID、null/零/负金额、非法occurredAt返回400且未调用Producer。
- **Step 2:** 编写Publisher测试：future未确认时不返回成功；成功绑定shopId key；
  发送失败返回503；超时返回504并说明“结果不确定，使用同一事件重试”。
- **Step 3:** 运行 `mvn -B -pl payment-service -am test` 确认失败，再实现Controller、Publisher及异常响应。
  Kafka Producer显式设置acks=all、enable.idempotence=true，
  使用String JSON序列化避免Java类型头耦合；等待发送确认最多10秒。
- **Step 4:** 重新运行本任务测试。确认Controller没有写数据库，eventId没有被替换为随机ID。

## Task 5: PostgreSQL 事务去重与版本更新

**Files:**
- Create: `payment-service/src/main/resources/db/migration/V1__create_payment_stats.sql`
- Create: `S/stat/ShopStatDTO.java`, `S/stat/ShopStatRepository.java`, `S/stat/ShopStatController.java`
- Create: `S/consumer/ProcessingOutcomeEnum.java`, `S/consumer/ResultProcessingService.java`
- Test: `ST/consumer/ResultProcessingServiceIT.java`
- Modify: `payment-service/pom.xml`, `payment-service/src/main/resources/application.yml`

**Interfaces:**
- `ResultProcessingService.process(ShopTotalDTO): ProcessingOutcomeEnum`（APPLIED、DUPLICATE、STALE）。
- `ShopStatRepository.find(String shopId): Optional<ShopStatDTO>`、
  `countProcessed(): long`、`insertProcessed(ShopTotalDTO): boolean`、`upsertIfNewer(ShopTotalDTO): boolean`。
- `GET /api/shops/{shopId}/stats`返回shopId、totalAmountCents、paymentCount、version；不存在404。
- `GET /api/processed-events/count`返回`{"count": n}`。
- handler_name 固定 `shop-total-projection-v1`；事件表unique(handler_name,event_id)。

- **Step 1:** 用Testcontainers PostgreSQL17.6写集成测试；容器不可用时失败，不静默skip。
  `mvn verify`通过Failsafe运行`*IT`，普通`mvn test`不要求Docker。
- **Step 2:** 测试首次A:1写10000、重复A:1只登记一次、首次到达的A:3覆盖为23000、
  随后首次到达A:2登记成功但金额和版本仍为23000/3。
- **Step 3:** 运行 `mvn -B -pl payment-service -am verify` 记录待实现失败。
- **Step 4:** 实现Flyway迁移、JdbcTemplate仓储与独立代理事务服务。
  insertProcessed显式指定冲突键，只有插入成功才执行条件Upsert；
  PostgreSQL WHERE incoming.version > current.version 必须在同一条Upsert语句内。
- **Step 5:** 增加真实回滚测试：外层TransactionTemplate调用process后主动抛异常，
  验证登记与统计均不存在。增加两个并发事务处理同一resultId的测试，断言只有一个APPLIED。
- **Step 6:** 检查resultId与shopId/version一致、paymentCount=version及必填字段；
  测试无效结果不会留下已处理标记。运行verify全部通过。

Testcontainers使用2.0.2 BOM及对应模块名；若启动失败先诊断Docker API/网络，
禁止改为H2或删除事务测试。

## Task 6: Spring Kafka 消费、重投与故障注入

**Files:**
- Create: `S/config/KafkaConsumerConfiguration.java`, `S/consumer/ShopTotalListener.java`
- Create: `S/lab/LabFaultInjector.java`, `S/lab/LabController.java`
- Create: `payment-service/src/main/resources/application-lab.yml`
- Test: `ST/consumer/ShopTotalListenerTest.java`, `ST/consumer/ConsumerRecoveryIT.java`, `ST/lab/LabProfileTest.java`

**Interfaces:**
- `ShopTotalListener.onMessage(ConsumerRecord<String, String>): void`。
- `LabFaultInjector.arm(String resultId): void`、`failOnceIfArmed(String resultId): void`。
- `POST /api/lab/fail-after-commit` body `{"resultId":"lab-failure:1"}`。
- `POST /api/lab/results` body ShopTotalDTO，发送到shop-payment-totals，要求shopId以`lab-`开头。
- lab入口与故障bean仅在lab profile存在；Listener通过可选注入访问，不要求默认profile有故障bean。

- **Step 1:** Listener测试验证调用顺序：解析校验 → 事务服务返回 → 故障注入；
  任一步失败都抛出异常，不正常返回；Kafka key与载荷shopId不一致时拒绝。
- **Step 2:** 启用StringDeserializer、enable.auto.commit=false、isolation.level=read_committed、
  AckMode.RECORD、concurrency=2、固定consumer group `payment-lab-projector`。
  使用DefaultErrorHandler配置无限可重试异常、固定1秒backoff；
  明确覆盖默认不可重试分类，避免反序列化/校验错误被默认recoverer跳过。
- **Step 3:** 实现同步Listener与lab故障：transaction service返回即代表事务提交完成，
  原子消耗一次性armed ID后抛出专用实验异常。日志包含resultId、outcome和重投信息。
- **Step 4:** 写真实Kafka3.9.1与PostgreSQL17.6容器测试：
  arm A:1，发送结果；等待数据库出现A:1、观察Listener至少两次尝试，
  最终offset推进且processed_event仅1行。用有限轮询，不以固定sleep断言完成。
- **Step 5:** 增加错误记录测试：非法结果后紧跟同分区正常记录，确认非法记录未被确认，
  正常记录未越过它；测试环境有界停止容器并保留日志。
- **Step 6:** 验证非lab profile下两个lab端点404；执行 `mvn -B -pl payment-service -am verify`。

这一步不通过强杀Spring进程实现注入；文档如实称为提交间隙异常实验。

## Task 7: 启动作业与完整实验脚本

**Files:**
- Create: `scripts/start-service.ps1`, `scripts/submit-job.ps1`, `scripts/run-lab.ps1`
- Create: `scripts/verify-lab.ps1`, `scripts/restart-taskmanager.ps1`
- Modify: `scripts/common.ps1`, `compose.yaml`

**Interfaces:**
- `submit-job.ps1 [-CheckpointIntervalMs 10000] [-RestorePath <checkpoint-or-savepoint-path>]`
  打包/复制jar后提交已定义的入口类；有同名运行作业时直接报告现有JobID，不重复提交。
- `start-service.ps1`前台执行Spring Boot lab profile，方便IDEA之外启动和看日志。
- `run-lab.ps1 -Chapter <1..6> -RunId <短ID>`使用带前缀独立店铺；默认RunId可生成一次并打印保存。
- `verify-lab.ps1`自动执行正常链路、重复支付、消费重投及旧版本实验。

- **Step 1:** submit脚本先校验本项目REST作业列表。
  没有运行作业但payment-events已有数据时，必须提供RestorePath，
  不允许对旧数据直接提交earliest且空状态的新作业。
  参数变化而已有作业运行时报告需要恢复重提，不伪称配置已变更。
- **Step 2:** 通过 `docker compose cp`复制jar后用容器flink CLI提交；
  轮询作业RUNNING、Checkpoint成功和消费者就绪。失败时输出该项目日志，不清空数据。
- **Step 3:** Chapter1发送A10000/B5000/A8000并轮询统计；
  Chapter2原样重发第一条，确认版本仍为2。
  使用同一RunId连续执行两章，fixtures和返回offset写入本次实验记录。
- **Step 4:** Chapter3提供“stop with savepoint → 从该路径以30000ms间隔恢复”的明确操作；
  保存路径通过Flink返回值确认，轮询新Checkpoint。观察应用计算与read_committed消费时间，
  不断言恰好30秒，结束后同样恢复到10000ms。
- **Step 5:** Chapter4先记录已完成Checkpoint ID及路径，再发送有限的唯一事件序列，
  从发送循环中在确定位置执行本项目TaskManager restart，继续发完。
  轮询作业恢复、成功Checkpoint、最终唯一事件总和与计数；记录恢复信息。
- **Step 6:** Chapter5使用独立店铺下一resultId布置提交后故障，再发支付，核对去重表与offset；
  Chapter6在独立lab店铺注入version3和未出现过的version2，核对防旧版本覆盖。
- **Step 7:** 实际执行 `mvn -B verify`、`scripts/verify-lab.ps1` 和Chapter4，
  确认所有验收值。复跑相同RunId不能意外重复累计，启动脚本第二次执行不能增加Flink作业。

PowerShell操作只针对Compose project及本次运行记录的服务，不扫描或杀掉其他Java进程。
教程给出显式 `docker compose -p flink-payment-lab down --volumes` 重建方式，
自动验证不调用它；删除前必须说明会丢失本项目所有实验数据。

## Task 8: 教程、验证记录与交付审查

**Files:**
- Create: `README.md`, `docs/message-contracts.md`, `docs/labs.md`, `docs/verification.md`
- Modify: `.env.example`、各步骤新增文件中的必要说明

- **Step 1:** README写从零启动顺序：构建/测试 → 基础设施 → 本机Spring → 提交Flink → Chapter1。
  列出IDEA运行类、lab profile、端口、数据目录、Flink Web UI、停止及带状态恢复方式。
- **Step 2:** labs按已确认六章组织，每章写业务问题、精确代码路径、PowerShell命令、
  输入、可观察日志/REST/SQL、预期值与原理；解释金额分单位及所有查询返回字段。
- **Step 3:** contracts写JSON示例、ID稳定性、载荷冲突、店铺内顺序、
  resultId/version只在持续状态历史内有效，以及三个去重/版本职责。
- **Step 4:** 记录真实执行命令、通过数、实验结果、使用版本及环境限制。
  只报告执行过的验证；不把单broker副本数1称为生产高可用，不把故障注入称为真实kill。
- **Step 5:** 核对spec全部验收项，审查事务边界、错误分类、旧值条件、并行顺序、
  sink事务配置与持久化Checkpoint权限；发现问题先补复现测试再修正。
- **Step 6:** 最后仅对变更影响的检查重跑；交付可点击README和主要入口文件，
  简述已通过测试、已运行实验、尚存限制，并从第一章开始带用户操作。

## 自审与执行交接

规格1–2对应Task1/2/8；规格3–4对应Task1/3/7；规格5–6对应Task4/5/6；
规格7对应Task7/8；规格8对应所有任务的测试与Task8验证记录。
五项Review Focus均已落到具体测试或运行检查。

推荐由当前会话顺序执行：两个模块的消息格式、事务时序、恢复脚本彼此依赖，
顺序实现更便于同步解释和调试。可选子代理逐任务实现与审查，但不在计划审核阶段启动代理。
用户审核本计划并选择执行方式后，进入相应执行技能。

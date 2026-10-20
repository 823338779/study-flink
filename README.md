# Flink 支付实时统计：从 Java 后端到流处理

这个项目用模拟支付带你观察“业务事件 → 持续计算 → 事务输出 → 幂等落库”。
你不需要真实支付账户。金额全部是整数**分**，`10000` 表示100元。

```text
POST /api/payments
  → Spring Boot 等待 Kafka 接收确认
  → payment-events（key=shopId）
  → Flink：分组、去重、更新 State、输出累计值
  → shop-payment-totals（Kafka 事务）
  → Spring：read_committed、数据库事务内去重、版本检查
  → PostgreSQL
  → GET /api/shops/{shopId}/stats
```

## 运行环境

| 组件 | 版本 / 地址 |
| --- | --- |
| 本机 | JDK21、Maven3.9、Docker Desktop Linux containers、PowerShell |
| Spring Boot | 3.5.7，http://127.0.0.1:18080 |
| Flink | 1.20.3 / Java17，http://127.0.0.1:18081 |
| Kafka Connector | 3.4.0-1.20 |
| Kafka | 3.9.1，localhost:19092 |
| PostgreSQL | 17.10 Alpine，localhost:15432；镜像锁定SHA256 |

这是一套单机学习配置，Kafka单副本不提供生产级高可用。
Docker需有足够资源同时运行Kafka、PostgreSQL和两个Flink进程，建议分配至少6GB内存。
运行集成测试还会临时创建隔离的Kafka/PG容器；不足时先停止学习集群再跑测试。
端口均绑定回环地址；不会接管本机已有的5432数据库。

## 第一次启动

在项目根目录打开PowerShell：

```powershell
# 包含真实Kafka/PostgreSQL集成测试，需要Docker运行。
mvn -B verify
# 仅单元测试：mvn -B test

.\scripts\start-infra.ps1
```

再开一个终端运行Spring（保持终端运行）：

```powershell
.\scripts\start-service.ps1
```

或者在IDEA导入根`pom.xml`，运行：

- 模块：`payment-service`
- 主类：`com.example.payment.PaymentApplication`
- JRE：21
- 程序参数：`--spring.profiles.active=lab`
- Working directory：项目根目录

回到第一个终端：

```powershell
# 首次提交必须在发送支付之前。已有数据时不能随意从空状态重跑！
.\scripts\submit-job.ps1
.\scripts\run-lab.ps1 -Chapter 1 -RunId lesson1
.\scripts\run-lab.ps1 -Chapter 2 -RunId lesson1
```

第1章完成后应看到：

```text
lab-lesson1-A：totalAmountCents=18000，paymentCount=2，version=2
lab-lesson1-B：totalAmountCents=5000， paymentCount=1，version=1
```

第2章重复发送第一笔支付，A的金额、计数和版本都不变。
这是业务去重；输入Topic里仍然可以存在两条相同业务事件。

## 代码阅读顺序

1. `payment-service/.../payment/PaymentPublisher.java`：只发Kafka，不双写业务数据库。
2. `flink-job/.../PaymentAggregationJob.java`：整条数据流水线、Checkpoint、Kafka事务。
3. `flink-job/.../aggregate/PaymentAggregateFunction.java`：同店铺状态、业务去重、版本。
4. `payment-service/.../consumer/ShopTotalListener.java`：提交数据库后才能返回。
5. `payment-service/.../consumer/ResultProcessingService.java`：去重登记和结果更新的同一个事务。
6. `payment-service/.../stat/ShopStatRepository.java`：唯一约束和条件Upsert分别解决什么。

DTO没有抽成公共模块，两个服务只通过JSON契约耦合。
代码里的中文注释说明了事务和恢复边界；[消息契约](docs/message-contracts.md)定义字段。

## 手工提交与查询

```powershell
$event = @{
    eventId='manual-001'
    shopId='manual-shop'
    amountCents=2000
    occurredAt='2026-10-20T10:00:00Z'
}
Invoke-RestMethod http://127.0.0.1:18080/api/payments -Method Post `
    -ContentType application/json -Body ($event | ConvertTo-Json)

# 刚发送时可能404：输入已接收，统计事务还没提交。
Invoke-RestMethod http://127.0.0.1:18080/api/shops/manual-shop/stats
```

支付响应`ACCEPTED`只说明Kafka已确认接收，不能当成已落库。
超时可能是确认丢失，重试时必须保留相同eventId、shopId、金额和发生时间。

## 接下来做什么

详见[六章实验](docs/labs.md)：

| 章 | 学习内容 |
| --- | --- |
| 1 | Source、keyBy、State、Sink与累计值 |
| 2 | 相同业务事件重复发送 |
| 3 | 30秒Checkpoint与Kafka事务可见性 |
| 4 | 重启TaskManager，恢复状态并重放 |
| 5 | 数据库已提交、消费进度未提交时失败 |
| 6 | 新版本先到，旧结果不能覆盖 |

正常链路和消费端验证：

```powershell
.\scripts\verify-lab.ps1
```

## 停止、恢复与清空

Spring本机进程用运行终端Ctrl+C停止。基础设施停止默认保留数据：

```powershell
.\scripts\stop-infra.ps1
```

**整套Flink集群重启不会自动替你重新提交作业。** 重启前在Flink UI记录最新成功Checkpoint的External Path，
或者使用第3章stop-with-savepoint脚本生成`.lab/last-savepoint.txt`。
重新启动基础设施和Spring后：

```powershell
.\scripts\submit-job.ps1 -RestorePath 'file:/opt/flink/checkpoints/<job-id>/chk-<n>'
```

请替换为实际路径。没有匹配快照时不要对旧Topic和旧结果库提交空状态作业；
脚本会阻止这种做法。TaskManager重启与整个集群重启不同，见第4章。

本项目Kafka Sink事务超时为10分钟。超时应覆盖Checkpoint间隔、快照耗时和预计故障恢复时间；
如果恢复时某个待提交事务已经被Kafka超时中止，不能仅凭“快照文件还在”就承诺输出无损。
计划长时间停机时，应先完成stop-with-savepoint、确认作业正常停止且输出已提交，
记录本次停止生成的路径，再关闭集群；不要把历史Savepoint当作当前停止点。
对应原理见[Flink Kafka故障容错文档](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/datastream/kafka/#fault-tolerance)。

计划停机可在关闭基础设施之前执行：

```powershell
# 从Flink Web UI复制当前Job ID；等待命令成功，保存它返回的Savepoint路径。
docker compose exec jobmanager /opt/flink/bin/flink stop `
    --savepointPath file:///opt/flink/checkpoints/savepoints <Job-ID>
```

如果想从头开始，先停止本机Spring，再**明确执行下面的清空命令**：

```powershell
# 删除本项目的全部Kafka事件、数据库数据和Checkpoint；不可恢复。
docker compose -p flink-payment-lab down --volumes
```

它仅针对此项目资源。清空后按首次启动顺序重建，不能只清Flink状态或只清数据库。

## 排障入口

- 下载失败：重试`docker compose pull`或`mvn -U verify`，不要把下载失败当成测试失败。
- 找不到统计：检查Flink作业是否RUNNING、是否有成功Checkpoint，再看Spring消费日志。
- 关键日志：`CALCULATED`、`PAYMENT_DUPLICATE`、`DATABASE_COMMITTED`、`INJECT_AFTER_COMMIT`。
- 查看Flink日志：`docker compose logs --tail 100 taskmanager`。
- 查看Kafka：`docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group payment-lab-projector --describe`。
- 查看PG：`docker compose exec postgres psql -U lab -d payment_lab`。
- 错误消息会阻止分区继续确认，不会静默丢弃；实验数据损坏时保留日志，修正输入来源后按明确的重建步骤重做。
- 人工改统计表不会改变Flink State，下次计算可能覆盖它。正常实验不要手工修改结果表。

默认配置使用教学密码。若复制`.env.example`为`.env`并修改密码，需要给IDEA中的Spring设置相同的
`POSTGRES_USER`、`POSTGRES_PASSWORD`环境变量；Compose的`.env`不会自动传给本机Java进程。

[实际验证记录](docs/verification.md)区分已经运行的实验与环境限制，不把示例代码等同于生产保证。

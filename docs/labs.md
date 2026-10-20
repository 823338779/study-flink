# 六章实验

先按根README启动Spring、Kafka、PG和Flink。默认Checkpoint间隔10秒，Kafka输出事务提交前，
Spring的read_committed消费者读不到该批结果。每章均可在Spring日志和Flink Web UI对照观察。

## 1. 先看数据怎么流

```powershell
.\scripts\run-lab.ps1 -Chapter 1 -RunId lesson1
```

输入：A10000分、B5000分、A8000分。预期A=18000/2笔/version2，B=5000/1笔/version1。

阅读`PaymentAggregationJob`和`PaymentAggregateFunction`：
同店铺通过keyBy进入同一并行实例，每个店铺有自己的MapState和ValueState。
观察`CALCULATED`日志，它表示已经计算，不表示数据库已提交。
Spring输出`DATABASE_COMMITTED`之后，通过查询接口可读到结果。

```sql
SELECT shop_id, total_amount_cents, payment_count, version FROM shop_payment_stat ORDER BY shop_id;
```

思考：A的10000和18000都是累计快照，为什么下游不能再把它们相加？

## 2. 重复支付

```powershell
.\scripts\run-lab.ps1 -Chapter 2 -RunId lesson1
```

必须与第1章使用同一个RunId。原样重发payment-001，A仍为18000/2笔/version2。
Flink日志出现`PAYMENT_DUPLICATE`，不会输出A的version3。
Checkpoint和Exactly-once不会识别两条业务上重复、但Kafka offset不同的输入；
这个判断由MapState中的业务身份完成。

## 3. 为什么Flink已经算了，数据库还没变化

```powershell
.\scripts\run-lab.ps1 -Chapter 3 -RunId visible1
```

脚本通过stop-with-savepoint停止作业，从保存路径恢复并将间隔设为30000ms；
实验后以同样方式改回10000ms。恢复路径保存在`.lab/last-savepoint.txt`。
停止和恢复期间短暂不能处理新消息；Kafka保留输入。

对比`CALCULATED`与`DATABASE_COMMITTED`时间，以及Web UI的Checkpoint触发、完成时间。
延迟不保证恰好30秒：消息可能落在周期中的任意位置，Checkpoint和事务提交也需要时间。
Kafka发送和消费者可见是两件事，不要把批量可见理解成Flink每30秒才计算一次。

## 4. 中断Flink计算进程

```powershell
.\scripts\run-lab.ps1 -Chapter 4 -RunId recovery1
```

脚本先支付10000分并等待统计，然后再发送20笔、每笔100分，
在发送过程中重启本项目TaskManager。JobManager、Kafka、Checkpoint卷保持运行。
预期最终12000分、21笔、version21。

脚本记录恢复前Checkpoint ID和路径，恢复后检查restored计数和新的成功快照。
Flink会恢复快照中的输入位置及状态，再重放后续数据。部分输入可能重新执行，
但读已提交事务的消费者不会把被中止事务当作有效结果。

每次重启时机不同，不能保证每次都命中同一个窗口；最终值和REST恢复记录才是验收依据。
这不是整个集群断电测试；整个JobManager重启后的重新提交方式见README。

## 5. PostgreSQL已提交，但Kafka消费进度尚未提交

```powershell
.\scripts\run-lab.ps1 -Chapter 5 -RunId failure1
```

请使用全新RunId。脚本为`lab-failure1-failure:1`布置一次性故障，然后发送700分支付。
阅读`ShopTotalListener`：它先调用代理事务服务，服务返回后数据库已经提交，
再在Listener返回之前抛教学异常。它没有强杀进程，但稳定复现了相同的重投条件。

Spring日志顺序：

```text
DATABASE_COMMITTED resultId=lab-failure1-failure:1 outcome=APPLIED
INJECT_AFTER_COMMIT resultId=lab-failure1-failure:1
DATABASE_COMMITTED resultId=lab-failure1-failure:1 outcome=DUPLICATE
```

```sql
SELECT event_id, version, processed_at FROM processed_event WHERE shop_id='lab-failure1-failure';
```

预期仅一条登记，统计700分/version1。数据库事务已提交、消费进度没提交时，
允许重投并在数据库去重，避免在两个系统之间假装存在一个本地原子事务。
脚本通过lab故障观察接口确认已重投且返回DUPLICATE，再独立查询Kafka消费组，
确认提交进度超过该记录offset，并用SQL核对仅一条去重登记。检查不满足就失败，不仅观察首次落库。
`GET /api/lab/faults/{resultId}`只保存本次服务进程里布置过的故障记录；重启Spring后重新选择RunId做本章。

## 6. 旧值第一次到达，也不能覆盖新值

```powershell
.\scripts\run-lab.ps1 -Chapter 6 -RunId stale1
```

通过lab端点给独立店铺先发送version3/23000分，再发送此前未出现的version2/18000分。
脚本等待消费者进度超过第二条结果，再确认统计仍然23000分/version3。
日志应分别为APPLIED和STALE，而不是DUPLICATE。

数据库会有两个不同事件身份，只有一个最新统计行。
这说明唯一约束只能解决重复身份，不能代替`WHERE EXCLUDED.version > current.version`。

## 实验边界

- 普通生产模式不启用lab端点；没有配置lab profile时返回404。
- 发现非法消息时消费者持续重试，不跳过分区进度；这个策略便于学习故障，生产需另行设计隔离和修复。
- 手工重放老事件时不能提前删除去重记录。
- 不修改Flink UID、不丢弃Checkpoint后直接复用旧结果库。
- 本项目的数据源是支付事件，不是PostgreSQL CDC；修改数据库不会自动回流到Flink。

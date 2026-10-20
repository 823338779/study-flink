# 消息契约与一致性边界

两个Topic都有2个分区，key必须是shopId。实验期间不改变分区数。

## payment-events

```json
{"eventId":"payment-001","shopId":"A","amountCents":10000,"occurredAt":"2026-10-20T10:00:00Z"}
```

ID为1–64位ASCII字母、数字、下划线或短横线。金额为正整数分，最大值受Java long限制。
occurredAt必须可解析为Instant，标准化为UTC；当前无窗口，不根据它排序。
Spring确认Kafka发送成功后返回topic、partition和offset。
Kafka不负责按业务eventId去重，重复HTTP请求可以产生多条输入记录。

Flink用(shopId,eventId)去重，相同身份且载荷一致则不再次累计；
相同店铺的同一eventId改变金额或时间会报错。不同店铺的相同eventId属于独立身份。
教学版去重状态不设TTL，只适合有限实验数据；无限生产流必须另行设计保留策略和容量。

## shop-payment-totals

```json
{"resultId":"A:1","shopId":"A","totalAmountCents":10000,"paymentCount":1,"version":1,"triggerEventId":"payment-001"}
```

这是累计值，不是“再加10000”。version等于唯一支付数，resultId为shopId:version。
恢复同一Checkpoint并重放同一店铺的同序输入，会生成相同的版本和身份。
版本不能跨空状态重建比较：重新从1开始会与旧结果库冲突。

Spring事务内：

1. 用(handler_name,resultId)唯一约束登记消费。
2. 重复身份直接跳过。
3. 新身份执行Upsert，仅当incoming.version大于数据库当前版本才覆盖。
4. 旧版本身份也可以登记为已处理，但金额保持新版本。
5. 事务服务返回后才允许Listener返回并提交消费进度。

| 机制 | 负责什么 | 不负责什么 |
| --- | --- | --- |
| Flink业务去重 | 同一个支付事件不重复累计 | Kafka源中不存在重复记录 |
| Flink Checkpoint | 状态与输入进度一致恢复 | 自动更新外部系统任意数据 |
| Kafka事务 + read_committed | 隔离中止事务，与快照协调输出提交 | Spring数据库的本地事务 |
| processed_event唯一约束 | 消费重投不重复产生数据库效果 | 不同身份消息的先后顺序 |
| 条件Upsert | 旧累计值不覆盖新版本 | 给不相关的状态历史建立可比版本 |

lab结果注入仅用于独立的lab-店铺，不要给真实Flink累计店铺伪造高版本。
数据库事务也不能回滚发邮件、HTTP调用等外部副作用，本项目没有这些操作。

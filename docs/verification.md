# 实际验证记录

执行日期：2026-10-20，Windows、PowerShell、本机JDK21、Docker Desktop Linux容器。
这是本机教学验证，不是生产可用性、容量或长时间断电保证。

## 构建与自动化测试

命令：`mvn -B verify`。最终BUILD SUCCESS，18项测试全部通过，0失败、0错误、0跳过。

| 测试范围 | 数量 | 覆盖 |
| --- | ---: | --- |
| Spring单元/MVC测试 | 4 | JSON契约、发送确认/失败/超时、默认profile关闭lab入口 |
| Kafka/PostgreSQL集成测试 | 8 | HTTP边界、原子回滚、并发去重、旧版本、提交后重投、错误key、坏消息 |
| Flink测试 | 6 | 分店聚合、冲突/溢出、快照恢复、JSON、Checkpoint配置 |

集成测试使用真实Kafka3.9.1与PostgreSQL17.10容器，不使用H2，也不跳过Docker不可用场景。
最终日志：`.superpowers/sdd/2026-10-20-payment-stream-learning/verify-final.log`。

额外验证了测试确实能发现错误：

- 小数金额原先被JSON解码器截断：HTTP期望400却得到200，Flink期望拒绝却未抛异常；关闭整数浮点强制转换后通过。
- 暂时把无限重试改成9次：坏消息只被调用10次，随后正常记录被处理，测试按预期失败；已恢复无限重试。
- 故障观察接口尚未实现时，提交后重投测试以404失败；实现后验证了重投结果及精确partition/offset。

PowerShell脚本全部通过语法解析；Compose配置有效；检查部署jar未包含Spring或Flink核心执行框架。

## 六个实际实验

| 章 | 实际RunId | 观察结果 |
| --- | --- | --- |
| 1 | reviewed02 | A=18000分/2笔/version2，B=5000分/1笔/version1 |
| 2 | reviewed02 | 重发payment-001后A仍为18000/2/2 |
| 3 | visible02 | 从Savepoint恢复为30秒间隔；支付1000分，查询延迟约27.47秒；再恢复到10秒 |
| 4 | recovery02 | 发送途中重启TaskManager，从Checkpoint35恢复；最终12000分/21笔/version21 |
| 5 | reviewed02 | 首次APPLIED，注入异常，再次DUPLICATE；只有1条登记，消费进度超过结果offset41（partition0） |
| 6 | reviewed02 | version3=23000先到、version2=18000后到；数据库保持23000/version3 |

执行命令：

```powershell
.\scripts\verify-lab.ps1 -RunId reviewed02
.\scripts\run-lab.ps1 -Chapter 3 -RunId visible02
.\scripts\run-lab.ps1 -Chapter 4 -RunId recovery02
```

这些RunId已存在。再次做第3–5章时使用新的RunId，第1–2章需共用RunId。
`verify-lab.ps1`覆盖第1、2、5、6章；没有把第3、4章伪称为同一脚本自动执行。

第3章时间证据：Flink于15:23:06.394 UTC计算，Spring于23:23:33.525 +08:00提交数据库。
二者时区不同，换算后约27.13秒；脚本查询计时包含HTTP请求和轮询，因此约27.47秒。

第4章恢复证据：作业`6864745cc02f795830df72057a77ee15`恢复路径为
`file:/opt/flink/checkpoints/6864745cc02f795830df72057a77ee15/chk-35`，
随后持续产生成功Checkpoint。恢复信息存于
`.superpowers/sdd/2026-10-20-payment-stream-learning/recovery-evidence.json`。

第5章是数据库提交后抛异常，真实触发Kafka重投；没有声称杀死了Spring进程。
脚本现在同时检查重投观察结果、broker消费进度、SQL登记行数和最终统计。
集成测试另行覆盖此行为，并检查日志。

## 启动与恢复保护

已实际执行：

- 重复运行基础设施启动：两个Topic保持2分区，不新建重复资源。
- 重复提交作业：返回现有JobID，不新增作业。
- 完成stop-with-savepoint，停止并重新启动本项目全部容器：Kafka、数据库、快照均保留。
- 此时不带RestorePath提交：明确拒绝对旧输入进行空状态重放。
- 指定刚完成的Savepoint恢复：作业RUNNING并产生成功Checkpoint，原有数据库结果可读。

计划停机生成的路径已记录在`.lab/last-savepoint.txt`。这是当时的快照；
后续再有新支付时，应生成新的停止Savepoint，不能把旧路径当作最新停止点。

## 实现调整与边界

- 最初计划PostgreSQL17.6。镜像下载遇到EOF后，改用本机已有的官方17.10 Alpine镜像并锁定SHA256；
  Compose与Testcontainers使用同一镜像摘要，没有跨主要版本。
- Flink镜像下载经官方镜像缓存完成，本机标签仍是`flink:1.20.3-scala_2.12-java17`。
- 首次启动发现Checkpoint卷属于root，Flink进程无法写入；增加一次性目录权限初始化后修复。
- Java21测试所需的模块开放参数只配置在测试进程；Flink实际运行于Java17容器。
- Spring数据库、HTTP和Kafka测试合并在一个集成测试类，共用隔离容器；保留独立断言。
- 顺序实现完成后做了一次独立只读审查，修复实验5误报成功风险、坏消息测试窗口不足、恢复超时边界文档缺失。
- 单Kafka副本、无Flink HA、无去重TTL，不适合直接承载无限生产数据。
- Kafka事务超时10分钟，不能把短故障验证扩大为任意时长停机保证；见README恢复说明。

## 当前服务与日志

本次交付保留学习服务运行，方便直接做第一章：

- Spring：`http://127.0.0.1:18080`，后台进程PID记录在`.lab/service.pid`。
- Flink：`http://127.0.0.1:18081`。
- Spring日志：`.lab/service.log`；本次重投实验日志：`.lab/service-reviewed.log`。
- 各次验证日志：`.superpowers/sdd/2026-10-20-payment-stream-learning/`。

要改用IDEA启动，先停止本次启动的Spring，避免18080端口冲突：

```powershell
$labProcessId = [int](Get-Content .lab/service.pid)
$labProcess = Get-CimInstance Win32_Process -Filter "ProcessId = $labProcessId"
if ($labProcess.CommandLine -like '*payment-service/target/payment-service-1.0.0.jar*') {
    Stop-Process -Id $labProcessId
}
```

Windows运行中的jar会被进程占用；再次`mvn verify`打包前也先停止此Spring进程。
此操作不关闭Kafka、PostgreSQL或Flink；数据库结果和Checkpoint保留。

# 在 Hive 4.2.1 中使用 Iceberg 表

这是一份供你**自己动手执行**的教程。以下 SQL 均为示例，不会由 Codex 代你运行。建议按顺序一次执行一段，观察结果后再继续；不要一次选中全文执行。

## 1. 适用环境和学习目标

本项目的 `compose.yaml` 使用 `apache/hive:4.2.1`，通过 HiveServer2 暴露 `localhost:10000`。元数据保存在容器内的 Derby，数据保存在 Docker 卷中的本地仓库。Hive 4.2.x 已内置 Iceberg 支持，无须为本教程额外下载 Iceberg JAR。

教程假设 `learn_hive.orders(id, city, amount)` 仍存在。你将创建**另一张演示表** `learn_hive.iceberg_orders_guide`；不会修改原表。如果你的 `orders` 内容已变化，示例中的行数也会相应变化。

完成后，你应当能回答三个问题：

1. Iceberg 表与 Parquet/ORC 文件分别负责什么？
2. 一次写入如何变成一个可查询的快照？
3. 怎样查看表当前引用的数据文件，以及怎样读取旧版本？

## 2. 先建立模型

`STORED BY ICEBERG` 选择的是**表格式**；Parquet、ORC、Avro 是**数据文件格式**。Iceberg 表通常使用 Parquet 文件，除非另行指定。

你可能在 Spark SQL 教程中看到 `CREATE TABLE ... USING iceberg`。那是 **Spark SQL** 的语法；本教程连接的是 **HiveServer2**，所以使用 Hive 的 `CREATE TABLE ... STORED BY ICEBERG`。两种写法都能创建 Iceberg 表，但不能把 Spark 的 `USING` 子句原样放进 Hive SQL。

```text
Hive Metastore 中的表记录
    ↓ 指向当前 Iceberg 元数据文件
表元数据（schema、分区规则、快照列表）
    ↓ 当前快照
manifest list → manifests（数据文件清单）
    ↓
Parquet/ORC/Avro 数据文件
```

普通 Hive 表主要依靠表和分区位置寻找文件；Iceberg 通过快照及清单确定某个版本**应当读取哪些文件**。因此，目录中存在某个文件，并不意味着它属于当前表版本。不要手动删除或替换 Iceberg 表目录中的文件。

> Iceberg 不等于 HDFS。当前 Docker 环境使用本地文件系统；将来可以改用共享的 HDFS 或对象存储。若希望 Spark 与 Hive 操作同一张表，两个引擎还必须使用兼容的 Catalog 和同一份存储。当前嵌入式 Derby 环境主要用于单机学习。

## 3. 连接方式

你可以在自己的 IDEA Hive 数据源中逐段执行 SQL。也可以在项目目录的 PowerShell 中手动打开 Beeline：

```powershell
docker compose up -d hiveserver2
docker exec -it flink-hiveserver2-1 beeline -u 'jdbc:hive2://localhost:10000/learn_hive'
```

进入 Beeline 后再输入下面的 SQL。若容器尚未完全启动，稍等片刻再连接。

## 4. 创建第一张 Iceberg 表

先确认演示表尚不存在：

```sql
SHOW TABLES IN learn_hive LIKE 'iceberg_orders_guide';
```

若返回了同名表，先检查它的内容，不要直接覆盖。没有同名表时执行：

```sql
CREATE TABLE learn_hive.iceberg_orders_guide (
    id INT,
    city STRING,
    amount INT
)
STORED BY ICEBERG;
```

这里没有写 `STORED AS`，所以 Iceberg 默认以 **Parquet** 写数据文件。`STORED BY ICEBERG` 才是选择表格式的关键；不写 `EXTERNAL` 也可以创建 Iceberg 表，Apache Iceberg 的 Hive 快速入门就是这样写的。

**注意：省略 `EXTERNAL` 不保证最终是托管表。** 如果 Metastore 配置了 `MetaStoreMetadataTransformer`，`CREATE TABLE ... STORED BY ICEBERG` 会被转换为外部表。请查看实际建表结果，不要根据 SQL 字面推断 `DROP TABLE` 是否会删除文件。如果当前 Metastore 不接受这种写法，再使用 `CREATE EXTERNAL TABLE ... STORED BY ICEBERG`，或先配置该 transformer。

查看建表结果：

```sql
DESCRIBE FORMATTED learn_hive.iceberg_orders_guide;
```

重点找 `Table Type`、`storage_handler`、`table_type`、`metadata_location` 和 `external.table.purge`（若存在）。`metadata_location` 是当前 Iceberg 表元数据文件的位置，而不是某个 Parquet 数据文件的位置。

如果明确想使用 ORC 数据文件，可以在**另一张新表**的建表语句中写 `STORED BY ICEBERG STORED AS ORC`；这仍是一张 Iceberg 表。

## 5. 写入、观察快照、读取旧版本

### 第一次写入

```sql
INSERT INTO learn_hive.iceberg_orders_guide
SELECT id, city, amount
FROM learn_hive.orders;
```

检查当前数据和第一个快照：

```sql
SELECT * FROM learn_hive.iceberg_orders_guide ORDER BY id;

SELECT snapshot_id, operation
FROM learn_hive.iceberg_orders_guide.snapshots;
```

如果提示“找不到 `snapshots` 表”，先不要删表或重建。手动运行 `DESCRIBE FORMATTED learn_hive.iceberg_orders_guide;`，检查 `storage_handler` 是否为 `org.apache.iceberg.mr.hive.HiveIcebergStorageHandler`，以及是否有 `metadata_location`。如果这两项存在，再到 Beeline 中运行同一条快照查询，以区分表本身的问题和 SQL 客户端的问题。Iceberg 元数据表必须使用完整的 `数据库.表.元数据表` 名称。

**记下你实际看到的第一个 `snapshot_id`。** Iceberg 会为每次成功写入生成新的快照；快照 ID 由系统生成，不能照抄别人机器上的数字。查询 Iceberg 元数据表时请写完整的 `数据库.表.元数据表`，例如上面的 `.snapshots`。

### 第二次写入

```sql
INSERT INTO learn_hive.iceberg_orders_guide
VALUES (4, 'Shenzhen', 120);
```

再查看快照和当前行数：

```sql
SELECT snapshot_id, operation
FROM learn_hive.iceberg_orders_guide.snapshots;

SELECT COUNT(*) AS current_rows
FROM learn_hive.iceberg_orders_guide;
```

如果原 `orders` 恰好是之前的三条示例数据，当前会有 4 行、两个 `append` 快照。

### 时间旅行

将下面的 `123456789` **替换为你在第一次写入后记下的真实快照 ID**，不要保留尖括号或引号：

```sql
SELECT COUNT(*) AS old_rows
FROM learn_hive.iceberg_orders_guide
FOR SYSTEM_VERSION AS OF 123456789;
```

如果原表是三条示例数据，旧快照的结果应为 3，而当前快照是 4。时间旅行是**读取旧版本**，不会改变当前版本。快照一旦过期清理，相关旧版本可能无法再读取。

## 6. 看 Iceberg 真正管理了什么

```sql
SELECT file_path, record_count
FROM learn_hive.iceberg_orders_guide.files;

SELECT *
FROM learn_hive.iceberg_orders_guide.history;
```

`.files` 表示**当前快照引用**的数据文件，不等于目录里的全部历史文件。前面两次写入可能产生多个 Parquet 文件；文件数量会受执行引擎和写入配置影响，不应假设“一次写入必定只有一个文件”。`.history` 用于观察快照成为当前版本的历史。

继续探索时，可以查询 `.manifests`、`.partitions` 等元数据表；先 `SELECT * ... LIMIT 5` 看列，再选择需要的字段。

## 7. 隐藏分区：与普通 Hive 分区的关键区别

普通 Hive 分区表常让你显式维护 `dt` 一类分区列。Iceberg 可以根据原始列计算分区值。例如，下面是**独立的建表示意**，不要在已有演示表上重复执行：

```sql
CREATE TABLE learn_hive.iceberg_events_guide (
    event_id BIGINT,
    event_time TIMESTAMP,
    city STRING
)
PARTITIONED BY SPEC (month(event_time), bucket(4, event_id))
STORED BY ICEBERG;
```

你写入的是 `event_time` 和 `event_id`；Iceberg 根据分区规则计算月份与桶值。查询仍按业务列过滤：

```sql
SELECT *
FROM learn_hive.iceberg_events_guide
WHERE event_time >= CAST('2026-10-01 00:00:00' AS TIMESTAMP)
  AND event_time <  CAST('2026-11-01 00:00:00' AS TIMESTAMP);
```

Iceberg 可利用分区规则推导可跳过的数据，但实际跳过范围仍取决于数据分布和执行计划。演示数据很少，不能用耗时比较性能。

## 8. 表结构演进和行级变更

以下命令只针对你自己的演示表；按需执行，不要一次性全跑。

给现有表加列：

```sql
ALTER TABLE learn_hive.iceberg_orders_guide
ADD COLUMNS (remark STRING);
```

已有行在新列上会表现为 `NULL`。Hive 4 还支持对 Iceberg 表执行 `UPDATE`、`DELETE` 和 `MERGE INTO`，例如：

```sql
UPDATE learn_hive.iceberg_orders_guide
SET amount = 130
WHERE id = 4;
```

行级变更可能重写数据文件并产生新快照；不要把它当成 MySQL 式高频单行事务。Hive 的 Iceberg 写入操作要求 Tez 执行引擎。执行后可再次查询 `.snapshots` 和当前数据，观察变化。

## 9. 维护与边界

- **小文件**：频繁小批量写入会增加文件数。用 `.files` 检查数量与大小，在真实大表中规划文件合并；Hive 4 支持 `OPTIMIZE TABLE ... REWRITE DATA`。
- **快照保留**：旧快照让时间旅行成为可能，也会占用元数据和可能的数据文件空间。确定保留期限后再执行过期清理；清理后旧版本可能不能查询。
- **不要直接改文件**：不要在 Iceberg 表目录中手工删 Parquet、Avro 或元数据文件。使用表级 SQL/维护操作。
- **跨引擎共享**：生产中让 Hive、Spark 共用 Iceberg 表时，先设计共享存储和 Catalog；本项目的嵌入式 Derby 是学习用配置。
- **不要误解性能**：Iceberg 带来快照、事务提交、分区演进等表级能力；并不保证每条查询都比普通 Hive + ORC/Parquet 快。

## 10. 本节自检

你可以用下面三项检查自己是否真正理解：

1. 为什么 `STORED BY ICEBERG` 与 `STORED AS ORC` 可以同时出现？
2. 为什么 `.files` 的结果比直接列出表目录更能说明当前版本在读什么？
3. 为什么第二次写入后，旧快照仍能查到第一次写入时的数据？

## 参考资料

- [Apache Iceberg：Hive 集成、DDL、元数据表、时间旅行](https://iceberg.apache.org/docs/latest/hive/)
- [Apache Iceberg：表配置与默认文件格式](https://iceberg.apache.org/docs/latest/configuration/)
- [Apache Iceberg：隐藏分区](https://iceberg.apache.org/docs/latest/partitioning/)
- [Apache Iceberg：表格式规范](https://iceberg.apache.org/spec/)

# 基于 Spark 的分布式查询优化实验

## 小组成员及分工

- 邓鹏：实验设计、环境搭建、执行实验 25%
- 夏倍蓓：实验设计、执行实验、汇报 25%
- 吕晔：实验设计、执行实验、数据处理 25%
- 赵伟珺：实验设计、执行实验、PPT制作 25%

## 一、实验目标

研究Spark和 Spark SQL 查询优化机制，探索 TPC-H 中复杂 SOL 查询优化方案。

## 二、实验设计

### 数据集

采用数据库系统性能的标准基准测试 TPC-H。

### 实验步骤

- 基于 Spark RDD 实现TPC-H中复杂SQL（以Q3为例）。
- 与单机环境和 Spark SQL 进行性能对比。
- 优化Spark RDD任务的执行效率，分别从任务级别——Reduce、aggregateByKey、foldByKey、combineByKey算子；任务调参——并行度、缓存；运行参数——driver 和worker 参数配置，三个角度来展开。

## 实验步骤

### 1.配置实验环境

采用 docker 配置分布式环境
操作系统：Ubuntu 22.04.1 LTS
Hadoop：hadoop 3.3.6
Spark：spark 3.4.4
Java：1.8.0_431
内存：

### 2.启动服务

（1）在 hadoop 目录下， 用 `./sbin/start-all.sh` 启动Hadoop服务和Yarn服务；
（2）使用 `hdfs dfs -put /usr/local/tpch-dbgen/*.tbl /tpch/` 将数据上传到 hdfs 中
（3）切换到 spark 目录下，`./sbin/start-all.sh` 启动 spark 服务；
（4）在每个节点下使用 jps 查看节点的进程状态。

![master_jps](imgs\master_jps.png)

![slave01_jps](imgs\slave01_jps.png)

![slave02_jps](imgs\slave02_jps.png)

这里将 master 设置为主节点和 master 节点，将 slave01 和 slave02 为 Worker 节点。

### 3.运行程序

在 spark 路径下执行以下命令：

```bash
spark-submit \
--master yarn \
--deploy-mode client \
--num-executors 2 \
--executor-cores 1 \
--executor-memory 512M \
--conf spark.eventLog.enabled=true \
--conf spark.eventLog.dir=hdfs://master:9000/spark-history \
--conf spark.history.fs.logDirectory=hdfs://master:9000/spark-history \
--conf spark.yarn.historyServer.address=master:18080 \
tpch_query_q3_rdd.py
```

### 实验结果
（1）基于 SparkRDD 实现TPC-H中复杂SQL
（2）SQLite、SparkRDD和SparkSQL的性能对比
（3）SparkRDD任务优化

- 调整算子
<img width="524" alt="{CC04EEC8-A8FC-42D0-8DBA-CFB15AB76865}" src="https://github.com/user-attachments/assets/dc264d82-b027-4b81-9151-9880bdf51581" />


## 实验反思

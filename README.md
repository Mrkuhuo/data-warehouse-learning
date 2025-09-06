## 开源不易，请各位朋友点个 ***★star★*** 支持一下，非常感谢~

[【 Github地址：https://github.com/Mrkuhuo/data-warehouse-learning 】](https://github.com/Mrkuhuo/data-warehouse-learning)

[【 Gitee 地址：https://gitee.com/wzylzjtn/data-warehouse-learning 】](https://gitee.com/wzylzjtn/data-warehouse-learning)

[【 推荐开发平台：https://github.com/642933588/jiron-cloud 】](https://github.com/642933588/jiron-cloud)
---

## 1 介绍

"《实时/离线数仓实战》是一个以电商系统为基础，围绕电商业务指标统计需求而构建的数仓项目。该项目涵盖了基于Doris、Piamon、Hudi和Iceberg的离线数仓和实时数仓（数据湖）的构建。两种场景在数据处理逻辑上保持一致，但采用了不同的技术实现，为数仓建设提供了多样化的思路。
## 2 技术架构

![技术架构](src/main/java/org/bigdatatechcir/images/jiagou1.png)

电商数仓项目（实时/离线）的技术架构由四个关键部分组成：

1. 数据源模块：本模块通过 **JAVA** 编写的代码来生成电商业务数据，并将这些数据写入 **MySQL** 数据库。同时，生成的用户日志数据被写入 **Kafka** 消息队列。模块支持在配置文件中设定数据生成的日期，以满足不同时间点的数据需求。

2. 数据采集模块：利用 **Dinky** 开发的 **FlinkSQL** 代码，消费 **Kafka** 中的用户日志数据，并将其写入 **Doris** 、**Paimon** 、 **Hudi** 和 **Iceberg** 的在线数据存储（ODS）层。此外，使用 **DolphinScheduler** 配置 **SeaTunnel** 任务，以同步 **MySQL** 中的业务数据到 **Doris** 的ODS层。**FlinkSQL/CDC** 技术则用于从 **Kafka** 和 **MySQL** 采集数据，并将它们分别写入 **Paimon** 、**Hudi** 和 **Iceberg** 的ODS层。

3. 数仓模块：遵循行业标准的ODS（操作数据存储）-> DWD（数据仓库明细层）/ DIM（维度数据层）-> DWS（数据服务层）-> ADS（应用数据存储）的四级数据分层架构。数据在**Doris** 、**Paimon**、**Hudi** 和 **Iceberg** 中通过批量和实时两种调度方式进行有效流转。

4. 数据可视化：ADS层和DWS层的数据可以利用 **SuperSet** 和 **DataRT** 工具进行报表和数据大屏的制作与展示，以直观地呈现数据洞察。

## 3 软件版本

| 软件               | 版本     | 安装包        | 对应依赖包                                                                                                                                      |
|------------------|--------|------------|--------------------------------------------------------------------------------------------------------------------------------------------|
| Zookeeper        | 3.9.1  | apache-zookeeper-3.9.1-bin.tar.gz       |                                                                                                                                            |
| Kafka            | 3.6.1  | kafka_2.12-3.6.1.tgz       |                                                                                                                                            |
| Seatunnel        | 2.3.3  | apache-seatunnel-2.3.3-bin.tar.gz       | seatunnel-hadoop3-3.1.4-uber-2.3.3-optional.jar                                                                                            |
| Dolphinscheduler | 3.2.0  | apache-dolphinscheduler-3.2.0-bin.tar.gz       | mysql-connector-java-8.0.16.jar                                                                                                            |
| Doris            | 2.0.4  | apache-doris-2.0.4-bin-x64.tar.gz       |                                                                                                                                            |
| Flink            | 1.18.1 | flink-1.18.1-bin-scala_2.12.tgz       | flink-sql-connector-mysql-cdc-2.4.2.jar <br> flink-sql-connector-kafka-3.1.0-1.18.jar <br> flink-sql-connector-hive-3.1.3_2.12-1.19.0.jar <br> flink-connector-jdbc-3.2.0-1.19.jar |
| Iceberg          | 1.5.2  | iceberg-flink-runtime-1.18-1.5.2.jar       |                                                                                                                                            |
| Hudi             | 0.15.0 | hudi-flink1.18-bundle-0.15.0.jar       |                                                                                                                                            |
| Paimon           | 0.8    | paimon-flink-1.18-0.8-20240301.002155-30.jar       | flink-shaded-hadoop-2-uber-2.7.5-9.0.jar                                                                                                   |
| Dinky            | 1.0.0  | dinky-release-1.18-1.0.0-rc4.tar.gz       |                                                                                                                                            |
| Hadoop           | 3.1.3  | hadoop-3.1.3.tar.gz       |                                                                                                                                            |
| Hive             | 3.1.3  | apache-hive-3.1.3-bin.tar.gz       | paimon-hive-connector-3.1-0.7.0-incubating.jar  <br> iceberg-hive-runtime-1.5.2.jar  <br> hudi-hadoop-mr-bundle-0.15.0.jar                 |
| Maven            | 3.9.6  | apache-maven-3.9.6-bin.tar.gz       |                                                                                                                                            |
| Centos           | 8      | CentOS-8.5.2111-x86_64-dvd1.iso       |                                                                                                                                            |
| OpenJDK          | 8      | openlogic-openjdk-8u402-b06-linux-x64.tar.gz       |                                                                                                                                            |

# 通用部分

#### 1) 组件安装

![安装文档](src/main/java/org/bigdatatechcir/images/anzhuang.jpg)

#### 2) 模拟数据生成

![模拟数据生成](src/main/java/org/bigdatatechcir/images/monishuju.png)

生成业务库数据如下图所示：

![业务库数据](src/main/java/org/bigdatatechcir/images/yewushuju.png)

生成用户日志数据如下图所示：

![用户日志数据](src/main/java/org/bigdatatechcir/images/yonghurizhishuju.png)

# 离线数仓建设部分(Doris)

涉及组件：**Kafka** + **Flink** + **Doris** + **Seatunnel** + **Dolphinscheduler**

#### 1) 数据采集

**Kafka** 数据通过 **Flink** 接入 **Doris**

![Kafka 数据接入](src/main/java/org/bigdatatechcir/images/flink.png)

**MySQL** 数据通过 **SeaTunnel** 接入 **Doris**

![MySQL 数据接入](src/main/java/org/bigdatatechcir/images/seatunnel.png)

#### 2) **Doris ODS** 层建设

数据采集进 **Doris ODS** 层，实现效果如下图所示：

![Doris ODS层建设](src/main/java/org/bigdatatechcir/images/ods.png)

#### 3) **Doris DIM** 层建设

开发 **DorisSQL** 进行 **DIM** 层数据处理

![Doris DIM层处理逻辑](src/main/java/org/bigdatatechcir/images/dim.png)

**DIM** 层数据实现效果如下图：

![Doris DIM层数据库](src/main/java/org/bigdatatechcir/images/dwddatabase.png)

#### 4) **Doris DWD** 层建设

开发 **DorisSQL** 进行 **DWD** 层数据处理

![Doris DWD层处理逻辑](src/main/java/org/bigdatatechcir/images/dwd.png)

**DWD** 层数据实现效果如下图：

![Doris DWD层数据库](src/main/java/org/bigdatatechcir/images/dimdatabase.png)

#### 5) **Doris DWS** 层建设

开发 **DorisSQL** 进行 **DWS** 层数据处理

![Doris DWS层处理逻辑](src/main/java/org/bigdatatechcir/images/dws.png)

**DWS** 层数据实现效果如下图：

![Doris DWS层数据库](src/main/java/org/bigdatatechcir/images/dwsdatabase.png)

#### 6) **Doris ADS** 层建设

开发 **DorisSQL** 进行 **ADS** 层数据处理

![Doris ADS层处理逻辑](src/main/java/org/bigdatatechcir/images/ads.png)

**ADS** 层数据实现效果如下图：

![Doris ADS层数据库](src/main/java/org/bigdatatechcir/images/adsdatabase.png)

#### 7) 任务编排

最终的任务概览如下图所示

![Doris 任务概览](src/main/java/org/bigdatatechcir/images/allrenwu.png)

任务编排效果如下图所示

![Doris 任务概览](src/main/java/org/bigdatatechcir/images/bianpai.png)

#### 8) 数据展示

![大屏](src/main/java/org/bigdatatechcir/images/daping1.png)

# 实时数仓（数据湖）建设部分（Paimon/Hudi/Iceberg）

涉及组件：**Kafka** + **Flink(CDC/SQL/UDF)** + **Paimon/Hudi/Iceberg** + **Hive** + **Dinky**

#### 1) **Paimon ODS** 层建设

**Kafka** 数据通过 **FlinkSQL** 接入 **Paimon/Hudi/Iceberg** ,实际数据落到 **Hive**

**MySQL** 数据通过 **FlinkCDC** 接入 **Paimon/Hudi/Iceberg** ,实际数据落到 **Hive**

![Kafka/MySQL 数据接入](src/main/java/org/bigdatatechcir/images/paimon%20ods.png)

数据采集进 **Paimon ODS** 层，实现效果如下图所示：

![Paimon ODS层建设](src/main/java/org/bigdatatechcir/images/hiveods.png)

#### 2) **Paimon DWD** 层建设

开发 **FlinkSQL** 进行 **DWD** 层数据处理

![Paimon DWD层建设](src/main/java/org/bigdatatechcir/images/paimondwd.png)

**DWD** 层数据实现效果如下图：

![Paimon DWD层建设](src/main/java/org/bigdatatechcir/images/hivedwd.png)

#### 3) **Paimon DIM** 层建设

开发 **FlinkSQL** 进行 **DIM** 层数据处理

![Paimon DIM层建设](src/main/java/org/bigdatatechcir/images/paimondim.png)

**DIM** 层数据实现效果如下图：

![Paimon DIM层建设](src/main/java/org/bigdatatechcir/images/hivedim.png)

#### 4) **Paimon DWS** 层建设

开发 **FlinkSQL** 进行 **DWS** 层数据处理

![Paimon DWS层建设](src/main/java/org/bigdatatechcir/images/paimondws.png)

**DWS** 层数据实现效果如下图：

![Paimon DWS层建设](src/main/java/org/bigdatatechcir/images/hivedws.png)

#### 5) **Paimon ADS** 层建设

开发 **FlinkSQL** 进行 **ADS** 层数据处理

![Paimon ADS层建设](src/main/java/org/bigdatatechcir/images/paimonads.png)

**ADS** 层数据实现效果如下图：

![Paimon ADS层建设](src/main/java/org/bigdatatechcir/images/hiveads.png)

#### 6) **Doris Catalog** 连接 **Paimon** + **DataRT** 进行数据展示

![大屏](src/main/java/org/bigdatatechcir/images/daping2.png)

[【 Flink面试题答案 】](https://t.zsxq.com/DKAQ3)

[【 FlinkSQL面试精讲 】](https://t.zsxq.com/GyF7n)

[【 Flink源码分析 】](https://t.zsxq.com/4e96L)

[【 Flink数据倾斜优化 】](https://t.zsxq.com/uMt82)

[【 Paimon面试题 】](https://t.zsxq.com/tfjLv)

[【 Doris面试精华 】](https://t.zsxq.com/PkNd1)

[【 StarRocks面试题 】](https://t.zsxq.com/iQK21)

[【 Hive面试题 】](https://t.zsxq.com/tdt8e)

[【 Spark面试题 】](https://t.zsxq.com/WwB9a)

[【 Paimon Join专项面试题 】](https://t.zsxq.com/femkb)

[【 Iceberg面试题 】](https://t.zsxq.com/r9mCw)

[【 Hudi面试题 】](https://t.zsxq.com/FIbGf)

[【 数据仓库面试题 】](https://t.zsxq.com/DLg8y)

[【 Clickhouse 面试题 】](https://t.zsxq.com/LegAQ)

[【 ElasticSearch面试题 】](https://t.zsxq.com/FmCvD)

[【 kafka面试题 】](https://t.zsxq.com/6KhyV)

[【 Hbase 面试题 】](https://t.zsxq.com/KCHy1)

[【 Zookeeper 面试题 】](https://t.zsxq.com/aXyFm)

[【 数仓最新文档 】](https://t.zsxq.com/7zQQH)

[【 Flink学习文档v1.0 】](https://t.zsxq.com/NWSIR)

[【 Doris学习手册 】](https://t.zsxq.com/FDDIz)

[【 Dinky最新指南 】](https://t.zsxq.com/04NdO)

[【 Flink 学习文档V2.0 】](https://t.zsxq.com/d4cDB)

[【 Paimon 学习文档V2.0 】](https://t.zsxq.com/5N2Pc)

[【 Fluss 学习文档V1.0 】](https://t.zsxq.com/wH86T)

[【 Flink2.0 ForSt状态后端 学习文档V1.0 】](https://t.zsxq.com/BIObu)

[【 数据治理指南 V1.0 】](https://t.zsxq.com/JOGjG)

[【 Iceberg学习文档 V1.0 】](https://t.zsxq.com/oq0Ad)

[【 Flink并行度优化指南 V1.0 】](https://t.zsxq.com/pgNNS)

[【 Iceberg学习文档 V2.0 】](https://t.zsxq.com/qC8iJ)

[【 Flink反压问题解决方案 V1.0 】](https://t.zsxq.com/vXQ8Q)

[【 Hudi学习文档 V1.0 】](https://t.zsxq.com/gb2Pl)

[【 数据湖选型 V1.0 】](https://t.zsxq.com/BB3LH)

[【 数仓建设方案 V1.0 】](https://t.zsxq.com/CzZiw)

[【 Spark Shuffle阶段详解 V1.0 】](https://t.zsxq.com/ZoB3a)

[【 大数据计算引擎发展历史】](https://t.zsxq.com/HlliS)

[【 Flink可以优化的方方面面 V1.0 】](https://t.zsxq.com/yaCFq)

[【 Paimon可以优化的方方面面 V1.0 】](https://t.zsxq.com/Vp9yu)

[【 Flink内存管理 V1.0 】](https://t.zsxq.com/sRhyo)

[【 Doris可以优化的方方面面 V1.0 】](https://t.zsxq.com/BcKVy)

[【 StarRocks可以优化的方方面面 V1.0 】](https://t.zsxq.com/YXMXf)

[【 Flink反压排查思路 V1.0 】](https://t.zsxq.com/APrl6)

[【 Paimon QPS太低调优思路 V1.0 】](https://t.zsxq.com/IeKVr)

[【 Flink100G级状态如何调优思路 V1.0 】](https://t.zsxq.com/tTE1X)

[【 Iceberg可以优化的方方面面 V1.0 】](https://t.zsxq.com/36gjm)

[【 Spark Checkpoint 和 Flink Checkpoint有什么区别  V1.0 】](https://t.zsxq.com/IbKvN)

[【 Spark可以优化的方方面面 V1.0 】](https://t.zsxq.com/rxIsz)

[【 Spark RDD优化方案 V1.0 】](https://t.zsxq.com/MgSfu)

[【 Kafka可以优化的方方面面 V1.0 】](https://t.zsxq.com/g2ndS)

[【 Paimon自动分区和快照清理原理 V1.0 】](https://t.zsxq.com/LAYJs)

[【 Paimon学习文档十一章 V1.0 】](https://t.zsxq.com/os0Ee)

[【 Iceberg学习文档十一章 V1.0 】](https://t.zsxq.com/0j01g)

[【 Spark学习文档十一章  V1.0 】](https://t.zsxq.com/PpsmT)

[【 Flink2.0学习文档十二章 V1.0 】](https://t.zsxq.com/3Wl1Z)

[【 基于Flink + Paimon的全流程数仓建设实践指南 V1.0 】](https://t.zsxq.com/RCYLl)

[【 Flink1.19 + Paimon1.2构建实时数仓教程文档 V1.0 】](https://t.zsxq.com/nJazP)

[【 K8s学习文档 V1.0 】](https://t.zsxq.com/MgFbT)

[【 Doris 学习文档 V1.0 】](https://t.zsxq.com/kuEDJ)

[【 Apache Doris全方位优化指南 】](https://t.zsxq.com/NfAhA)

[【 Doris+Paimon构建湖仓一体方案 】](https://t.zsxq.com/scQuV)

[【 Flink SQL 四种 Join 方式详解：原理、场景与实战 】](https://t.zsxq.com/6ECBG)

[【 Flink SQL + Paimon 数据湖建设全流程详解 】](https://t.zsxq.com/RCYLl)

[【 使用Kubernetes提交Flink任务详解 】](https://t.zsxq.com/sScKG)

[【 使用YARN提交Flink任务详解 】](https://t.zsxq.com/EEcUP)

[【 结合Flink + GitLab + K8s实现代码变更追踪与自动化部署 】](https://t.zsxq.com/Lzqfp)

[【 Kubernetes详细学习路线图 】](https://t.zsxq.com/H18YP)

[【 基于Kubernetes的湖仓一体高可用架构部署指南 】](https://t.zsxq.com/7JPFp)

[【 Flink 运维全指南：监控、问题排查、优化与实战 】](https://t.zsxq.com/naKp1)

[【 数仓建设规范 】](https://t.zsxq.com/1lZec)

[【 如何系统调研一个大数据组件 】](https://t.zsxq.com/olpRH)

[【 Flink精确一次提交原理深度解析：从机制到源码实现 】](https://t.zsxq.com/r24gG)

[【 Flink 逐条处理模式详解：毫秒级延迟的实现原理与源码剖析 】](https://t.zsxq.com/Zro2i)

[【 Flink 流水线执行（Pipelined Execution）原理详解 】](https://t.zsxq.com/FgIlS)

[【 Flink异步Checkpoint机制详解：原理、流程与源码剖析 】](https://t.zsxq.com/wop5x)

[【 Flink三种状态后端（State Backend）实现原理详解 】](https://t.zsxq.com/UPvkn)

[【 Flink 三种时间语义（Time Semantics）详解：原理、源码与实践 】](https://t.zsxq.com/mhKiv)

[【 Flink Watermark 机制深度解析：原理、源码与实践 】](https://t.zsxq.com/ogHw7)

[【 Flink窗口机制（Windowing）详解：从原理到源码深度解析 】](https://t.zsxq.com/LVjsa)

[【 Flink分布式快照（Checkpoint）机制深度解析 】](https://t.zsxq.com/djU3s)

[【 Flink动态资源调整机制详解：原理、源码与实践 】](https://t.zsxq.com/zwoGd)

[【 Flink 内存模型详解：原理、源码与实践 】](https://t.zsxq.com/pSHT8)

[【 Spark RDD (Resilient Distributed Dataset) 机制详解 】](https://t.zsxq.com/6MKcb)

[【 Spark 调度模型深度解析：Stage 划分、任务调度与本地性优化 】](https://t.zsxq.com/Jisvo)

[【 Spark Tungsten执行引擎深度解析：突破JVM性能瓶颈的内存与计算优化 】](https://t.zsxq.com/USZgr)

[【 Spark内存管理模型详解：统一内存管理与堆外内存机制 】](https://t.zsxq.com/TpBgH)

[【 Spark 缓存机制详解：Storage Level 与序列化存储原理及源码分析 】](https://t.zsxq.com/dv6zP)

[【 Spark Shuffle 核心技术深度解析：从原理到源码 】](https://t.zsxq.com/oyWDt)

[【 Spark Catalyst 优化器详解：从逻辑计划到物理执行的深度优化 】](https://t.zsxq.com/Txob2)

[【 Spark DataFrame/Dataset API详解：从RDD到向量化查询执行 】](https://t.zsxq.com/aeBPQ)

[【 Spark数据源API (DataSource V2)详解：统一读写接口与优化技术 】](https://t.zsxq.com/EfaOT)

[【 Spark微批处理模型（Micro-Batch）深度解析：原理、容错与源码实现 】](https://t.zsxq.com/Ww8BS)

[【 Spark状态管理详解：State Store与Watermark机制 】](https://t.zsxq.com/gYntB)

[【 Spark Pipeline API：基于Transformer和Estimator构建机器学习流水线 】](https://t.zsxq.com/zZoXX)

[【 Spark分布式机器学习算法实现与模型持久化详解 】](https://t.zsxq.com/bVqVq)

[【 Spark Pregel模型详解：基于BSP的迭代图计算框架 】](https://t.zsxq.com/RxXU4)

[【 Spark图分区策略详解：Edge Partition与Vertex Partition优化及源码剖析 】](https://t.zsxq.com/HYPy9)

[【 Spark数据倾斜优化详解：Salting技术与自定义分区器 】](https://t.zsxq.com/JX7gA)

[【 Spark Join 优化策略深度解析：Broadcast Join 与 Bucket Pruning 】](https://t.zsxq.com/mnGaD)

[【 Spark AQE (Adaptive Query Execution) 深度解析：动态优化查询执行 】](https://t.zsxq.com/C0AXT)

[【 Iceberg 基于文件的元数据管理：三层架构与源码深度解析 】](https://t.zsxq.com/dABYe)

[【 Iceberg 原子性操作与事务隔离深度解析 】](https://t.zsxq.com/9l0YM)

[【 Iceberg 时间旅行（Time Travel）深度解析：原理、实现与应用 】](https://t.zsxq.com/pijTh)

[【 Iceberg统计信息驱动优化：从列级统计到文件级过滤的深度解析 】](https://t.zsxq.com/EtJ6z)

[【 Iceberg数据布局优化深度解析：排序与Z-Order的多维性能提升 】](https://t.zsxq.com/KTSjC)

[【 Iceberg 分区演进（Partition Evolution）详解：无需重写数据的动态分区策略调整 】](https://t.zsxq.com/pFyST)

[【 Iceberg 快照过期与清理机制详解 】](https://t.zsxq.com/jiaxg)

[【 Iceberg 孤儿文件清理（Orphan File Cleanup）深度解析 】](https://t.zsxq.com/7fDkf)

[【 Iceberg数据校验与修复：深度解析rewrite_data_files与rewrite_manifests 】](https://t.zsxq.com/Vql3N)

[【 Iceberg 存储无关性深度解析：架构、实现与跨存储系统适配 】](https://t.zsxq.com/mWa47)

[【 Iceberg多引擎兼容性详解：统一数据访问的新范式 】](https://t.zsxq.com/Ya70i)

[【 Iceberg 流批一体支持详解：流式写入与增量读取的原理与实践 】](https://t.zsxq.com/803JX)

[【 Iceberg 物化视图详解：自动刷新机制与实现 】](https://t.zsxq.com/e976O)

[【 Iceberg 分支与标签详解：实现数据版本管理的强大功能 】](https://t.zsxq.com/MG79f)

[【 Iceberg行级删除与更新（Row-Level Deletes & Updates）深度解析 】](https://t.zsxq.com/UJdn9)

[【 Paimon LSM-Tree存储结构详解：高效写入与更新的核心引擎 】](https://t.zsxq.com/NCNZV)

[【 Paimon列式存储格式深度解析：基于Parquet/ORC的优化、压缩与谓词下推 】](https://t.zsxq.com/HoUG9)

[【 Spark 缓存机制详解：Storage Level 与序列化存储原理及源码分析 】](https://t.zsxq.com/dKHNq)

[【 Paimon流批一体架构深度解析：统一实时与批处理的数据湖技术 】](https://t.zsxq.com/QKXBT)

[【 Paimon实时数据更新技术深度解析：从主键行级更新到CDC场景实践 】](https://t.zsxq.com/c4VNo)

[【 Paimon 轻量级元数据（Lightweight Metadata）详解 】](https://t.zsxq.com/tWB0f)

[【 Paimon 分区与桶（Partitioning & Bucketing）详解：原理、源码与实践 】](https://t.zsxq.com/WMsyK)

[【 Paimon 小文件自动合并（Auto Compaction）技术详解 】](https://t.zsxq.com/A5pCS)

[【 Paimon事务支持详解：基于快照隔离的读写事务与数据一致性 】](https://t.zsxq.com/Y6Pb7)

[【 Doris 列式存储深度解析：原理、实现与性能优化 】](https://t.zsxq.com/TFX2z)

[【 Doris 前缀索引（Prefix Index）深度解析：原理、实现与性能优化 】](https://t.zsxq.com/mFe86)

[【 Doris 数据分桶与分片 (Bucketing & Sharding) 深度解析 】](https://t.zsxq.com/cvAvC)

[【 Doris数据模型与更新机制深度解析 】](https://t.zsxq.com/sR549)

[【 Doris 冷热数据分层（Tiered Storage）技术详解 】](https://t.zsxq.com/XCTH0)

[【 Doris MPP架构详解：从查询拆分到分布式执行与线性扩展 】](https://t.zsxq.com/9hg44)

[【 Doris向量化执行引擎（Vectorized Execution）深度解析 】](https://t.zsxq.com/5AtdI)

[【 Doris查询优化器深度解析 】](https://t.zsxq.com/VZXrJ)

[【 Doris 物化视图 (Materialized View) 技术详解 】](https://t.zsxq.com/Nevtm)

[【 Doris FE-BE架构深度解析 】](https://t.zsxq.com/Ksq0J)

[【 Doris 元数据管理深度解析：基于 BDBJE 的强一致存储与高可用实现 】](https://t.zsxq.com/OzA49)

[【 Doris副本与容错机制深度解析 】](https://t.zsxq.com/Vmafk)

[【 Doris 自动负载均衡机制深度解析：原理、实践与源码剖析 】](https://t.zsxq.com/7l0G9)

[【 Doris高并发查询支持：MPP架构、内存计算与队列管理的深度解析 】](https://t.zsxq.com/Tn3Ez)

[【 Doris延迟物化（Late Materialization）技术详解：原理、实现与源码分析 】](https://t.zsxq.com/oVZzu)

[【 Doris 智能缓存深度解析：BE节点Block Cache的设计与实现 】](https://t.zsxq.com/BKpGb)

[【 Doris查询限流与资源隔离详解：原理、源码与实践 】](https://t.zsxq.com/F5BHI)

[【 Kafka分布式集群架构详解：从Broker到KRaft的演进 】](https://t.zsxq.com/DqAES)

[【 Kafka主题（Topic）与分区（Partition）深度解析：架构、原理与源码实现 】](https://t.zsxq.com/AvPxv)

[【 Kafka 生产者与消费者模型深度解析 】](https://t.zsxq.com/eJWjp)

[【 Kafka顺序写入与日志分段机制深度解析 】](https://t.zsxq.com/68m92)

[【 Kafka索引设计详解：稀疏索引与时间索引的深度剖析 】](https://t.zsxq.com/0jble)

[【 Kafka零拷贝（Zero-Copy）详解：原理、实现与源码分析 】](https://t.zsxq.com/9D5vh)

[【 Kafka数据压缩详解：原理、算法与源码深度剖析 】](https://t.zsxq.com/VFyGs)

[【 Kafka副本同步机制（ISR）深度解析 】](https://t.zsxq.com/ybn3a)

[【 Kafka Leader 选举机制深度解析：从原理到源码剖析 】](https://t.zsxq.com/eT04T)

[【 Kafka数据持久化与保留策略深度解析 】](https://t.zsxq.com/4GXzj)

[【 Kafka生产者深度解析：分区策略、幂等性与事务支持 】](https://t.zsxq.com/75CoK)

[【 Kafka消费者特性详解 】](https://t.zsxq.com/NN5C9)

[【 Kafka二进制协议深度解析：高效紧凑的TCP通信基石 】](https://t.zsxq.com/Mdl6W)

[【 Kafka批处理（Batching）详解：从原理到源码分析 】](https://t.zsxq.com/1AiIS)

[【 Kafka线程模型深度解析：从Broker到客户端的全链路并发机制 】](https://t.zsxq.com/P7Lrn)

[【 Kafka Exactly-Once 语义详解：从原理到源码实现 】](https://t.zsxq.com/QGNgi)

[【 Elasticsearch底层存储与索引引擎详解（基于Apache Lucene） 】](https://t.zsxq.com/ksZur)

[【 Elasticsearch集群与节点深度解析：架构、角色与源码实现 】](https://t.zsxq.com/4IchN)

[【 Elasticsearch分片(Shard)深度解析：架构、原理与源码实现 】](https://t.zsxq.com/ntkPg)

[【 Elasticsearch集群发现（Discovery）机制深度解析：从Zen Discovery到现代实现 】](https://t.zsxq.com/Uyh4B)

[【 Elasticsearch相关性评分 (Relevance Scoring) 详解：基于TF-IDF与BM25算法 】](https://t.zsxq.com/DeZco)

[【 Elasticsearch搜索上下文（Search Context）深度解析：Query与Filter的原理、源码与实践 】](https://t.zsxq.com/oOHha)

[【 Elasticsearch跨索引搜索（Cross-Index Search）详解：原理、实践与优化 】](https://t.zsxq.com/nMNaj)

[【 Elasticsearch副本机制（Replication）深度解析：从原理到源码 】](https://t.zsxq.com/f3u5V)

[【 Elasticsearch 快照与恢复 (Snapshot & Restore) 详解：原理、配置与源码分析 】](https://t.zsxq.com/C6oHx)

[【 Elasticsearch跨集群复制（CCR）深度解析：异步复制与灾备实践 】](https://t.zsxq.com/APA2P)

[【 Elasticsearch索引生命周期管理（ILM）详解：从原理到实践 】](https://t.zsxq.com/gr8iI)

[【 Elasticsearch缓存机制深度解析：从原理到源码 】](https://t.zsxq.com/YA6MV)

[【 Elasticsearch索引优化深度解析：索引模板、动态映射与索引压缩 】](https://t.zsxq.com/N9tcA)

[【 Elasticsearch查询优化深度解析：查询重写与提前终止机制 】](https://t.zsxq.com/KwRCU)

[【 Hive架构组件详解 】](https://t.zsxq.com/5KFMf)

[【 Hive Metastore详解：元数据存储的核心组件 】](https://t.zsxq.com/izaQ8)

[【 Hive与HiveQL：大数据仓库的SQL之门 】](https://t.zsxq.com/6leka)

[【 Hive执行引擎详解：从MapReduce到Tez再到Spark 】](https://t.zsxq.com/AkNjG)

[【 Hive数据存储详解：基于HDFS的高效存储与优化 】](https://t.zsxq.com/etDAK)

[【 Hive分区与分桶详解：优化大数据查询与处理的利器 】](https://t.zsxq.com/1zKN9)

[【 Hive SerDe详解：数据序列化与反序列化的核心组件 】](https://t.zsxq.com/obCZO)

[【 Hive用户定义函数(UDF)全面详解 】](https://t.zsxq.com/FJSTc)

[【 Hive索引详解：加速查询的利器 】](https://t.zsxq.com/GvMAk)

[【 Hive视图与物化视图详解 】](https://t.zsxq.com/aSRUh)

[【 Hive性能优化详解 】](https://t.zsxq.com/49XQH)

[【 Hive事务支持详解：ACID、Delta文件与源码分析 】](https://t.zsxq.com/0Pv9S)

[【 Hive动态分区裁剪详解：原理、实现与性能优化 】](https://t.zsxq.com/gO0qK)

[【 HBase分布式架构深度解析：从设计理念到源码实现 】](https://t.zsxq.com/HCcF1)

[【 HBase数据模型详解：从概念到源码的深度剖析 】](https://t.zsxq.com/Ktdy5)

[【 HBase存储引擎详解：基于LSM-Tree的高性能写入架构 】](https://t.zsxq.com/oC9tD)

[【 HBase Region管理机制深度解析：从核心概念到源码实现 】](https://t.zsxq.com/UNtOH)

[【 HBase读写流程详解：原理、源码与实践 】](https://t.zsxq.com/Gjr6T)

[【 HBase Compaction（合并）机制深度解析：从原理到源码 】](https://t.zsxq.com/BowQr)

[【 HBase缓存机制深度解析：BlockCache、MemStore与BucketCache的协同艺术 】](https://t.zsxq.com/pyiqS)

[【 HBase过滤器详解：服务端数据过滤机制与实战 】](https://t.zsxq.com/3c6Dg)

[【 HBase协处理器（Coprocessor）详解：原理、实践与性能优化 】](https://t.zsxq.com/Zn9HB)

[【 HBase备份与恢复详解：保障数据可靠性的核心机制 】](https://t.zsxq.com/cP7We)

[【 HBase性能优化详解：从架构原理到实战调优 】](https://t.zsxq.com/1zuDJ)

[【 HBase监控与运维详解：基于内置工具的集群状态管理 】](https://t.zsxq.com/FXglZ)
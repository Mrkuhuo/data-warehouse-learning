# 开源不易，请各位朋友点个 ***★star★*** 支持一下，非常感谢~

[【 Github地址：https://github.com/Mrkuhuo/data-warehouse-learning 】](https://github.com/Mrkuhuo/data-warehouse-learning)

[【 Gitee 地址：https://gitee.com/wzylzjtn/data-warehouse-learning 】](https://gitee.com/wzylzjtn/data-warehouse-learning)

#### 介绍

《实时/离线数仓实战》代码，其中 **warehouseV1** 和 **warehouseV2** 的区别是V1业务逻辑较为简单，V2业务逻辑比较齐全，V1的模拟数据生产是通过 **Python** 脚本模拟生成，V2的模拟数据是通过 **JAVA** 代码来生成。

#### 技术架构

![技术架构](src/main/java/org/bigdatatechcir/warehousev1/images/jiagou.png)

该电商数仓（实时/离线）项目技术架构分为四部分：
1. 数据源模块：采用 **JAVA** 代码来生成电商业务数据写入 **MySQL** ，生成用户日志数据写入 **Kafka** ，两者都可以在配置文件中配置需要生成数据的日期
2. 数据采集模块：使用 **Dinky** 开发 **FlinkSQL** 代码来消费 **Kafka** 数据并写入 **Doris** \ **Paimon** ODS 层，使用 **DolphinScheduler** 配置 **SeaTunnel** 任务同步 **MySQL** 业务数据到 **Doris** \ **Paimon** ODS 层
3. 数仓模块：数仓模块采用业界通用的 **ODS** -> **DWD/DIM** -> **DWS** -> **ADS** 四级分层，数据在 **Doris** \ **Paimon** 中分别通过批量调度和实时处理的方式进行流转
4. 数据可视化：**ADS** 层和 **DWS** 层的数据可以通过 **SuperSet** 和 **DataRT** 来进行报表和大屏制作及展示 

# 通用部分

#### 第一步 组件安装

![安装文档](src/main/java/org/bigdatatechcir/warehousev1/images/anzhuangbuzhou1.png)

#### 第二步 模拟数据生成

![模拟数据生成](src/main/java/org/bigdatatechcir/warehousev1/images/monishuju.png)

生成业务库数据如下图所示：

![业务库数据](src/main/java/org/bigdatatechcir/warehousev1/images/yewushuju.png)

生成用户日志数据如下图所示：

![用户日志数据](src/main/java/org/bigdatatechcir/warehousev1/images/yonghurizhishuju.png)

# 离线数仓建设部分

#### 涉及组件：Kafka + Flink + Doris + Seatunnel + Dolphinscheduler

#### 第一步  数据采集

**Kafka** 数据通过 **Flink** 接入 **Doris**

![Kafka 数据接入](src/main/java/org/bigdatatechcir/warehousev1/images/flink.png)

**MySQL** 数据通过 **SeaTunnel** 接入 **Doris**

![MySQL 数据接入](src/main/java/org/bigdatatechcir/warehousev1/images/seatunnel.png)

#### 第二步  **Doris ODS** 层建设

数据采集进 **Doris ODS** 层，实现效果如下图所示：

![Doris ODS层建设](src/main/java/org/bigdatatechcir/warehousev1/images/ods.png)

#### 第三步  **Doris DIM** 层建设

开发 **DorisSQL** 进行 **DIM** 层数据处理

![Doris DIM层处理逻辑](src/main/java/org/bigdatatechcir/warehousev1/images/dim.png)

**DIM** 层数据实现效果如下图：

![Doris DIM层数据库](src/main/java/org/bigdatatechcir/warehousev1/images/dwddatabase.png)

#### 第四步  **Doris DWD** 层建设

开发 **DorisSQL** 进行 **DWD** 层数据处理

![Doris DWD层处理逻辑](src/main/java/org/bigdatatechcir/warehousev1/images/dwd.png)

**DWD** 层数据实现效果如下图：

![Doris DWD层数据库](src/main/java/org/bigdatatechcir/warehousev1/images/dimdatabase.png)

#### 第五步  **Doris DWS** 层建设

开发 **DorisSQL** 进行 **DWS** 层数据处理

![Doris DWS层处理逻辑](src/main/java/org/bigdatatechcir/warehousev1/images/dws.png)

**DWS** 层数据实现效果如下图：

![Doris DWS层数据库](src/main/java/org/bigdatatechcir/warehousev1/images/dwsdatabase.png)

#### 第六步  **Doris ADS** 层建设

开发 **DorisSQL** 进行 **ADS** 层数据处理

![Doris ADS层处理逻辑](src/main/java/org/bigdatatechcir/warehousev1/images/ads.png)

**ADS** 层数据实现效果如下图：

![Doris ADS层数据库](src/main/java/org/bigdatatechcir/warehousev1/images/adsdatabase.png)

#### 第七步  任务编排

最终的任务概览如下图所示

![Doris 任务概览](src/main/java/org/bigdatatechcir/warehousev1/images/allrenwu.png)

任务编排效果如下图所示

![Doris 任务概览](src/main/java/org/bigdatatechcir/warehousev1/images/bianpai.png)

#### 第八步  数据展示

![大屏](src/main/java/org/bigdatatechcir/warehousev1/images/daping1.png)

# 实时数仓（数据湖）建设

#### 实时数仓建设，涉及组件：Kafka + Flink(CDC/SQL/UDF) + Paimon + Hive + Dinky

#### 第一步  **Paimon ODS** 层建设

**Kafka** 数据通过 **FlinkSQL** 接入 **Paimon** ,实际数据落到 **Hive**

**MySQL** 数据通过 **FlinkCDC** 接入 **Paimon** ,实际数据落到 **Hive**

![Kafka/MySQL 数据接入](src/main/java/org/bigdatatechcir/warehousev1/images/paimon%20ods.png)

数据采集进 **Paimon ODS** 层，实现效果如下图所示：

![Paimon ODS层建设](src/main/java/org/bigdatatechcir/warehousev1/images/hiveods.png)

#### 第二步  **Paimon DWD** 层建设

开发 **FlinkSQL** 进行 **DWD** 层数据处理

![Paimon DWD层建设](src/main/java/org/bigdatatechcir/warehousev1/images/paimondwd.png)

**DWD** 层数据实现效果如下图：

![Paimon DWD层建设](src/main/java/org/bigdatatechcir/warehousev1/images/hivedwd.png)

#### 第三步  **Paimon DIM** 层建设

开发 **FlinkSQL** 进行 **DIM** 层数据处理

![Paimon DIM层建设](src/main/java/org/bigdatatechcir/warehousev1/images/paimondim.png)

**DIM** 层数据实现效果如下图：

![Paimon DIM层建设](src/main/java/org/bigdatatechcir/warehousev1/images/hivedim.png)

#### 第四步  **Paimon DWS** 层建设

开发 **FlinkSQL** 进行 **DWS** 层数据处理

![Paimon DWS层建设](src/main/java/org/bigdatatechcir/warehousev1/images/paimondws.png)

**DWS** 层数据实现效果如下图：

![Paimon DWS层建设](src/main/java/org/bigdatatechcir/warehousev1/images/hivedws.png)

#### 第五步  **Paimon ADS** 层建设

开发 **FlinkSQL** 进行 **ADS** 层数据处理

![Paimon ADS层建设](src/main/java/org/bigdatatechcir/warehousev1/images/paimonads.png)

**ADS** 层数据实现效果如下图：

![Paimon ADS层建设](src/main/java/org/bigdatatechcir/warehousev1/images/hiveads.png)

#### 第六步  **Doris Catalog** 连接 **Paimon** + **DataRT** 进行数据展示

![大屏](src/main/java/org/bigdatatechcir/warehousev1/images/daping2.png)
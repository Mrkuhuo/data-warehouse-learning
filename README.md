# 开源不易，请各位朋友点个 ***★star★*** 支持一下，非常感谢~

#### 介绍

大数据技能圈《实时/离线数仓实战》配套代码其中 **warehouseV1** 和 **warehouseV2** 的区别是V1业务逻辑较为简单，V2业务逻辑比较齐全，V1的模拟数据生产是通过 **Python** 脚本模拟生成，V2的模拟数据是通过 **JAVA** 代码来生成。

#### 技术架构

![技术架构](src/main/java/org/bigdatatechcir/warehousev1/images/jiagou.png)

该电商项目技术架构分为四部分：
1. 数据源模块：采用 **JAVA** 代码来生成电商业务数据写入 **MySQL** ，生成用户日志数据写入 **Kafka** ，两者都可以在配置文件中配置需要生成数据的日期
2. 数据采集模块：使用 **Dinky** 开发 **FlinkSQL** 代码来消费 **Kafka** 数据并写入 **Doris** \ **Paimon** ODS 层，使用 **DolphinScheduler** 配置 **SeaTunnel** 任务同步 **MySQL** 业务数据到 **Doris** \ **Paimon** ODS 层
3. 数仓模块：数仓模块采用业界通用的 **ODS** -> **DWD/DIM** -> **DWS** -> **ADS** 四级分层，数据在 **Doris** \ **Paimon** 中分别通过批量调度和实时处理的方式进行流转
4. 数据可视化：**ADS** 层和 **DWS** 层的数据可以通过 **SuperSet** 和 **DataRT** 来进行报表和大屏制作及展示 

#### 第一步 组件安装

![安装文档](src/main/java/org/bigdatatechcir/warehousev1/images/anzhaungbuzhou.png)

#### 第二步 模拟数据生成

![模拟数据生成](src/main/java/org/bigdatatechcir/warehousev1/images/monishuju.png)

生成业务库数据如下图所示：

![业务库数据](src/main/java/org/bigdatatechcir/warehousev1/images/yewushuju.png)

生成用户日志数据如下图所示：

![用户日志数据](src/main/java/org/bigdatatechcir/warehousev1/images/yonghurizhishuju.png)

#### 第三步  数据采集

**Kafka** 数据通过 **Flink** 接入 **Doris**

![Kafka 数据接入](src/main/java/org/bigdatatechcir/warehousev1/images/flink.png)

**MySQL** 数据通过 **SeaTunnel** 接入 **Doris**

![MySQL 数据接入](src/main/java/org/bigdatatechcir/warehousev1/images/seatunnel.png)

#### 第四步  **Doris ODS** 层建设

数据采集进 **Doris ODS** 层，实现效果如下图所示：

![Doris ODS层建设](src/main/java/org/bigdatatechcir/warehousev1/images/ods.png)

#### 第五步  **Doris DIM** 层建设

开发 **DorisSQL** 进行 **DIM** 层数据处理

![Doris DIM层处理逻辑](src/main/java/org/bigdatatechcir/warehousev1/images/dim.png)

**DIM** 层数据实现效果如下图：

![Doris DIM层数据库](src/main/java/org/bigdatatechcir/warehousev1/images/dwddatabase.png)

#### 第六步  **Doris DWD** 层建设

开发 **DorisSQL** 进行 **DWD** 层数据处理

![Doris DWD层处理逻辑](src/main/java/org/bigdatatechcir/warehousev1/images/dwd.png)

**DWD** 层数据实现效果如下图：

![Doris DWD层数据库](src/main/java/org/bigdatatechcir/warehousev1/images/dimdatabase.png)

#### 第七步  **Doris DWS** 层建设

开发 **DorisSQL** 进行 **DWS** 层数据处理

![Doris DWS层处理逻辑](src/main/java/org/bigdatatechcir/warehousev1/images/dws.png)

**DWS** 层数据实现效果如下图：

![Doris DWS层数据库](src/main/java/org/bigdatatechcir/warehousev1/images/dwsdatabase.png)

#### 第八步  **Doris ADS** 层建设

开发 **DorisSQL** 进行 **ADS** 层数据处理

![Doris ADS层处理逻辑](src/main/java/org/bigdatatechcir/warehousev1/images/ads.png)

**ADS** 层数据实现效果如下图：

![Doris ADS层数据库](src/main/java/org/bigdatatechcir/warehousev1/images/adsdatabase.png)

#### 第九步  任务编排

最终的任务概览如下图所示

![Doris 任务概览](src/main/java/org/bigdatatechcir/warehousev1/images/allrenwu.png)

任务编排效果如下图所示

![Doris 任务概览](src/main/java/org/bigdatatechcir/warehousev1/images/bianpai.png)

## 关注微信公众号《大数据技能圈》获取最新消息

![公众号](src/main/java/org/bigdatatechcir/warehousev1/images/gongzhonghao.jpg)

![公众号](src/main/java/org/bigdatatechcir/warehousev1/images/weixinshcang.png)

## 添加作者微信

![作者微信](src/main/java/org/bigdatatechcir/warehousev1/images/weixin.jpg)

## 离线文档获取：知识星球《大数据技能圈》

![知识星球](src/main/java/org/bigdatatechcir/warehousev1/images/zhishixingqiu.jpg)
# data-warehouse-learning

#### 介绍

大数据技能圈《实时/离线数仓实战》配套代码其中warehouseV1和warehouseV2的区别是V1业务逻辑较为简单，V2业务逻辑比较齐全，V1的模拟数据生产是通过Python脚本模拟生成，V2的模拟数据是通过JAVA代码来生成。

# 开源不易，请各位朋友点个 ***★star★*** 支持一下，非常感谢~

#### 技术架构

![技术架构](src/main/java/org/bigdatatechcir/warehousev1/images/jiagou.png)

该电商项目技术架构分为四部分：
1. 数据源模块：采用 JAVA 代码来生成电商业务数据写入 MySQL ，生成用户日志数据写入 Kafka ，两者都可以在配置文件中配置需要生成数据的日期
2. 数据采集模块：使用 Dinky 开发 FlinkSQL 代码来消费Kafka数据并写入 Doris\Paimon ODS 层，使用 DolphinScheduler 配置 SeaTunnel 任务同步 MySQL 业务数据到 Doris\Paimon ODS 层
3. 数仓模块：数仓模块采用业界通用的 ODS -> DWD/DIM -> DWS -> ADS 四级分层，数据在 Doris\Paimon 中分别通过批量调度和实时处理的方式进行流转
4. 数据可视化：ADS 层和 DWS 层的数据可以通过 SuperSet 和 DataRT 来进行报表和大屏制作及展示 

#### 第一步 组件安装

[【实时离线数仓实战组件安装教程】](https://mp.weixin.qq.com/s?__biz=Mzg5Mzg3MzkwNA==&mid=2247488063&idx=1&sn=10ecc03fccfc90649e308aa8c357dcaf&chksm=c02969a0f75ee0b67769fba698b2a00746eb25ffcf19f1f56e324dea33a315703754f0736b2c#rd)

文档获取：知识星球《大数据技能圈》

![知识星球](src/main/java/org/bigdatatechcir/warehousev1/images/zhishixingqiu.jpg)

关注微信公众号《大数据技能圈》

![公众号](src/main/java/org/bigdatatechcir/warehousev1/images/gongzhonghao.jpg)

添加作者微信

![作者微信](src/main/java/org/bigdatatechcir/warehousev1/images/weixin.jpg)
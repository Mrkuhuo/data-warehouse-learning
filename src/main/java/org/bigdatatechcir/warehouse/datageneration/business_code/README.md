# 电商业务数据生成器

## 项目说明
该模块用于生成模拟的电商业务数据，包括商品、订单、用户、活动等全业务链路数据。数据直接写入MySQL数据库，用于数据仓库的构建和分析。

## 数据模块
1. 基础数据
   - 商品分类（一级、二级、三级）
   - 品牌信息
   - 商品属性
   - 销售属性
   - 地区信息

2. 商品数据
   - SKU信息
   - SPU信息
   - 商品图片
   - 商品属性值
   - 销售属性值

3. 营销数据
   - 促销活动
   - 优惠券
   - 秒杀商品
   - 广告Banner

4. 交易数据
   - 订单信息
   - 订单明细
   - 支付信息
   - 退款信息

5. 用户数据
   - 用户信息
   - 收货地址
   - 购物车
   - 收藏夹
   - 评价信息

6. 仓储数据
   - 仓库信息
   - 库存信息
   - 出库任务
   - 库存成本

## 目录结构
```
business_code/
├── generator/          # 数据生成器
│   ├── BaseDataGenerator.java      # 基础数据
│   ├── ProductDataGenerator.java   # 商品数据
│   ├── ActivityDataGenerator.java  # 活动数据
│   ├── OrderDataGenerator.java     # 订单数据
│   ├── UserBehaviorGenerator.java  # 用户行为
│   ├── WarehouseDataGenerator.java # 仓储数据
│   └── CMSDataGenerator.java       # 内容管理
├── util/              # 工具类
│   ├── DbUtil.java   # 数据库操作
│   └── RandomUtil.java # 随机数据生成
└── BusinessApplication.java  # 主应用类
```

## 配置说明
1. 数据库配置（application.yml）：
```yaml
spring:
  datasource:
    url: jdbc:mysql://localhost:3306/gmall?useUnicode=true&characterEncoding=utf-8&useSSL=false
    username: gmall
    password: gmall
    hikari:
      maximum-pool-size: 10
      minimum-idle: 5
```

2. 生成控制（application.yml）：
```yaml
generator:
  batch-size: 1000  # 基准批量大小
  interval: 5000    # 生成间隔（毫秒）
```

## 数据生成规则

### 1. 基础数据（一次性生成）
- 分类：三级分类结构
- 品牌：知名品牌列表
- 属性：常用商品属性
- 地区：省份和地区信息

### 2. 业务数据（持续生成）
- 商品数据：每批次 batchSize/10
- 活动数据：每批次 batchSize/10
- 优惠券数据：每批次 batchSize/10
- 订单数据：每批次 batchSize
- 用户行为：每批次 batchSize
- 仓储数据：每批次 batchSize/5
- CMS数据：每批次 batchSize/20

### 3. 数据关联规则
- SKU关联SPU
- 订单关联商品和用户
- 活动关联商品
- 优惠券关联订单
- 库存关联商品和仓库

## 运行说明
1. 环境准备：
   - JDK 8+
   - Maven 3.6+
   - MySQL 5.7+

2. 创建数据库：
```sql
CREATE DATABASE gmall;
mysql -u root -p gmall < gmall.sql
```

3. 编译运行：
```bash
mvn clean package
java -jar target/data-warehouse-learning-1.0-SNAPSHOT.jar
```

## 日志说明

### 1. 日志格式
程序使用标准的日志格式：
```
%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n
```

### 2. 日志级别
- INFO：表更新提示（"正在更新 xxx 表..."）
- ERROR：数据库操作错误和其他异常

### 3. 日志输出示例
```
2025-03-05 01:41:52.643 [main] INFO  o.b.w.d.b.g.BaseDataGenerator - 正在更新 base_category1 表...
2025-03-05 01:41:52.747 [main] INFO  o.b.w.d.b.g.BaseDataGenerator - 正在更新 base_category2 表...
```

## 注意事项
1. 数据量控制：
   - 合理设置batch-size和interval
   - 监控数据库性能和空间

2. 数据清理：
   - 建议定期清理历史数据
   - 保留必要的归档数据

3. 异常处理：
   - 程序会记录详细错误日志
   - 数据库异常时会自动重试

4. 扩展建议：
   - 可以增加更多的数据特征
   - 可以调整数据分布规律 
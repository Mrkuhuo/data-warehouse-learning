# 用户行为日志生成器

## 项目说明
该模块用于生成模拟的用户行为日志数据，数据格式符合 Flink SQL 的处理要求。生成的数据将被发送到 Kafka 主题中，用于后续的实时数据处理。

## 数据结构
生成的日志数据包含以下主要字段：
```json
{
    "common": {
        "ar": "地区",
        "ba": "品牌",
        "ch": "渠道",
        "is_new": "是否新用户",
        "md": "机型",
        "mid": "设备id",
        "os": "操作系统",
        "uid": "用户id",
        "vc": "版本号"
    },
    "start": {
        "entry": "入口",
        "loading_time": "加载时长",
        "open_ad_id": "广告id",
        "open_ad_ms": "广告时长",
        "open_ad_skip_ms": "广告跳过时长"
    },
    "page": {
        "during_time": "页面停留时长",
        "item": "商品id",
        "item_type": "商品类型",
        "last_page_id": "上一页id",
        "page_id": "页面id",
        "source_type": "来源类型"
    },
    "actions": "用户行为数组",
    "displays": "曝光数据数组",
    "ts": "时间戳"
}
```

## 目录结构
```
userlog_code/
├── model/          # 数据模型
│   ├── UserLog.java
│   ├── Common.java
│   ├── Start.java
│   ├── Page.java
│   └── Error.java
├── generator/      # 数据生成器
│   └── UserLogGenerator.java
├── util/          # 工具类
│   └── RandomUtil.java
└── UserLogApplication.java  # 主应用类
```

## 配置说明
在 application.yml 中配置：
```yaml
spring:
  application:
    name: user-log-generator

kafka:
  bootstrap-servers: 192.168.244.129:9092
  topic: ODS_BASE_LOG
  
generator:
  interval: 1000  # 生成日志的时间间隔（毫秒）
```

## 运行说明
1. 确保环境准备：
   - JDK 8+
   - Maven 3.6+
   - Kafka 服务可用

2. 编译项目：
```bash
mvn clean package
```

3. 运行应用：
```bash
java -jar target/data-warehouse-learning-1.0-SNAPSHOT.jar
```

4. 验证数据：
```bash
# 使用Kafka消费者查看生成的数据
kafka-console-consumer.sh --bootstrap-server 192.168.244.129:9092 --topic ODS_BASE_LOG --from-beginning
```

## 数据生成规则
1. 用户信息：
   - 随机生成用户ID、设备ID
   - 在预设列表中随机选择地区、品牌、渠道等

2. 页面信息：
   - 随机生成页面停留时长
   - 按照预定义规则生成页面跳转路径

3. 行为数据：
   - 随机生成用户点击、滑动等行为
   - 生成商品曝光数据

4. 时间控制：
   - 按照配置的时间间隔持续生成数据
   - 时间戳使用当前系统时间

## 注意事项
1. 数据量控制：
   - 可通过修改生成间隔调整数据量
   - 注意监控Kafka积压情况

2. 异常处理：
   - 程序会自动重试发送失败的消息
   - Kafka不可用时会本地打印数据

3. 性能优化：
   - 使用了批量发送机制
   - 采用异步发送提高吞吐量 
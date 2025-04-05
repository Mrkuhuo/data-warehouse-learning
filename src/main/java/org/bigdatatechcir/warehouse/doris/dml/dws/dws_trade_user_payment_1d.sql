/*
 * 表名: dws_trade_user_payment_1d
 * 说明: 交易域用户粒度支付最近1日汇总表
 * 数据粒度: 用户 + 日期
 * 存储策略: 分区存储，按日期分区
 * 刷新周期: 每日刷新
 * 应用场景:
 *   - 用户支付行为分析：了解用户每日的支付频次和金额特征
 *   - 支付转化率分析：结合订单数据分析下单到支付的转化情况
 *   - 用户支付偏好研究：分析用户的支付习惯和偏好
 *   - 支付风险监控：识别异常支付行为，预防支付风险
 *   - 营销活动效果评估：评估促销活动对支付转化的影响
 */
-- DROP TABLE IF EXISTS dws.dws_trade_user_payment_1d;
CREATE TABLE dws.dws_trade_user_payment_1d
(
    /* 维度字段 */
    `user_id`                   VARCHAR(255) COMMENT '用户ID - 标识用户的唯一标识',
    `k1`                        DATE NOT NULL COMMENT '数据日期 - 分区字段，记录数据所属日期',
    
    /* 度量值字段 - 支付指标 */
    `payment_count_1d`          BIGINT COMMENT '最近1日支付次数 - 用户当天完成支付的总次数',
    `payment_num_1d`            BIGINT COMMENT '最近1日支付商品件数 - 用户当天支付购买的商品总数量',
    `payment_amount_1d`         DECIMAL(16, 2) COMMENT '最近1日支付金额 - 用户当天支付的总金额'
)
ENGINE=OLAP
UNIQUE KEY(`user_id`,`k1`)
COMMENT '交易域用户粒度支付最近1日汇总事实表'
PARTITION BY RANGE(`k1`) ()
DISTRIBUTED BY HASH(`user_id`)
PROPERTIES
(
    "replication_allocation" = "tag.location.default: 1",
    "is_being_synced" = "false",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false",
    "dynamic_partition.enable" = "true",           /* 启用动态分区功能 */
    "dynamic_partition.time_unit" = "DAY",         /* 按天进行分区 */
    "dynamic_partition.start" = "-60",             /* 保留60天历史数据 */
    "dynamic_partition.end" = "3",                 /* 预创建未来3天分区 */
    "dynamic_partition.prefix" = "p",              /* 分区名称前缀 */
    "dynamic_partition.buckets" = "32",            /* 每个分区的分桶数 */
    "dynamic_partition.create_history_partition" = "true" /* 创建历史分区 */
);

/*
 * 表设计说明：
 *
 * 1. 主键与分布设计：
 *    - 主键：使用user_id + k1复合主键，支持用户在不同日期的支付行为分析
 *    - 分桶：按用户ID分桶(32桶)，优化用户级查询性能
 *    - 分区：使用日期（k1）分区，支持历史数据生命周期管理，提升查询效率
 *
 * 2. 数据组织：
 *    - 支付量指标：记录用户每日支付次数和支付购买的商品件数，反映支付规模
 *    - 支付金额指标：记录支付涉及的金额，评估用户消费能力
 *    - 时间范围：聚焦于最近1日的支付行为，适用于日常监控和短期分析
 * 
 * 3. 存储管理：
 *    - 动态分区：自动管理历史数据，保留最近60天数据，预创建3天分区
 *    - 存储格式：使用V2存储格式和轻量级模式变更，优化存储效率
 *
 * 4. 典型应用场景：
 *    - 用户支付行为分析：识别用户的支付习惯和偏好
 *    - 支付转化分析：结合订单数据，分析下单到支付的转化率和时效
 *    - 支付异常监控：识别异常的支付行为，预防支付风险
 *    - 支付方式分析：结合支付方式数据，分析用户支付偏好
 *    - 营销效果评估：评估促销活动对支付行为的影响
 *
 * 5. 优化建议：
 *    - 扩展为n日汇总表(7日、30日)，支持中长期支付行为分析
 *    - 增加支付时效指标，分析下单到支付的平均时间间隔
 *    - 添加支付转化率指标，评估下单到支付的转化效率
 *    - 结合订单和退款表，构建完整的订单-支付-退款分析链路
 *    - 增加支付方式维度，分析不同支付方式的使用情况
 */
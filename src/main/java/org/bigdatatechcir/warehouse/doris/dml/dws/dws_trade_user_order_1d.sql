/*
 * 表名: dws_trade_user_order_1d
 * 说明: 交易域用户粒度订单最近1日汇总表
 * 数据粒度: 用户 + 日期
 * 存储策略: 分区存储，按日期分区
 * 刷新周期: 每日刷新
 * 应用场景:
 *   - 用户消费行为分析：了解用户的下单频率和消费金额
 *   - 活动效果评估：分析促销活动对用户下单的影响
 *   - 用户价值评估：通过订单金额评估用户的价值贡献
 *   - 销售业绩监控：监控每日用户订单量和销售额的变化
 *   - 优惠策略效果分析：评估不同优惠方式（活动、优惠券）的影响
 */
-- DROP TABLE IF EXISTS dws.dws_trade_user_order_1d;
CREATE TABLE dws.dws_trade_user_order_1d
(
    /* 维度字段 */
    `user_id`                   VARCHAR(255) COMMENT '用户ID - 标识用户的唯一标识',
    `k1`                        DATE NOT NULL COMMENT '数据日期 - 分区字段，记录数据所属日期',
    
    /* 度量值字段 - 订单量指标 */
    `order_count_1d`            BIGINT COMMENT '最近1日下单次数 - 用户当天下单的总次数',
    `order_num_1d`              BIGINT COMMENT '最近1日下单商品件数 - 用户当天购买的商品总数量',
    
    /* 度量值字段 - 金额指标 */
    `order_original_amount_1d`  DECIMAL(16, 2) COMMENT '最近1日下单原始金额 - 优惠前的订单总金额',
    `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT '最近1日活动优惠金额 - 营销活动带来的优惠总额',
    `coupon_reduce_amount_1d`   DECIMAL(16, 2) COMMENT '最近1日优惠券优惠金额 - 优惠券带来的优惠总额',
    `order_total_amount_1d`     DECIMAL(16, 2) COMMENT '最近1日下单最终金额 - 优惠后的实际支付总金额'
)
ENGINE=OLAP
UNIQUE KEY(`user_id`,`k1`)
COMMENT '交易域用户粒度订单最近1日汇总事实表'
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
 *    - 主键：使用user_id + k1复合主键，支持用户在不同日期的订单行为分析
 *    - 分桶：按用户ID分桶(32桶)，优化用户级查询性能
 *    - 分区：使用日期（k1）分区，支持历史数据生命周期管理，提升查询效率
 *
 * 2. 数据组织：
 *    - 订单量指标：记录用户每日下单次数和商品件数，反映用户购买频率和规模
 *    - 金额指标：包含原始金额、优惠金额和最终金额，全面描述用户消费价值和优惠情况
 *    - 时间范围：聚焦于最近1日的订单行为，适用于日常销售监控和短期分析
 * 
 * 3. 存储管理：
 *    - 动态分区：自动管理历史数据，保留最近60天数据，预创建3天分区
 *    - 存储格式：使用V2存储格式和轻量级模式变更，优化存储效率
 *
 * 4. 典型应用场景：
 *    - 用户价值分析：结合RFM模型，评估用户的最近消费金额(Monetary)
 *    - 促销效果评估：分析不同促销手段对用户实际消费的影响和贡献
 *    - 用户消费画像：构建用户短期消费习惯特征，如消费频率、规模、优惠敏感度等
 *    - 销售异常监控：及时发现用户消费行为的波动和异常
 *
 * 5. 优化建议：
 *    - 扩展为n日汇总表(7日、30日)，支持中长期消费行为分析
 *    - 增加SKU种类数和品类覆盖指标，评估用户购买的多样性
 *    - 添加客单价指标，分析用户单次订单价值
 *    - 考虑与加购行为表关联，构建完整的加购-下单转化链路
 *    - 引入同环比指标，便于分析用户消费行为的变化趋势
 */
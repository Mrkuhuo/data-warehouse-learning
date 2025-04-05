/*
 * 表名: dws_trade_user_order_nd
 * 说明: 交易域用户粒度订单最近n日汇总表
 * 数据粒度: 用户 + 日期
 * 存储策略: 分区存储，按日期分区
 * 刷新周期: 每日刷新
 * 应用场景:
 *   - 用户中长期消费行为分析：了解用户在更长时间范围内的购买习惯
 *   - 用户价值评估：通过中长期订单量和金额评估用户的稳定价值
 *   - 用户生命周期管理：分析用户在不同阶段的消费变化
 *   - 促销活动ROI分析：评估促销活动在中长期的投资回报
 *   - RFM模型构建：提供频次(Frequency)和金额(Monetary)的中长期数据
 */
-- DROP TABLE IF EXISTS dws.dws_trade_user_order_nd;
CREATE TABLE dws.dws_trade_user_order_nd
(
    /* 维度字段 */
    `user_id`                    VARCHAR(255) COMMENT '用户ID - 标识用户的唯一标识',
    `k1`                         DATE NOT NULL COMMENT '数据日期 - 分区字段，记录数据所属日期',
    
    /* 度量值字段 - 7日汇总指标 */
    `order_count_7d`             BIGINT COMMENT '最近7日下单次数 - 用户近7天下单的总次数',
    `order_num_7d`               BIGINT COMMENT '最近7日下单商品件数 - 用户近7天购买的商品总数量',
    `order_original_amount_7d`   DECIMAL(16, 2) COMMENT '最近7日下单原始金额 - 7天内优惠前的订单总金额',
    `activity_reduce_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日活动优惠金额 - 7天内营销活动带来的优惠总额',
    `coupon_reduce_amount_7d`    DECIMAL(16, 2) COMMENT '最近7日优惠券优惠金额 - 7天内优惠券带来的优惠总额',
    `order_total_amount_7d`      DECIMAL(16, 2) COMMENT '最近7日下单最终金额 - 7天内优惠后的实际支付总金额',
    
    /* 度量值字段 - 30日汇总指标 */
    `order_count_30d`            BIGINT COMMENT '最近30日下单次数 - 用户近30天下单的总次数',
    `order_num_30d`              BIGINT COMMENT '最近30日下单商品件数 - 用户近30天购买的商品总数量',
    `order_original_amount_30d`  DECIMAL(16, 2) COMMENT '最近30日下单原始金额 - 30天内优惠前的订单总金额',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '最近30日活动优惠金额 - 30天内营销活动带来的优惠总额',
    `coupon_reduce_amount_30d`   DECIMAL(16, 2) COMMENT '最近30日优惠券优惠金额 - 30天内优惠券带来的优惠总额',
    `order_total_amount_30d`     DECIMAL(16, 2) COMMENT '最近30日下单最终金额 - 30天内优惠后的实际支付总金额'
)
ENGINE=OLAP
UNIQUE KEY(`user_id`,`k1`)
COMMENT '交易域用户粒度订单最近n日汇总事实表'
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
 *    - 时间维度：分别提供7日和30日两个时间窗口的汇总指标，覆盖中期和长期分析
 *    - 指标维度：包含订单量指标(次数、件数)和金额指标(原始金额、优惠金额、最终金额)
 *    - 完整性：保持指标定义的一致性，便于与1日汇总表对比分析
 * 
 * 3. 存储管理：
 *    - 动态分区：自动管理历史数据，保留最近60天数据，预创建3天分区
 *    - 存储格式：使用V2存储格式和轻量级模式变更，优化存储效率
 *
 * 4. 典型应用场景：
 *    - RFM模型构建：提供用户消费频率(F)和金额(M)的中长期指标
 *    - 用户价值分层：基于中长期消费行为对用户进行价值分层
 *    - 用户行为稳定性分析：评估用户消费行为的稳定性和持续性
 *    - 营销活动效果跟踪：分析促销活动在中长期内的效果持续性
 *    - 用户生命周期管理：识别用户的活跃度变化和生命周期阶段
 *
 * 5. 优化建议：
 *    - 增加消费频率指标：如平均下单间隔天数，评估用户的消费频率稳定性
 *    - 添加客单价和件单价指标：分析用户的消费能力和消费习惯
 *    - 引入同比指标：与去年同期比较，评估用户消费的季节性变化
 *    - 增加SKU和品类指标：评估用户购买的多样性和忠诚度
 *    - 结合与1日表和加购表数据：构建完整的用户行为分析体系
 */
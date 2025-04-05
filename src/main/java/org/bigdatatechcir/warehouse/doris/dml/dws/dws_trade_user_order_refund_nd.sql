/*
 * 表名: dws_trade_user_order_refund_nd
 * 说明: 交易域用户粒度退单最近n日汇总表
 * 数据粒度: 用户 + 日期
 * 存储策略: 分区存储，按日期分区
 * 刷新周期: 每日刷新
 * 应用场景:
 *   - 用户中长期退单行为分析：评估用户的退单习惯和稳定性
 *   - 用户满意度趋势监控：通过退单变化趋势评估用户满意度变化
 *   - 商品质量问题追踪：中长期退单数据可揭示持续性商品质量问题
 *   - 售后服务效果评估：检验售后服务改进措施的中长期效果
 *   - 用户流失风险识别：通过中长期退单行为识别潜在流失风险用户
 */
-- DROP TABLE IF EXISTS dws.dws_trade_user_order_refund_nd;
CREATE TABLE dws.dws_trade_user_order_refund_nd
(
    /* 维度字段 */
    `user_id`                   VARCHAR(255) COMMENT '用户ID - 标识用户的唯一标识',
    `k1`                        DATE NOT NULL COMMENT '数据日期 - 分区字段，记录数据所属日期',
    
    /* 度量值字段 - 7日汇总指标 */
    `order_refund_count_7d`     BIGINT COMMENT '最近7日退单次数 - 用户近7天退单的总次数',
    `order_refund_num_7d`       BIGINT COMMENT '最近7日退单商品件数 - 用户近7天退回的商品总数量',
    `order_refund_amount_7d`    DECIMAL(16, 2) COMMENT '最近7日退单金额 - 用户近7天退单的总金额',
    
    /* 度量值字段 - 30日汇总指标 */
    `order_refund_count_30d`    BIGINT COMMENT '最近30日退单次数 - 用户近30天退单的总次数',
    `order_refund_num_30d`      BIGINT COMMENT '最近30日退单商品件数 - 用户近30天退回的商品总数量',
    `order_refund_amount_30d`   DECIMAL(16, 2) COMMENT '最近30日退单金额 - 用户近30天退单的总金额'
)
ENGINE=OLAP
UNIQUE KEY(`user_id`,`k1`)
COMMENT '交易域用户粒度退单最近n日汇总事实表'
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
 *    - 主键：使用user_id + k1复合主键，支持用户在不同日期的退单行为分析
 *    - 分桶：按用户ID分桶(32桶)，优化用户级查询性能
 *    - 分区：使用日期（k1）分区，支持历史数据生命周期管理，提升查询效率
 *
 * 2. 数据组织：
 *    - 时间维度：分别提供7日和30日两个时间窗口的汇总指标，覆盖中期和长期分析
 *    - 退单量指标：记录用户在不同时间范围内的退单次数和退回商品件数
 *    - 退单金额指标：记录不同时间范围内的退单金额，评估财务影响
 * 
 * 3. 存储管理：
 *    - 动态分区：自动管理历史数据，保留最近60天数据，预创建3天分区
 *    - 存储格式：使用V2存储格式和轻量级模式变更，优化存储效率
 *
 * 4. 典型应用场景：
 *    - 用户价值评估：结合订单与退单数据，全面评估用户稳定性和价值
 *    - 用户分层分群：基于退单行为特征进行用户分群，制定差异化策略
 *    - 商品质量问题诊断：识别长期存在退单问题的商品，进行质量改进
 *    - 售后服务优化：分析不同时期的退单趋势，评估售后服务改进效果
 *    - 经营风险预警：通过退单趋势变化预测潜在经营风险
 *
 * 5. 优化建议：
 *    - 增加退单率指标：计算退单次数/订单次数，评估退单发生比例
 *    - 添加退单时效指标：分析从下单到退单的平均时间间隔
 *    - 引入同比环比指标：与去年同期、上月同期比较，评估趋势变化
 *    - 增加退单原因维度：分析不同原因的退单占比和趋势变化
 *    - 结合订单表与1日表数据：构建完整的用户消费-退单分析体系
 */
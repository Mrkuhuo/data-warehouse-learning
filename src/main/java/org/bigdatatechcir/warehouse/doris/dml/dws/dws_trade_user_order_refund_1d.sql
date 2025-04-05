/*
 * 表名: dws_trade_user_order_refund_1d
 * 说明: 交易域用户粒度退单最近1日汇总表
 * 数据粒度: 用户 + 日期
 * 存储策略: 分区存储，按日期分区
 * 刷新周期: 每日刷新
 * 应用场景:
 *   - 用户退单行为分析：了解用户退单频率和退单金额特征
 *   - 商品质量评估：通过退单数据分析商品可能存在的质量问题
 *   - 用户满意度监控：退单是用户不满意的重要指标之一
 *   - 售后服务评估：评估售后服务的效率和质量
 *   - 经营风险识别：通过退单数据识别潜在的经营风险和问题
 */
-- DROP TABLE IF EXISTS dws.dws_trade_user_order_refund_1d;
CREATE TABLE dws.dws_trade_user_order_refund_1d
(
    /* 维度字段 */
    `user_id`                   VARCHAR(255) COMMENT '用户ID - 标识用户的唯一标识',
    `k1`                        DATE NOT NULL COMMENT '数据日期 - 分区字段，记录数据所属日期',
    
    /* 度量值字段 - 退单指标 */
    `order_refund_count_1d`     BIGINT COMMENT '最近1日退单次数 - 用户当天退单的总次数',
    `order_refund_num_1d`       BIGINT COMMENT '最近1日退单商品件数 - 用户当天退回的商品总数量',
    `order_refund_amount_1d`    DECIMAL(16, 2) COMMENT '最近1日退单金额 - 用户当天退单的总金额'
)
ENGINE=OLAP
UNIQUE KEY(`user_id`,`k1`)
COMMENT '交易域用户粒度退单最近1日汇总事实表'
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
 *    - 退单量指标：记录用户每日退单次数和退回商品件数，反映退单规模
 *    - 退单金额指标：记录退单涉及的金额，评估退单对业务的财务影响
 *    - 时间范围：聚焦于最近1日的退单行为，适用于日常监控和短期分析
 * 
 * 3. 存储管理：
 *    - 动态分区：自动管理历史数据，保留最近60天数据，预创建3天分区
 *    - 存储格式：使用V2存储格式和轻量级模式变更，优化存储效率
 *
 * 4. 典型应用场景：
 *    - 用户行为异常监控：识别短期内高频退单的异常用户
 *    - 售后服务优化：分析退单原因和规律，优化售后服务流程
 *    - 商品质量改进：结合商品维度分析，发现可能存在质量问题的商品
 *    - 用户体验改进：分析退单与商品描述、物流时间等因素的关系
 *
 * 5. 优化建议：
 *    - 扩展为n日汇总表(7日、30日)，支持中长期退单行为分析
 *    - 增加退单原因维度，分析不同原因的退单占比和趋势
 *    - 添加退单率指标，评估退单相对于订单的比例
 *    - 考虑与订单表关联，构建完整的订单-退单分析链路
 *    - 引入商品和类目维度，分析不同商品和类目的退单特征
 */
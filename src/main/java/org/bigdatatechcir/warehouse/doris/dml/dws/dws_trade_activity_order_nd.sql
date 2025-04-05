/*
 * 表名: dws_trade_activity_order_nd
 * 说明: 交易域活动粒度订单最近n日汇总事实表
 * 数据粒度: 活动 + 日期
 * 存储策略: 分区存储，按日期分区
 * 刷新周期: 每日刷新
 * 应用场景:
 *   - 活动效果分析：评估不同营销活动的销售表现
 *   - 活动优惠分析：计算活动带来的优惠金额和转化率
 *   - 活动类型对比：对比不同类型活动的效果和ROI
 *   - 活动趋势监控：监控活动效果随时间的变化
 */

-- DROP TABLE IF EXISTS dws.dws_trade_activity_order_nd;
CREATE  TABLE dws.dws_trade_activity_order_nd
(
    /* 维度字段 */
    `activity_id`                VARCHAR(255) COMMENT '活动ID - 唯一标识营销活动',
    `k1`                        DATE NOT NULL   COMMENT '分区字段 - 数据日期，用于分区管理',
    
    /* 冗余维度 - 用于提高分析效率，避免关联查询 */
    `activity_name`              STRING COMMENT '活动名称 - 活动的业务描述名称',
    `activity_type_code`         STRING COMMENT '活动类型编码 - 例如满减、折扣、秒杀等类型的编码',
    `activity_type_name`         STRING COMMENT '活动类型名称 - 活动类型的文字描述',
    `start_date`                 STRING COMMENT '发布日期 - 活动开始执行的日期',
    
    /* 度量值字段 - 30日汇总 */
    `original_amount_30d`        DECIMAL(16, 2) COMMENT '参与活动订单原始金额 - 30天内参与该活动订单的原始总金额',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '参与活动订单优惠金额 - 30天内该活动提供的优惠总金额'
)
    ENGINE=OLAP
UNIQUE KEY(`activity_id`,`k1`)
COMMENT '交易域活动粒度订单最近n日汇总事实表'
PARTITION BY RANGE(`k1`) ()
DISTRIBUTED BY HASH(`activity_id`)
PROPERTIES
(
    "replication_allocation" = "tag.location.default: 1",
    "is_being_synced" = "false",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false",
    
    /* 动态分区配置 - 自动管理历史数据 */
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-60",    /* 保留60天历史数据 */
    "dynamic_partition.end" = "3",        /* 预创建未来3天分区 */
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "32",
    "dynamic_partition.create_history_partition" = "true"
);

/*
 * 表设计说明：
 * 
 * 1. 主键设计：
 *    - 使用活动ID + 日期作为联合主键，确保每个活动每天只有一条汇总记录
 *    - 这种设计支持按天查询特定活动的表现，也支持按天比较不同活动
 *
 * 2. 分区和分桶策略：
 *    - 按日期分区，支持高效的历史数据管理和按时间范围的查询
 *    - 使用活动ID作为分桶键，优化并行查询性能
 *
 * 3. 字段选择考量：
 *    - 仅包含最核心的维度和指标，保持表结构简洁
 *    - 包含足够的冗余维度，减少分析时的表关联需求
 *    - 当前仅保留30天汇总数据，可根据需要扩展1天、7天等周期
 *
 * 4. 维护建议：
 *    - 定期检查分区状态，确保动态分区正常运行
 *    - 监控表大小增长，评估是否需要调整历史数据保留周期
 *    - 根据查询模式变化，考虑调整分桶数量或分桶键
 */
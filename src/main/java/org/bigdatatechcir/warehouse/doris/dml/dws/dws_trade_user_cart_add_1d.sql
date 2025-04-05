/*
 * 表名: dws_trade_user_cart_add_1d
 * 说明: 交易域用户粒度加购最近1日汇总表
 * 数据粒度: 用户 + 日期
 * 存储策略: 分区存储，按日期分区
 * 刷新周期: 每日刷新
 * 应用场景:
 *   - 用户购物行为分析：了解用户加购但未购买的行为特征
 *   - 商品兴趣度分析：识别高加购但低转化的商品
 *   - 用户活跃度评估：加购行为是用户活跃的重要指标
 *   - 营销活动效果评估：评估活动期间加购行为的变化
 *   - 数据仓库指标构建：为用户行为分析提供基础数据
 */
-- DROP TABLE IF EXISTS dws.dws_trade_user_cart_add_1d;
CREATE TABLE dws.dws_trade_user_cart_add_1d
(
    /* 维度字段 */
    `user_id`                   VARCHAR(255) COMMENT '用户ID - 标识用户的唯一标识',
    `k1`                        DATE NOT NULL COMMENT '数据日期 - 分区字段，记录数据所属日期',
    
    /* 度量值字段 */
    `cart_add_count_1d`         BIGINT COMMENT '最近1日加购次数 - 用户当天加购行为的发生次数',
    `cart_add_num_1d`           BIGINT COMMENT '最近1日加购商品件数 - 用户当天加购的商品总数量'
)
ENGINE=OLAP
UNIQUE KEY(`user_id`,`k1`)
COMMENT '交易域用户粒度加购最近1日汇总事实表'
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
 *    - 主键：使用user_id + k1复合主键，支持用户在不同日期的加购行为分析
 *    - 分桶：按用户ID分桶(32桶)，优化用户级查询性能
 *    - 分区：使用日期（k1）分区，支持历史数据生命周期管理，提升查询效率
 *
 * 2. 数据组织：
 *    - 行为指标：记录用户每日加购行为次数和加购商品件数两个基础指标
 *    - 时间范围：聚焦于最近1日的加购行为，适用于日常监控和用户行为分析
 * 
 * 3. 存储管理：
 *    - 动态分区：自动管理历史数据，保留最近60天数据，预创建3天分区
 *    - 存储格式：使用V2存储格式和轻量级模式变更，优化存储效率
 *
 * 4. 典型应用场景：
 *    - 用户行为分析：结合浏览、收藏等行为，构建用户购物决策路径
 *    - 用户活跃度评估：加购是比浏览更强的购买意向信号
 *    - 营销活动效果分析：评估活动对用户加购行为的影响
 *    - 转化率优化：分析加购但未购买的用户，进行针对性营销
 *
 * 5. 优化建议：
 *    - 考虑扩展为n日汇总表(7日、30日)，支持更长周期的趋势分析
 *    - 可增加SKU种类数和加购金额指标，提供更丰富的行为描述
 *    - 添加加购转化率指标，关联订单数据评估加购到下单的效率
 */
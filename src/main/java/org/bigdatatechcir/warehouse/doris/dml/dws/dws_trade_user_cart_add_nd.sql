/*
 * 表名: dws_trade_user_cart_add_nd
 * 说明: 交易域用户粒度加购最近n日汇总表
 * 数据粒度: 用户 + 日期
 * 存储策略: 分区存储，按日期分区
 * 刷新周期: 每日刷新
 * 应用场景:
 *   - 用户中长期购物行为分析：了解用户在较长时间范围内的加购行为趋势
 *   - 用户价值评估：通过较长周期的加购行为评估用户活跃度和忠诚度
 *   - 商品流行度分析：识别在较长周期内持续受关注的商品
 *   - 季节性购物行为研究：分析用户加购行为的周期性变化
 *   - 用户生命周期管理：评估用户在不同生命周期阶段的购物意向变化
 */
-- DROP TABLE IF EXISTS dws.dws_trade_user_cart_add_nd;
CREATE TABLE dws.dws_trade_user_cart_add_nd
(
    /* 维度字段 */
    `user_id`                   VARCHAR(255) COMMENT '用户ID - 标识用户的唯一标识',
    `k1`                        DATE NOT NULL COMMENT '数据日期 - 分区字段，记录数据所属日期',
    
    /* 度量值字段 - 7日汇总 */
    `cart_add_count_7d`         BIGINT COMMENT '最近7日加购次数 - 用户近7天加购行为的发生次数',
    `cart_add_num_7d`           BIGINT COMMENT '最近7日加购商品件数 - 用户近7天加购的商品总数量',
    
    /* 度量值字段 - 30日汇总 */
    `cart_add_count_30d`        BIGINT COMMENT '最近30日加购次数 - 用户近30天加购行为的发生次数',
    `cart_add_num_30d`          BIGINT COMMENT '最近30日加购商品件数 - 用户近30天加购的商品总数量'
)
ENGINE=OLAP
UNIQUE KEY(`user_id`,`k1`)
COMMENT '交易域用户粒度加购最近n日汇总事实表'
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
 *    - 行为指标：记录用户在7日和30日时间窗口内的加购行为次数和加购商品件数
 *    - 时间范围：覆盖中期(7日)和长期(30日)的用户行为，适用于趋势分析和用户画像
 * 
 * 3. 存储管理：
 *    - 动态分区：自动管理历史数据，保留最近60天数据，预创建3天分区
 *    - 存储格式：使用V2存储格式和轻量级模式变更，优化存储效率
 *
 * 4. 典型应用场景：
 *    - 用户行为趋势分析：评估用户加购行为的变化趋势和稳定性
 *    - 用户分群：基于中长期加购行为将用户分为高、中、低活跃度群体
 *    - 商品持续关注度衡量：分析商品在较长周期内的持续关注情况
 *    - RFM模型构建：作为Frequency指标的重要数据源
 *
 * 5. 优化建议：
 *    - 增加SKU种类数指标，评估用户兴趣的广度和多样性
 *    - 添加加购金额指标，量化用户加购行为的价值
 *    - 引入加购转化率，评估用户加购到下单的效率
 *    - 考虑加入品类维度的加购占比，分析用户兴趣偏好
 *    - 与1日汇总表整合设计，形成完整的短、中、长期分析体系
 */
/*
 * 表名: dws_trade_user_payment_td
 * 说明: 交易域用户粒度支付历史至今汇总表
 * 数据粒度: 用户 + 日期
 * 存储策略: 分区存储，按日期分区
 * 刷新周期: 每日刷新
 * 应用场景:
 *   - 用户生命周期价值(LTV)分析：评估用户从首次支付至今的贡献总值
 *   - 用户支付习惯分析：通过长期数据了解用户的支付偏好和消费能力
 *   - 用户价值分层：基于历史累计支付进行用户分层和分级
 *   - 忠诚用户识别：通过支付频率和金额识别高价值忠诚用户
 *   - 用户活跃度评估：结合首末次支付时间分析用户的活跃状态
 */
DROP TABLE IF EXISTS dws.dws_trade_user_payment_td;
CREATE TABLE dws.dws_trade_user_payment_td
(
    /* 维度字段 */
    `user_id`                   VARCHAR(255) COMMENT '用户ID - 标识用户的唯一标识',
    `k1`                        DATE NOT NULL COMMENT '数据日期 - 分区字段，记录数据所属日期',
    
    /* 时间指标字段 */
    `payment_date_first`        STRING COMMENT '首次支付日期 - 用户第一次支付的日期，用于分析用户获取时间',
    `payment_date_last`         STRING COMMENT '末次支付日期 - 用户最近一次支付的日期，用于分析用户活跃状态',
    
    /* 度量值字段 - 累计支付指标 */
    `payment_count_td`          BIGINT COMMENT '累计支付次数 - 用户历史至今的支付总次数',
    `payment_num_td`            BIGINT COMMENT '累计支付商品件数 - 用户历史至今支付购买的商品总数量',
    `payment_amount_td`         DECIMAL(16, 2) COMMENT '累计支付金额 - 用户历史至今支付的总金额'
)
ENGINE=OLAP
UNIQUE KEY(`user_id`,`k1`)
COMMENT '交易域用户粒度支付历史至今汇总事实表'
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
 *    - 主键：使用user_id + k1复合主键，支持用户在不同日期的累计支付分析
 *    - 分桶：按用户ID分桶(32桶)，优化用户级查询性能
 *    - 分区：使用日期（k1）分区，支持历史数据生命周期管理，提升查询效率
 *
 * 2. 数据组织：
 *    - 时间指标：记录用户首次和末次支付时间，便于分析用户生命周期
 *    - 支付量指标：包含累计支付次数和商品件数，反映用户支付规模
 *    - 金额指标：记录用户历史累计支付金额，全面评估用户价值
 * 
 * 3. 存储管理：
 *    - 动态分区：自动管理历史数据，保留最近60天数据，预创建3天分区
 *    - 存储格式：使用V2存储格式和轻量级模式变更，优化存储效率
 *
 * 4. 典型应用场景：
 *    - RFM模型构建：提供用户支付总频次(F)和总金额(M)，可结合最近支付时间(R)构建完整RFM模型
 *    - 用户生命周期分析：结合首次和末次支付时间，评估用户的留存周期和活跃状态
 *    - 用户价值分层：基于累计支付金额和频次，对用户进行价值分层
 *    - 会员等级评估：为会员等级设计提供数据支持，可基于累计消费设计会员体系
 *    - 支付行为趋势分析：与订单历史至今表对比，分析支付转化率的长期趋势
 *
 * 5. 优化建议：
 *    - 添加用户留存期指标：计算末次支付与首次支付的时间间隔
 *    - 增加客单价指标：计算用户的历史平均支付金额，评估消费能力
 *    - 添加支付频率指标：计算用户的平均支付间隔，评估支付习惯
 *    - 引入活跃度指标：根据末次支付时间与当前日期的间隔，评估用户活跃度
 *    - 增加支付转化率：结合订单历史至今表，计算用户历史支付转化率
 */
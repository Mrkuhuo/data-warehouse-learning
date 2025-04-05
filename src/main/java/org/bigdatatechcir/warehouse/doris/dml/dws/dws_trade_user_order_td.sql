/*
 * 表名: dws_trade_user_order_td
 * 说明: 交易域用户粒度订单历史至今汇总表
 * 数据粒度: 用户 + 日期
 * 存储策略: 分区存储，按日期分区
 * 刷新周期: 每日刷新
 * 应用场景:
 *   - 用户生命周期价值(LTV)分析：评估用户从首次下单至今的贡献总值
 *   - 用户消费习惯分析：通过长期数据了解用户的购买偏好和消费模式
 *   - 用户价值分层：基于历史累计消费进行用户分层和分级
 *   - 忠诚用户识别：通过购买频率和金额识别高价值忠诚用户
 *   - 用户活跃度评估：结合首末次购买时间分析用户的活跃状态和生命周期阶段
 */
-- DROP TABLE IF EXISTS dws.dws_trade_user_order_td;
CREATE TABLE dws.dws_trade_user_order_td
(
    /* 维度字段 */
    `user_id`                    VARCHAR(255) COMMENT '用户ID - 标识用户的唯一标识',
    `k1`                         DATE NOT NULL COMMENT '数据日期 - 分区字段，记录数据所属日期',
    
    /* 时间指标字段 */
    `order_date_first`           STRING COMMENT '首次下单日期 - 用户第一次下单的日期，用于分析用户获取时间',
    `order_date_last`            STRING COMMENT '末次下单日期 - 用户最近一次下单的日期，用于分析用户活跃状态',
    
    /* 度量值字段 - 累计订单指标 */
    `order_count_td`             BIGINT COMMENT '累计下单次数 - 用户历史至今的订单总数',
    `order_num_td`               BIGINT COMMENT '累计购买商品件数 - 用户历史至今购买的商品总数量',
    `original_amount_td`         DECIMAL(16, 2) COMMENT '累计下单原始金额 - 用户历史至今下单的原始总金额（优惠前）',
    `activity_reduce_amount_td`  DECIMAL(16, 2) COMMENT '累计活动优惠金额 - 用户历史至今获得的活动优惠总额',
    `coupon_reduce_amount_td`    DECIMAL(16, 2) COMMENT '累计优惠券优惠金额 - 用户历史至今使用优惠券的优惠总额',
    `total_amount_td`            DECIMAL(16, 2) COMMENT '累计下单最终金额 - 用户历史至今实际支付的总金额（优惠后）'
)
ENGINE=OLAP
UNIQUE KEY(`user_id`,`k1`)
COMMENT '交易域用户粒度订单历史至今汇总事实表'
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
 *    - 主键：使用user_id + k1复合主键，支持用户在不同日期的累计消费分析
 *    - 分桶：按用户ID分桶(32桶)，优化用户级查询性能
 *    - 分区：使用日期（k1）分区，支持历史数据生命周期管理，提升查询效率
 *
 * 2. 数据组织：
 *    - 时间指标：记录用户首次和末次下单时间，便于分析用户生命周期
 *    - 订单量指标：包含累计订单次数和商品件数，反映用户购买规模
 *    - 金额指标：包含原始金额、优惠金额和最终金额，全面评估用户价值
 * 
 * 3. 存储管理：
 *    - 动态分区：自动管理历史数据，保留最近60天数据，预创建3天分区
 *    - 存储格式：使用V2存储格式和轻量级模式变更，优化存储效率
 *
 * 4. 典型应用场景：
 *    - RFM模型构建：提供用户消费总频次(F)和总金额(M)，可结合最近购买时间(R)构建完整RFM模型
 *    - 用户生命周期分析：结合首次和末次购买时间，评估用户的留存周期和活跃状态
 *    - 用户价值分层：基于累计购买金额和频次，对用户进行价值分层
 *    - 会员等级评估：为会员等级设计提供数据支持，可基于累计消费设计会员体系
 *
 * 5. 优化建议：
 *    - 添加用户留存期指标：计算末次购买与首次购买的时间间隔
 *    - 增加客单价指标：计算用户的历史平均客单价，评估消费能力
 *    - 添加购买频率指标：计算用户的平均购买间隔，评估购买习惯
 *    - 引入活跃度指标：根据末次购买时间与当前日期的间隔，评估用户活跃度
 *    - 增加季节性分析：识别用户是否有季节性消费习惯
 */
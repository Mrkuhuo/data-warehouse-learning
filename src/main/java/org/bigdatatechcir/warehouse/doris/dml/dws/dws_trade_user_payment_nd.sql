/*
 * 表名: dws_trade_user_payment_nd
 * 说明: 交易域用户粒度支付最近n日汇总表
 * 数据粒度: 用户 + 日期
 * 存储策略: 分区存储，按日期分区
 * 刷新周期: 每日刷新
 * 应用场景:
 *   - 用户中长期支付行为分析：了解用户在更长时间范围内的支付习惯
 *   - 支付稳定性评估：分析用户支付频率和金额的稳定性
 *   - 用户价值评估：结合支付金额和频次，评估用户的中长期价值
 *   - 支付趋势分析：识别用户支付行为的变化趋势
 *   - 用户分层分群：基于支付行为特征进行用户分群，制定差异化营销策略
 */
-- DROP TABLE IF EXISTS dws.dws_trade_user_payment_nd;
CREATE TABLE dws.dws_trade_user_payment_nd
(
    /* 维度字段 */
    `user_id`                   VARCHAR(255) COMMENT '用户ID - 标识用户的唯一标识',
    `k1`                        DATE NOT NULL COMMENT '数据日期 - 分区字段，记录数据所属日期',
    
    /* 度量值字段 - 7日汇总指标 */
    `payment_count_7d`          BIGINT COMMENT '最近7日支付次数 - 用户近7天完成支付的总次数',
    `payment_num_7d`            BIGINT COMMENT '最近7日支付商品件数 - 用户近7天支付购买的商品总数量',
    `payment_amount_7d`         DECIMAL(16, 2) COMMENT '最近7日支付金额 - 用户近7天支付的总金额',
    
    /* 度量值字段 - 30日汇总指标 */
    `payment_count_30d`         BIGINT COMMENT '最近30日支付次数 - 用户近30天完成支付的总次数',
    `payment_num_30d`           BIGINT COMMENT '最近30日支付商品件数 - 用户近30天支付购买的商品总数量',
    `payment_amount_30d`        DECIMAL(16, 2) COMMENT '最近30日支付金额 - 用户近30天支付的总金额'
)
ENGINE=OLAP
UNIQUE KEY(`user_id`,`k1`)
COMMENT '交易域用户粒度支付最近n日汇总事实表'
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
 *    - 时间维度：分别提供7日和30日两个时间窗口的汇总指标，覆盖中期和长期分析
 *    - 支付量指标：记录用户在不同时间范围内的支付次数和支付商品件数
 *    - 支付金额指标：记录不同时间范围内的支付金额，评估用户消费能力
 * 
 * 3. 存储管理：
 *    - 动态分区：自动管理历史数据，保留最近60天数据，预创建3天分区
 *    - 存储格式：使用V2存储格式和轻量级模式变更，优化存储效率
 *
 * 4. 典型应用场景：
 *    - RFM模型构建：提供用户支付频率(F)和金额(M)的中长期指标
 *    - 用户价值分层：基于中长期支付行为对用户进行价值分层
 *    - 支付习惯分析：评估用户支付行为的稳定性和持续性
 *    - 用户生命周期管理：结合订单数据分析用户在不同生命周期阶段的支付特征
 *    - 支付转化率趋势：分析中长期内支付转化率的变化趋势
 *
 * 5. 优化建议：
 *    - 增加支付频率指标：如平均支付间隔天数，评估用户的支付频率稳定性
 *    - 添加客单价指标：计算用户的平均支付单价，评估消费能力
 *    - 引入订单支付转化指标：结合订单数据，计算下单到支付的转化率
 *    - 增加同比环比指标：与去年同期、上月同期比较，评估趋势变化
 *    - 结合1日表和订单表数据：构建完整的用户行为分析体系
 */
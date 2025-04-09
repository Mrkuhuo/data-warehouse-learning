-- ===============================================================================
-- 用户价值分层分析报表(ads_user_value_analysis)
-- 功能描述：基于用户交易金额、频次、活跃度等多维度指标进行用户价值评估和分层
-- 数据来源：dws_trade_user_order_td、dws_user_user_login_td等
-- 刷新策略：每日刷新
-- 应用场景：用户分层运营、精准营销、会员价值评估、用户画像构建
-- ===============================================================================

-- DROP TABLE IF EXISTS ads.ads_user_value_analysis;
CREATE TABLE ads.ads_user_value_analysis
(
    `dt`                       VARCHAR(255) COMMENT '统计日期',
    `user_id`                  VARCHAR(255) COMMENT '用户ID',
    `order_count_td`           BIGINT COMMENT '累计下单次数',
    `order_amount_td`          DECIMAL(16, 2) COMMENT '累计下单金额',
    `order_last_date`          VARCHAR(255) COMMENT '最近下单日期',
    `order_first_date`         VARCHAR(255) COMMENT '首次下单日期',
    `login_count_td`           BIGINT COMMENT '累计登录次数',
    `login_last_date`          VARCHAR(255) COMMENT '最近登录日期',
    `average_order_amount`     DECIMAL(16, 2) COMMENT '平均客单价',
    `purchase_cycle_days`      BIGINT COMMENT '平均购买周期(天)',
    `account_days`             BIGINT COMMENT '账号存续天数',
    `life_time_value`          DECIMAL(16, 2) COMMENT '生命周期价值(LTV)',
    `recency_score`            BIGINT COMMENT '最近活跃度评分(R)',
    `frequency_score`          BIGINT COMMENT '活动频次评分(F)',
    `monetary_score`           BIGINT COMMENT '消费金额评分(M)',
    `rfm_score`                BIGINT COMMENT 'RFM总分',
    `user_value_level`         STRING COMMENT '用户价值分层(高价值/中高价值/中价值/低价值/流失风险)',
    `active_status`            STRING COMMENT '活跃状态(活跃/沉默/流失)',
    `life_cycle_status`        STRING COMMENT '生命周期状态(新用户/成长期/成熟期/衰退期/回流)',
    `shopping_preference`      STRING COMMENT '购物偏好(高频低额/低频高额/高频高额/低频低额)',
    `growth_trend`             STRING COMMENT '价值发展趋势(上升/稳定/下降)'
)
ENGINE=OLAP
DUPLICATE KEY(`dt`, `user_id`)
COMMENT '用户价值分层分析报表'
DISTRIBUTED BY HASH(`dt`, `user_id`)
PROPERTIES
(
    "replication_allocation" = "tag.location.default: 1",
    "is_being_synced" = "false",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false"
); 

/*
 * 表设计说明：
 *
 * 1. 表用途与价值：
 *    本表是用户价值评估与分层的核心分析表，综合了用户的交易行为、活跃度、生命周期等多个维度，
 *    构建了全面的用户价值评价体系，可直接支撑用户分层运营、会员体系建设和精准营销活动。
 *
 * 2. 数据构成与计算逻辑：
 *    - 基础行为数据：从交易和活跃维度DWS表中提取用户的基础行为数据
 *    - 衍生指标：通过计算得出平均客单价、购买周期、账号存续等指标
 *    - RFM模型：基于最近购买(Recency)、购买频率(Frequency)、消费金额(Monetary)三个维度评分
 *    - 用户分层：综合RFM评分和其他指标，将用户划分为不同价值层级
 *    - 行为特征：基于用户购买模式，标记用户的活跃状态、生命周期和购物偏好
 *
 * 3. 关键字段说明：
 *    - 交易指标：order_count_td、order_amount_td等反映用户历史交易情况
 *    - 活跃指标：login_count_td、login_last_date等反映用户活跃程度
 *    - 价值指标：average_order_amount、life_time_value等评估用户商业价值
 *    - RFM评分：recency_score、frequency_score、monetary_score构成RFM模型
 *    - 分层标签：user_value_level、active_status等为用户打上业务标签
 *
 * 4. 典型应用场景：
 *    - 用户价值分层：基于用户价值层级，制定差异化的运营策略
 *    - 精准营销：根据用户购物偏好和活跃状态，针对性投放营销内容
 *    - 会员体系设计：基于用户价值评估结果，设计会员等级和权益
 *    - 流失预警与召回：识别流失风险用户，及时进行召回
 *    - 用户生命周期管理：针对不同生命周期阶段的用户，实施相应的运营手段
 *    - 用户增长策略：分析高价值用户的成长路径，复制培养经验
 *
 * 5. 查询示例：
 *    - 查询高价值用户画像：
 *      SELECT life_cycle_status, shopping_preference, COUNT(1) AS user_count
 *      FROM ads.ads_user_value_analysis
 *      WHERE dt = '${yesterday}' AND user_value_level = '高价值'
 *      GROUP BY life_cycle_status, shopping_preference
 *      ORDER BY user_count DESC;
 *    
 *    - 流失风险用户分析：
 *      SELECT account_days, order_count_td, average_order_amount
 *      FROM ads.ads_user_value_analysis
 *      WHERE dt = '${yesterday}' AND active_status = '流失'
 *      ORDER BY life_time_value DESC
 *      LIMIT 100;
 *
 *    - 用户价值分布统计：
 *      SELECT user_value_level, COUNT(1) AS user_count, 
 *             SUM(order_amount_td) AS total_amount
 *      FROM ads.ads_user_value_analysis
 *      WHERE dt = '${yesterday}'
 *      GROUP BY user_value_level
 *      ORDER BY total_amount DESC;
 *
 * 6. 价值模型说明：
 *    - RFM模型：将用户按R、F、M三个维度分别打分(1-5分)，综合得出总分
 *    - 生命周期值(LTV)：预估用户未来在平台的消费总额，计算公式为：平均客单价 * 平均购买频率 * 预期留存时间
 *    - 生命周期状态：基于首次交易时间、最近交易时间和交易频率，判定用户所处的生命周期阶段
 *    - 价值分层阈值：系统根据用户RFM评分、LTV等指标，参考业务规则动态调整分层阈值
 */ 
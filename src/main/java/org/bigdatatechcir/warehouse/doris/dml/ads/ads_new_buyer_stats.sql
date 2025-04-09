-- ===============================================================================
-- 新增交易用户统计报表(ads_new_buyer_stats)
-- 功能描述：统计平台新增下单和支付用户数量
-- 数据来源：dws_trade_user_order_1d、dws_trade_user_payment_1d等
-- 刷新策略：每日刷新
-- 应用场景：用户增长分析、获客效果评估、渠道质量分析
-- ===============================================================================

-- DROP TABLE IF EXISTS ads.ads_new_buyer_stats;
CREATE  TABLE ads.ads_new_buyer_stats
(
    `dt`          VARCHAR(255) COMMENT '统计日期',
    `recent_days`            BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `new_order_user_count`   BIGINT COMMENT '新增下单人数',
    `new_payment_user_count` BIGINT COMMENT '新增支付人数'
)
    ENGINE=OLAP
DUPLICATE KEY(`dt`)
COMMENT '新增交易用户统计'
DISTRIBUTED BY HASH(`dt`)
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
 *    本表是用户增长分析的核心报表，统计平台新增的交易用户数量，包括新增下单用户和新增支付用户，
 *    帮助业务团队监控用户增长趋势，评估获客效果，分析用户购买转化情况。
 *
 * 2. 数据组成与维度：
 *    - 时间维度：统计日期和时间周期(1天/7天/30天)，支持不同时间粒度的增长趋势分析
 *    - 用户行为维度：区分下单行为和支付行为，反映用户从下单到支付的转化情况
 *
 * 3. 关键指标解读：
 *    - 新增下单人数：首次在平台下单的用户数，反映获客能力和用户增长情况
 *    - 新增支付人数：首次在平台完成支付的用户数，反映有效获客和实际转化情况
 *    - 下单-支付转化率：可通过计算 new_payment_user_count/new_order_user_count 得出，反映首次购买用户的支付意愿
 *
 * 4. 典型应用场景：
 *    - 用户增长监控：监控日/周/月新增用户趋势，评估业务增长健康度
 *    - 获客效果评估：结合营销活动时间，分析获客效果和获客成本
 *    - 新用户转化分析：分析新用户从下单到支付的转化率，发现潜在障碍
 *    - 渠道质量评估：结合渠道数据，比较不同渠道带来的新用户质量
 *    - 周期性分析：评估节假日、活动期等特殊时期的新用户获取情况
 *
 * 5. 查询示例：
 *    - 查看最近30天的用户增长趋势：
 *      SELECT dt, new_order_user_count, new_payment_user_count,
 *             new_payment_user_count/new_order_user_count AS conversion_rate
 *      FROM ads.ads_new_buyer_stats
 *      WHERE recent_days = 1
 *      AND dt BETWEEN DATE_SUB('${yesterday}', 29) AND '${yesterday}'
 *      ORDER BY dt;
 *    
 *    - 比较不同时间周期的增长情况：
 *      SELECT recent_days, 
 *             new_order_user_count, 
 *             new_payment_user_count
 *      FROM ads.ads_new_buyer_stats
 *      WHERE dt = '${yesterday}'
 *      ORDER BY recent_days;
 *
 * 6. 建议扩展：
 *    - 维度扩展：添加渠道维度、设备类型维度、地域维度等，支持多维度交叉分析
 *    - 指标扩展：添加新用户首单金额、客单价、复购率等质量指标，评估新用户价值
 *    - 对比指标：添加环比、同比增长率，更直观地反映增长趋势
 *    - 成本指标：添加获客成本(CAC)指标，评估获客效率
 *    - 留存指标：添加新用户次日/7日/30日留存率，评估用户质量
 */
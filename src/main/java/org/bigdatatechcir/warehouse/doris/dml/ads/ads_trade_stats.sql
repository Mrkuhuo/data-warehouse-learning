-- ===============================================================================
-- 交易统计报表(ads_trade_stats)
-- 功能描述：统计整体交易量、订单量、用户数和退单情况
-- 数据来源：dws_trade_user_order_nd、dws_trade_user_order_refund_nd
-- 刷新策略：每日刷新
-- 应用场景：整体交易监控、业务趋势分析、GMV达成分析
-- ===============================================================================

-- DROP TABLE IF EXISTS ads.ads_trade_stats;
CREATE  TABLE ads.ads_trade_stats
(
    `dt`          VARCHAR(255) COMMENT '统计日期',
    `recent_days`             BIGINT COMMENT '最近天数,1:最近1日,7:最近7天,30:最近30天',
    `order_total_amount`      DECIMAL(16, 2) COMMENT '订单总额,GMV',
    `order_count`             BIGINT COMMENT '订单数',
    `order_user_count`        BIGINT COMMENT '下单人数',
    `order_refund_count`      BIGINT COMMENT '退单数',
    `order_refund_user_count` BIGINT COMMENT '退单人数'
)
    ENGINE=OLAP
DUPLICATE KEY(`dt`)
COMMENT '交易统计'
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
 *    本表是整体交易分析的核心报表，统计平台整体的GMV、订单数、用户数、退单情况等核心指标，
 *    是最高层级的交易指标汇总，用于监控业务整体健康度，支持高层决策和业绩跟踪。
 *
 * 2. 数据组成与维度：
 *    - 时间维度：统计日期和时间周期(1天/7天/30天)，支持不同时间粒度的整体分析
 *    - 交易指标：包含GMV、订单数等正向指标和退单相关的负向指标，全面反映交易情况
 *
 * 3. 关键指标解读：
 *    - 订单总额(GMV)：平台总交易规模，是最核心的业务体量指标
 *    - 订单数：平台总订单量，反映交易频次
 *    - 下单人数：产生交易的用户数量，反映活跃购买用户规模
 *    - 退单数与退单人数：反映交易的质量问题规模，是需要控制的负向指标
 *    - 客单价：可通过计算 order_total_amount/order_count 得出，反映单笔订单价值
 *    - 人均订单数：可通过计算 order_count/order_user_count 得出，反映用户购买频次
 *    - 退单率：可通过计算 order_refund_count/order_count 得出，反映整体订单质量
 *
 * 4. 典型应用场景：
 *    - 业务监控与报告：每日跟踪核心业务指标，监控业务健康度
 *    - GMV达成分析：跟踪GMV目标完成情况，预测月度/季度/年度业绩达成
 *    - 交易趋势分析：分析交易指标的环比和同比变化，把握业务发展趋势
 *    - 用户购买力分析：通过客单价和人均订单数，评估用户购买力变化
 *    - 质量问题监控：通过退单率变化，监控平台整体质量问题
 *    - 活动效果评估：通过活动期与非活动期对比，评估营销活动整体拉动效果
 *
 * 5. 查询示例：
 *    - 计算不同周期的关键指标：
 *      SELECT recent_days, 
 *             order_total_amount AS gmv, 
 *             order_count,
 *             order_user_count,
 *             order_total_amount/order_count AS avg_order_amount
 *      FROM ads.ads_trade_stats
 *      WHERE dt = '${yesterday}'
 *      ORDER BY recent_days;
 *    
 *    - 分析近30天日均指标：
 *      SELECT order_total_amount/30 AS daily_gmv,
 *             order_count/30 AS daily_order_count,
 *             order_user_count/30 AS daily_user_count
 *      FROM ads.ads_trade_stats
 *      WHERE dt = '${yesterday}' AND recent_days = 30;
 *
 *    - 计算最近30天的整体退单率：
 *      SELECT order_refund_count/order_count AS refund_rate,
 *             order_refund_user_count/order_user_count AS refund_user_rate
 *      FROM ads.ads_trade_stats
 *      WHERE dt = '${yesterday}' AND recent_days = 30;
 *
 * 6. 建议扩展：
 *    - 添加同环比指标：增加环比和同比增长率，直观反映业务变化趋势
 *    - 添加支付维度：区分下单和支付的GMV、订单数，分析支付转化
 *    - 添加新老用户维度：区分新用户和老用户贡献，评估获客和留存效果
 *    - 添加渠道维度：增加各渠道GMV分布，评估渠道价值
 *    - 添加预测指标：基于历史数据预测未来一段时间的GMV和订单量
 */
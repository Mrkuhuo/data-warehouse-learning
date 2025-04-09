-- ===============================================================================
-- 各品牌复购率统计报表(ads_repeat_purchase_by_tm)
-- 功能描述：统计各品牌的用户复购率
-- 数据来源：dws_trade_user_sku_order_nd、dim_sku_full
-- 刷新策略：每日刷新
-- 应用场景：品牌忠诚度分析、商品复购分析、用户留存评估
-- ===============================================================================

-- DROP TABLE IF EXISTS ads.ads_repeat_purchase_by_tm;
CREATE  TABLE ads.ads_repeat_purchase_by_tm
(
    `dt`          VARCHAR(255) COMMENT '统计日期',
    `recent_days`       BIGINT COMMENT '最近天数,7:最近7天,30:最近30天',
    `tm_id`             STRING COMMENT '品牌ID',
    `tm_name`           STRING COMMENT '品牌名称',
    `order_repeat_rate` DECIMAL(16, 2) COMMENT '复购率'
)
    ENGINE=OLAP
DUPLICATE KEY(`dt`)
COMMENT '各品牌复购率统计'
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
 *    本表是品牌忠诚度分析的核心报表，统计各品牌的用户复购率，
 *    帮助业务团队了解品牌的用户粘性和忠诚度，为品牌战略和产品策略提供数据支持。
 *
 * 2. 数据组成与维度：
 *    - 时间维度：统计日期和时间周期(7天/30天)，支持不同时间范围的复购分析
 *    - 品牌维度：包含品牌ID和名称，便于品牌间的比较分析
 *    - 指标维度：复购率指标，衡量品牌忠诚度的核心指标
 *
 * 3. 关键指标解读：
 *    - 复购率：计算公式为再次购买该品牌的用户数/购买过该品牌的总用户数，反映品牌的用户留存能力
 *    - 复购率高的品牌通常具有较强的用户忠诚度和产品竞争力
 *    - 不同类型商品的复购率基准不同，应结合品类特性进行评估
 *
 * 4. 典型应用场景：
 *    - 品牌忠诚度评估：评估不同品牌的用户粘性，发现用户忠诚度高的优质品牌
 *    - 品牌竞争力分析：比较同品类不同品牌的复购率，评估品牌竞争力差异
 *    - 品牌营销效果评估：结合营销活动时间，分析活动对品牌复购率的提升效果
 *    - 新品牌培育监控：监控新引入品牌的复购率变化，评估品牌发展趋势
 *    - 品牌结构调整：基于复购率表现，优化平台品牌结构，增强高复购品牌的资源投入
 *
 * 5. 查询示例：
 *    - 查询复购率最高的前十个品牌：
 *      SELECT tm_name, order_repeat_rate
 *      FROM ads.ads_repeat_purchase_by_tm
 *      WHERE dt = '${yesterday}' AND recent_days = 30
 *      ORDER BY order_repeat_rate DESC
 *      LIMIT 10;
 *    
 *    - 比较品牌在不同时间周期的复购率差异：
 *      SELECT tm_name, 
 *             MAX(CASE WHEN recent_days = 7 THEN order_repeat_rate END) AS repeat_rate_7d,
 *             MAX(CASE WHEN recent_days = 30 THEN order_repeat_rate END) AS repeat_rate_30d
 *      FROM ads.ads_repeat_purchase_by_tm
 *      WHERE dt = '${yesterday}' AND recent_days IN (7, 30)
 *      GROUP BY tm_name
 *      ORDER BY repeat_rate_30d DESC;
 *
 * 6. 建议扩展：
 *    - 添加品类维度：增加品牌所属品类信息，支持品类内品牌对比
 *    - 添加复购周期：增加平均复购天数指标，了解品牌的复购周期特征
 *    - 添加复购频次：增加平均复购次数指标，深入了解忠诚用户的购买习惯
 *    - 添加用户价值指标：增加复购用户客单价等指标，评估复购价值
 *    - 添加复购趋势：增加环比、同比增长率，监控复购率的变化趋势
 */
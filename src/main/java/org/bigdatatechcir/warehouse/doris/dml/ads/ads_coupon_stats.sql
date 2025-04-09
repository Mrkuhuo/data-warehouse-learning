-- ===============================================================================
-- 优惠券统计报表(ads_coupon_stats)
-- 功能描述：统计各类优惠券的使用效果和转化情况
-- 数据来源：dwd_trade_order_detail、dim_coupon_full等
-- 刷新策略：每日刷新
-- 应用场景：优惠券效果评估、精准营销、营销成本控制
-- ===============================================================================

-- DROP TABLE IF EXISTS ads.ads_coupon_stats;
CREATE  TABLE ads.ads_coupon_stats
(
    `dt`          VARCHAR(255) COMMENT '统计日期',
    `coupon_id`   STRING COMMENT '优惠券ID',
    `coupon_name` STRING COMMENT '优惠券名称',
    `start_date`  STRING COMMENT '发布日期',
    `rule_name`   STRING COMMENT '优惠规则，例如满100元减10元',
    `reduce_rate` DECIMAL(16, 2) COMMENT '补贴率'
)
    ENGINE=OLAP
DUPLICATE KEY(`dt`)
COMMENT '优惠券统计'
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
 *    本表是优惠券分析的核心报表，统计各类优惠券的使用情况、转化效果和成本收益，
 *    帮助营销团队评估不同优惠券策略的效果，优化优惠券发放策略，控制营销成本。
 *
 * 2. 数据组成与维度：
 *    - 优惠券维度：记录优惠券ID、名称、规则等信息，便于识别不同类型的优惠券
 *    - 时间维度：包含统计日期和发布日期，支持优惠券效果的时间序列分析
 *    - 效果维度：通过补贴率等指标，评估优惠券的成本效益
 *
 * 3. 关键指标解读：
 *    - 补贴率：优惠券使用金额/带动销售额，反映优惠券成本效益比，值越低表示投入产出比越高
 *    - 建议扩展指标：可考虑添加领取数、使用数、使用率、使用间隔等指标，全面评估优惠券生命周期效果
 *
 * 4. 典型应用场景：
 *    - 优惠券效果评估：评估不同类型、金额、门槛的优惠券对销售的拉动效果
 *    - 优惠券策略优化：基于历史数据，优化优惠券的发放对象、金额和使用条件
 *    - 成本收益分析：分析优惠券营销的投入产出比，控制营销成本
 *    - 用户行为分析：结合用户画像，分析不同用户群体对优惠券的敏感度和使用模式
 *    - 促销规划：为不同时期、不同品类规划最佳优惠券策略
 *
 * 5. 查询示例：
 *    - 查找最高效的优惠券类型：
 *      SELECT coupon_name, rule_name, reduce_rate
 *      FROM ads.ads_coupon_stats
 *      WHERE dt = '${yesterday}'
 *      ORDER BY reduce_rate ASC
 *      LIMIT 10;
 *    
 *    - 比较不同规则优惠券的效果：
 *      SELECT SUBSTRING_INDEX(rule_name, '减', 1) AS threshold,
 *             AVG(reduce_rate) AS avg_reduce_rate
 *      FROM ads.ads_coupon_stats
 *      WHERE dt = '${yesterday}' AND rule_name LIKE '%减%'
 *      GROUP BY SUBSTRING_INDEX(rule_name, '减', 1)
 *      ORDER BY avg_reduce_rate ASC;
 *
 * 6. 建议扩展：
 *    - 覆盖率指标：添加领取人数、使用人数、覆盖用户数等
 *    - 转化指标：添加领券-使用转化率、使用-复购转化率等
 *    - 时效指标：添加平均使用时间、过期率等衡量时效性的指标
 *    - 用户价值指标：添加优惠券带来的新客数、客单价提升等指标
 *    - 商品关联指标：添加优惠券使用的关联品类、带动品类等信息
 */
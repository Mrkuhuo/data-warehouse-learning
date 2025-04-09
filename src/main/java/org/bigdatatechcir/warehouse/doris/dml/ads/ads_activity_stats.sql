-- ===============================================================================
-- 活动统计报表(ads_activity_stats)
-- 功能描述：统计各营销活动的关键指标和效果评估
-- 数据来源：dwd_trade_order_detail、dim_activity_full等
-- 刷新策略：每日刷新
-- 应用场景：活动效果评估、营销策略优化、活动ROI分析
-- ===============================================================================

-- DROP TABLE IF EXISTS ads.ads_activity_stats;
CREATE  TABLE ads.ads_activity_stats
(
    `dt`          VARCHAR(255) COMMENT '统计日期',
    `activity_id`   STRING COMMENT '活动ID',
    `activity_name` STRING COMMENT '活动名称',
    `start_date`    STRING COMMENT '活动开始日期',
    `reduce_rate`   DECIMAL(16, 2) COMMENT '补贴率'
)
    ENGINE=OLAP
DUPLICATE KEY(`dt`)
COMMENT '活动统计'
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
 *    本表是营销活动分析的核心报表，统计各类促销活动的关键指标和效果，
 *    帮助营销团队评估活动表现，优化营销策略，提高活动ROI。
 *
 * 2. 数据组成与维度：
 *    - 活动维度：记录活动ID、名称等基本信息，便于识别和筛选特定活动
 *    - 时间维度：包含统计日期和活动开始日期，支持活动时序分析
 *    - 效果维度：通过补贴率等指标，评估活动的成本效益
 *
 * 3. 关键指标解读：
 *    - 补贴率：活动优惠金额/带动销售额，反映活动成本效益比，值越低表示投入产出比越高
 *    - 建议扩展指标：可考虑添加参与用户数、转化率、销售额提升等指标，全面评估活动效果
 *
 * 4. 典型应用场景：
 *    - 活动效果评估：评估不同活动类型和策略的销售拉动效果和成本效益
 *    - 活动策略优化：基于历史活动数据，优化未来活动的设计和资源投入
 *    - 活动ROI分析：计算活动投资回报率，指导营销预算分配
 *    - 活动比较分析：比较不同活动的表现，识别最佳活动类型和形式
 *    - 季节性活动规划：结合历史活动效果，规划季节性促销活动安排
 *
 * 5. 查询示例：
 *    - 按补贴率排序查看活动效益：
 *      SELECT activity_name, start_date, reduce_rate
 *      FROM ads.ads_activity_stats
 *      WHERE dt = '${yesterday}'
 *      ORDER BY reduce_rate ASC
 *      LIMIT 10;
 *    
 *    - 比较不同时期活动效果：
 *      SELECT activity_name, start_date, reduce_rate
 *      FROM ads.ads_activity_stats
 *      WHERE dt = '${yesterday}' 
 *      AND start_date BETWEEN '${startDate}' AND '${endDate}'
 *      ORDER BY start_date;
 *
 * 6. 建议扩展：
 *    - 活动参与指标：添加活动参与人数、参与订单数、客单价等指标
 *    - 活动效果指标：添加销售额同比提升、新客拉新数、转化率等指标
 *    - 活动成本指标：添加活动总成本、每客获取成本(CAC)等指标
 *    - 活动ROI指标：添加投资回报率、边际效益等财务评估指标
 */
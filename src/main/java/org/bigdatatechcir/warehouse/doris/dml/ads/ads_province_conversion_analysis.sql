-- ===============================================================================
-- 省份转化率分析报表(ads_province_conversion_analysis)
-- 功能描述：对各省份用户从浏览、加购、下单到支付的转化漏斗进行分析
-- 数据来源：dws_trade_province_order_nd、dws_trade_province_sku_order_nd等
-- 刷新策略：每日刷新
-- 应用场景：区域营销策略优化、地域销售瓶颈分析、区域用户行为差异研究
-- ===============================================================================

-- DROP TABLE IF EXISTS ads.ads_province_conversion_analysis;
CREATE TABLE ads.ads_province_conversion_analysis
(
    `dt`                        VARCHAR(255) COMMENT '统计日期',
    `recent_days`               BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `province_id`               STRING COMMENT '省份ID',
    `province_name`             STRING COMMENT '省份名称',
    `region_id`                 STRING COMMENT '地区ID',
    `region_name`               STRING COMMENT '地区名称',
    `visitor_count`             BIGINT COMMENT '访客数',
    `product_view_count`        BIGINT COMMENT '商品浏览人数',
    `cart_count`                BIGINT COMMENT '加购人数',
    `order_count`               BIGINT COMMENT '下单人数',
    `payment_count`             BIGINT COMMENT '支付人数',
    `view_to_cart_rate`         DECIMAL(10, 2) COMMENT '浏览-加购转化率',
    `cart_to_order_rate`        DECIMAL(10, 2) COMMENT '加购-下单转化率',
    `order_to_payment_rate`     DECIMAL(10, 2) COMMENT '下单-支付转化率',
    `overall_conversion_rate`   DECIMAL(10, 2) COMMENT '整体转化率(访客到支付)',
    `average_order_amount`      DECIMAL(16, 2) COMMENT '平均订单金额',
    `user_penetration_rate`     DECIMAL(10, 2) COMMENT '用户渗透率(相对全国)',
    `gmv_contribution_rate`     DECIMAL(10, 2) COMMENT 'GMV贡献率',
    `wow_change_rate`           DECIMAL(10, 2) COMMENT '周环比变化率',
    `regional_rank`             BIGINT COMMENT '地区内排名'
)
ENGINE=OLAP
DUPLICATE KEY(`dt`)
COMMENT '省份转化率分析报表'
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
 *    本表是区域销售分析的核心报表，从地域维度监控和分析用户从浏览到支付的全链路转化情况，
 *    帮助业务发现区域销售差异和瓶颈，为区域营销策略调整和资源投放优化提供数据支持。
 *
 * 2. 数据维度与粒度：
 *    - 地域维度：省份是主要分析粒度，同时提供地区维度便于区域聚合分析
 *    - 时间维度：支持1天、7天、30天多种时间跨度分析，便于短期波动和中长期趋势研究
 *    - 漏斗维度：完整呈现用户行为漏斗各环节数据，包括访问→浏览→加购→下单→支付
 *
 * 3. 核心指标解读：
 *    - 用户规模指标：包括各环节的用户数量，反映区域用户基础
 *    - 转化率指标：计算漏斗各环节的转化效率，识别区域销售瓶颈
 *    - 贡献度指标：从用户渗透率和GMV贡献率评估区域重要性
 *    - 效率指标：平均订单金额反映区域客单价水平
 *    - 趋势指标：周环比变化率监控区域业绩走势
 *    - 竞争指标：区域内排名反映省份在区域内的竞争地位
 *
 * 4. 典型应用场景：
 *    - 区域营销策略制定：根据不同地区的转化漏斗特点，制定差异化营销策略
 *    - 区域销售瓶颈分析：发现各地区销售流程中的卡点，有针对性地改进
 *    - 区域资源投放优化：基于转化效率和贡献度，优化区域间的资源分配
 *    - 区域市场潜力评估：通过转化率和渗透率，评估区域市场发展潜力
 *    - 特殊区域问题诊断：针对转化异常的区域进行深入分析和问题诊断
 *    - 区域阶段性目标设定：基于历史数据和行业标准，为各区域设定合理的转化目标
 *
 * 5. 查询示例：
 *    - 查找转化率最低的省份：
 *      SELECT province_name, overall_conversion_rate
 *      FROM ads.ads_province_conversion_analysis
 *      WHERE dt = '${yesterday}' AND recent_days = 30
 *      ORDER BY overall_conversion_rate ASC
 *      LIMIT 10;
 *    
 *    - 分析区域内省份排名情况：
 *      SELECT region_name, province_name, order_count, payment_count, 
 *             overall_conversion_rate, regional_rank
 *      FROM ads.ads_province_conversion_analysis
 *      WHERE dt = '${yesterday}' AND recent_days = 7 AND region_id = '${region_id}'
 *      ORDER BY regional_rank ASC;
 *
 *    - 查找高客单价但低转化率的省份：
 *      SELECT province_name, average_order_amount, overall_conversion_rate
 *      FROM ads.ads_province_conversion_analysis
 *      WHERE dt = '${yesterday}' AND recent_days = 30
 *      AND average_order_amount > (SELECT AVG(average_order_amount) FROM ads.ads_province_conversion_analysis WHERE dt = '${yesterday}' AND recent_days = 30)
 *      AND overall_conversion_rate < (SELECT AVG(overall_conversion_rate) FROM ads.ads_province_conversion_analysis WHERE dt = '${yesterday}' AND recent_days = 30)
 *      ORDER BY average_order_amount DESC;
 *
 * 6. 指标计算说明：
 *    - 转化率计算：环节间转化率 = 下一环节人数 / 上一环节人数 * 100%
 *    - 整体转化率：payment_count / visitor_count * 100%
 *    - 用户渗透率：省份用户数 / 全国用户总数 * 100%
 *    - GMV贡献率：省份GMV / 全国总GMV * 100%
 *    - 周环比变化率：(本周指标 - 上周指标) / 上周指标 * 100%
 */ 
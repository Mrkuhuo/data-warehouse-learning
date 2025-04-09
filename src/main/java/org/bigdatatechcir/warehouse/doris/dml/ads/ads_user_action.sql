-- ===============================================================================
-- 用户行为漏斗分析报表(ads_user_action)
-- 功能描述：统计各环节的用户数量，分析从浏览到购买的转化漏斗
-- 数据来源：dws_traffic_page_visitor_page_view_nd、dws_trade_user_cart_add_nd等
-- 刷新策略：每日刷新
-- 应用场景：用户行为分析、转化率优化、流量质量评估
-- ===============================================================================

-- DROP TABLE IF EXISTS ads.ads_user_action;
CREATE  TABLE ads.ads_user_action
(
    `dt`               VARCHAR(255) COMMENT '统计日期',
    `recent_days`       BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `home_count`        BIGINT COMMENT '浏览首页人数',
    `good_detail_count` BIGINT COMMENT '浏览商品详情页人数',
    `cart_count`        BIGINT COMMENT '加入购物车人数',
    `order_count`       BIGINT COMMENT '下单人数',
    `payment_count`     BIGINT COMMENT '支付人数'
)
    ENGINE=OLAP
DUPLICATE KEY(`dt`)
COMMENT '漏斗分析'
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
 *    本表是用户行为漏斗分析的核心报表，统计用户从浏览首页到最终支付的各环节人数，
 *    帮助产品和运营团队发现用户流失节点，优化用户体验和转化流程，提升整体转化率。
 *
 * 2. 数据组成与维度：
 *    - 时间维度：统计日期和时间周期(1天/7天/30天)，支持不同时间粒度的转化分析
 *    - 行为环节：从浏览首页、查看详情、加购、下单到支付的完整购买路径
 *    - 人数指标：每个环节的用户数量，用于计算环节间的转化率
 *
 * 3. 关键指标解读：
 *    - 各环节人数：反映各环节的用户规模，是计算转化率的基础
 *    - 环节转化率：可计算相邻环节间的转化率，如详情页到加购转化率 = cart_count/good_detail_count
 *    - 总体转化率：可计算从首页到支付的整体转化率 = payment_count/home_count
 *    - 环节流失率：1 - 环节转化率，反映每个环节的用户流失情况
 *
 * 4. 典型应用场景：
 *    - 转化漏斗分析：分析各环节转化率，发现最薄弱环节，针对性优化
 *    - 用户行为模式：了解用户在购买路径上的行为特征和决策点
 *    - A/B测试评估：对比不同版本的转化漏斗表现，评估优化效果
 *    - 营销活动影响：分析活动期间各环节转化变化，评估活动对转化的影响
 *    - 长期转化趋势：监控转化率的长期变化趋势，评估产品迭代和运营策略效果
 *    - 跨渠道对比：结合渠道数据，对比不同渠道用户的转化路径差异
 *
 * 5. 查询示例：
 *    - 计算各环节转化率：
 *      SELECT recent_days,
 *             good_detail_count/home_count AS home_to_detail_rate,
 *             cart_count/good_detail_count AS detail_to_cart_rate,
 *             order_count/cart_count AS cart_to_order_rate,
 *             payment_count/order_count AS order_to_payment_rate,
 *             payment_count/home_count AS overall_conversion_rate
 *      FROM ads.ads_user_action
 *      WHERE dt = '${yesterday}'
 *      ORDER BY recent_days;
 *    
 *    - 分析不同时间周期的转化趋势：
 *      SELECT 
 *        dt, 
 *        payment_count/home_count AS conversion_rate
 *      FROM ads.ads_user_action
 *      WHERE recent_days = 1
 *      AND dt BETWEEN DATE_SUB('${yesterday}', 29) AND '${yesterday}'
 *      ORDER BY dt;
 *
 *    - 识别转化漏斗的薄弱环节：
 *      SELECT recent_days,
 *             (home_count - good_detail_count)/home_count AS home_loss_rate,
 *             (good_detail_count - cart_count)/good_detail_count AS detail_loss_rate,
 *             (cart_count - order_count)/cart_count AS cart_loss_rate,
 *             (order_count - payment_count)/order_count AS order_loss_rate
 *      FROM ads.ads_user_action
 *      WHERE dt = '${yesterday}' AND recent_days = 7;
 *
 * 6. 建议扩展：
 *    - 添加渠道维度：增加不同渠道的转化漏斗，对比渠道质量
 *    - 添加用户维度：区分新老用户的转化路径差异
 *    - 添加商品维度：分析不同品类商品的转化特点
 *    - 添加时间指标：记录各环节间的平均时间间隔，分析决策时长
 *    - 添加退出页面：增加各环节的主要退出页面，定位问题页面
 */
-- =================================================================
-- 表名: ads_sku_association_analysis
-- 说明: 商品关联分析报表ETL，分析用户购买行为中的商品关联关系
-- 数据来源: dwd.dwd_trade_order_detail_inc
-- 计算粒度: 商品对(source_sku_id, target_sku_id)
-- 业务应用: 商品推荐、搭配销售策略、商品陈列优化、营销活动设计
-- 更新策略: 每日全量刷新
-- 字段说明:
--   dt: 统计日期
--   recent_days: 最近天数(7/30/90)
--   source_sku_id: 来源商品ID
--   source_sku_name: 来源商品名称
--   source_category1_id: 来源商品一级类目ID
--   source_category1_name: 来源商品一级类目名称
--   target_sku_id: 目标商品ID
--   target_sku_name: 目标商品名称
--   target_category1_id: 目标商品一级类目ID
--   target_category1_name: 目标商品一级类目名称
--   co_purchase_count: 共同购买次数
--   co_purchase_user_count: 共同购买用户数
--   support: 支持度(共同购买次数/总订单数)
--   confidence: 置信度(共同购买次数/来源商品购买次数)
--   lift: 提升度(置信度/目标商品购买概率)
--   score: 推荐分数(综合考虑支持度、置信度和提升度)
-- =================================================================
INSERT INTO ads.ads_sku_association_analysis
(dt, recent_days, source_sku_id, source_sku_name, source_category1_id, source_category1_name,
 target_sku_id, target_sku_name, target_category1_id, target_category1_name,
 co_purchase_count, co_purchase_user_count, support, confidence, lift, recommendation_score)
-- 商品关联分析ETL脚本
WITH base_orders AS (
    SELECT
        k1,
        recent_days,
        user_id,
        order_id,
        sku_id
    FROM dwd.dwd_trade_order_detail_inc
             CROSS JOIN (SELECT 7 as recent_days UNION ALL SELECT 30 UNION ALL SELECT 90) d
    WHERE k1 >= date_sub(date('${pdate}'), 90)
      AND date_format(create_time, 'yyyy-MM-dd') >= date_sub(date('${pdate}'), d.recent_days - 1)
      AND date_format(create_time, 'yyyy-MM-dd') <= date('${pdate}')
),
     sku_pairs AS (
         SELECT
             a.k1,
             a.recent_days,
             a.user_id,
             a.order_id,
             a.sku_id AS source_sku_id,
             b.sku_id AS target_sku_id
         FROM base_orders a
                  JOIN base_orders b
                       ON a.order_id = b.order_id
                           AND a.recent_days = b.recent_days
                           AND a.k1 = b.k1
                           AND a.sku_id < b.sku_id
     ),
     total_orders AS (
         SELECT
             k1,
             recent_days,
             COUNT(DISTINCT order_id) AS order_count
         FROM base_orders
         GROUP BY k1, recent_days
     ),
     sku_purchases AS (
         SELECT
             k1,
             recent_days,
             sku_id,
             COUNT(DISTINCT order_id) AS purchase_count
         FROM base_orders
         GROUP BY k1, recent_days, sku_id
     )


SELECT
    date('${pdate}') AS dt,
    pair.recent_days,
    source.id AS source_sku_id,
    source.sku_name AS source_sku_name,
    source.category1_id AS source_category1_id,
    source.category1_name AS source_category1_name,
    target.id AS target_sku_id,
    target.sku_name AS target_sku_name,
    target.category1_id AS target_category1_id,
    target.category1_name AS target_category1_name,
    COUNT(*) AS co_purchase_count,
    COUNT(DISTINCT pair.user_id) AS co_purchase_user_count,
    CAST(COUNT(*) / total_orders.order_count AS DECIMAL(10, 4)) AS support,
    CAST(COUNT(*) / source_purchases.purchase_count AS DECIMAL(10, 4)) AS confidence,
    CAST((COUNT(*) / source_purchases.purchase_count) /
         (target_purchases.purchase_count / total_orders.order_count) AS DECIMAL(10, 4)) AS lift,
    CAST(
                    (COUNT(*) / total_orders.order_count) * 0.2 +
                    (COUNT(*) / source_purchases.purchase_count) * 0.5 +
                    ((COUNT(*) / source_purchases.purchase_count) /
                     (target_purchases.purchase_count / total_orders.order_count)) * 0.3
        AS DECIMAL(10, 4)) AS recommendation_score
FROM sku_pairs pair
         JOIN dim.dim_sku_full source ON pair.source_sku_id = source.id
         JOIN dim.dim_sku_full target ON pair.target_sku_id = target.id
         JOIN total_orders ON pair.k1 = total_orders.k1 AND pair.recent_days = total_orders.recent_days
         JOIN sku_purchases source_purchases ON pair.k1 = source_purchases.k1
    AND pair.recent_days = source_purchases.recent_days
    AND pair.source_sku_id = source_purchases.sku_id
         JOIN sku_purchases target_purchases ON pair.k1 = target_purchases.k1
    AND pair.recent_days = target_purchases.recent_days
    AND pair.target_sku_id = target_purchases.sku_id
GROUP BY
    pair.recent_days,
    source.id, source.sku_name, source.category1_id, source.category1_name,
    target.id, target.sku_name, target.category1_id, target.category1_name,
    total_orders.order_count, source_purchases.purchase_count, target_purchases.purchase_count
HAVING support >= 0.001
ORDER BY pair.recent_days, recommendation_score DESC; 
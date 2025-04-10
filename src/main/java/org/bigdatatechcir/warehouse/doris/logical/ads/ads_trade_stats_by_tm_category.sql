-- =================================================================
-- 表名: ads_trade_stats_by_tm_category
-- 说明: 品牌品类交易统计报表ETL，分析不同品牌和品类组合的交易情况
-- 数据来源: dwd.dwd_trade_order_detail_inc, dim.dim_sku_info
-- 计算粒度: 品牌(商标)和品类(一级类目)组合
-- 业务应用: 品牌品类结构分析、商品策略制定、类目与品牌关联性分析
-- 更新策略: 每日全量刷新
-- 字段说明:
--   dt: 统计日期
--   recent_days: 最近天数(7/30)
--   tm_id: 品牌(商标)ID
--   tm_name: 品牌(商标)名称
--   category1_id: 一级类目ID
--   category1_name: 一级类目名称
--   order_count: 订单数
--   order_user_count: 下单用户数
--   order_sku_count: 下单商品种类数
--   order_amount: 订单金额
--   category_amount_ratio: 品类金额占比
--   tm_amount_ratio: 品牌金额占比
--   sku_coverage_rate: 品牌SKU覆盖率
--   wow_rate: 周环比变化率
--   yoy_rate: 同比变化率
--   growth_type: 增长类型
-- =================================================================

-- 品牌品类交易统计报表ETL
INSERT INTO ads.ads_trade_stats_by_tm_category
(dt, recent_days, tm_id, tm_name, category1_id, category1_name, 
 order_count, order_user_count, order_sku_count, order_amount, 
 category_amount_ratio, tm_amount_ratio, sku_coverage_rate, wow_rate, yoy_rate, growth_type)
SELECT
    date('${pdate}') AS dt,                     -- 统计日期
    final_stats.recent_days,                    -- 统计周期(7/30天)
    final_stats.tm_id,                          -- 品牌ID
    final_stats.tm_name,                        -- 品牌名称
    final_stats.category1_id,                   -- 一级类目ID
    final_stats.category1_name,                 -- 一级类目名称
    final_stats.order_count,                    -- 订单数
    final_stats.order_user_count,               -- 下单用户数
    final_stats.order_sku_count,                -- 下单商品种类数
    final_stats.order_amount,                   -- 订单金额
    -- 品类金额占比
    CAST(final_stats.order_amount / category_totals.category_total_amount * 100 AS DECIMAL(10, 2)) AS category_amount_ratio,
    -- 品牌金额占比
    CAST(final_stats.order_amount / tm_totals.tm_total_amount * 100 AS DECIMAL(10, 2)) AS tm_amount_ratio,
    -- 品牌SKU覆盖率
    CAST(final_stats.order_sku_count / category_totals.category_sku_count * 100 AS DECIMAL(10, 2)) AS sku_coverage_rate,
    -- 周环比变化率
    CASE 
        WHEN final_stats.recent_days = 7 AND final_stats.prev_7d_amount > 0 
        THEN CAST((final_stats.order_amount - final_stats.prev_7d_amount)/final_stats.prev_7d_amount * 100 AS DECIMAL(10, 2))
        ELSE NULL 
    END AS wow_rate,
    -- 同比变化率
    CASE 
        WHEN final_stats.recent_days = 30 AND final_stats.prev_year_amount > 0 
        THEN CAST((final_stats.order_amount - final_stats.prev_year_amount)/final_stats.prev_year_amount * 100 AS DECIMAL(10, 2))
        ELSE NULL 
    END AS yoy_rate,
    -- 增长类型
    CASE
        WHEN final_stats.recent_days = 7 AND final_stats.prev_7d_amount > 0 THEN
            CASE
                WHEN (final_stats.order_amount - final_stats.prev_7d_amount)/final_stats.prev_7d_amount * 100 >= 20 THEN '高速增长'
                WHEN (final_stats.order_amount - final_stats.prev_7d_amount)/final_stats.prev_7d_amount * 100 >= 0 THEN '平稳增长'
                WHEN (final_stats.order_amount - final_stats.prev_7d_amount)/final_stats.prev_7d_amount * 100 >= -20 THEN '下降'
                ELSE '急剧下降'
            END
        ELSE NULL
    END AS growth_type
FROM
(
    -- 按品牌品类统计交易指标
    SELECT
        stats.recent_days,
        stats.tm_id,
        stats.tm_name,
        stats.category1_id,
        stats.category1_name,
        COUNT(DISTINCT stats.order_id) AS order_count,                -- 订单数(去重)
        COUNT(DISTINCT stats.user_id) AS order_user_count,            -- 下单用户数(去重)
        COUNT(DISTINCT stats.sku_id) AS order_sku_count,              -- 下单商品种类数
        SUM(stats.split_total_amount) AS order_amount,                -- 订单总金额
        -- 上一个7天周期的金额，用于计算环比
        LAG(SUM(stats.split_total_amount), 1, 0) OVER(
            PARTITION BY stats.tm_id, stats.category1_id 
            ORDER BY stats.recent_days
        ) AS prev_7d_amount,
        -- 上一个年度同期的金额，用于计算同比
        LAG(SUM(stats.split_total_amount), 52, 0) OVER(
            PARTITION BY stats.tm_id, stats.category1_id 
            ORDER BY stats.recent_days
        ) AS prev_year_amount
    FROM
    (
        -- 按品牌品类统计
        SELECT
            detail.recent_days,
            detail.user_id,
            detail.order_id,
            detail.sku_id,
            detail.tm_id,
            detail.tm_name,
            detail.category1_id,
            detail.category1_name,
            detail.split_total_amount
        FROM
        (
            -- 基础交易数据
            SELECT
                d.recent_days,
                dwd.user_id,
                dwd.order_id,
                dwd.sku_id,
                dim.tm_id,
                dim.tm_name,
                dim.category1_id,
                dim.category1_name,
                dwd.split_total_amount
            FROM 
            (
                -- 获取近7天和30天的订单明细数据
                SELECT
                    k1,
                    user_id,
                    order_id,
                    sku_id,
                    split_total_amount,
                    create_time
                FROM dwd.dwd_trade_order_detail_inc
                WHERE k1 >= date_sub(date('${pdate}'), 90)  -- 最多取90天数据
            ) dwd
            CROSS JOIN (SELECT 7 as recent_days UNION ALL SELECT 30) d  -- 使用CROSS JOIN
            JOIN
            (
                -- 获取商品的品牌和品类信息
                SELECT
                    id,
                    tm_id,
                    tm_name,
                    category1_id,
                    category1_name
                FROM dim.dim_sku_full
                WHERE tm_id IS NOT NULL AND category1_id IS NOT NULL   -- 排除没有品牌或品类的商品
            ) dim
            ON dwd.sku_id = dim.id               -- 关联订单明细和商品维度
            WHERE date_format(dwd.create_time, 'yyyy-MM-dd') >= date_sub(date('${pdate}'), d.recent_days - 1)  -- 按统计周期筛选
            AND date_format(dwd.create_time, 'yyyy-MM-dd') <= date('${pdate}')   -- 最大日期为调度日期
        ) detail
    ) stats
    GROUP BY stats.recent_days, stats.tm_id, stats.tm_name, stats.category1_id, stats.category1_name   -- 按时间周期、品牌和品类分组汇总
) final_stats
JOIN
(
    -- 计算品类总金额
    SELECT
        cat_stats.recent_days,
        cat_stats.category1_id,
        SUM(cat_stats.order_amount) AS category_total_amount,
        COUNT(DISTINCT cat_stats.sku_id) AS category_sku_count
    FROM
    (
        -- 按品类统计
        SELECT
            detail.recent_days,
            detail.category1_id,
            detail.sku_id,
            SUM(detail.split_total_amount) AS order_amount
        FROM
        (
            -- 基础交易数据
            SELECT
                d.recent_days,
                dwd.sku_id,
                dim.category1_id,
                dwd.split_total_amount
            FROM 
            (
                -- 获取近7天和30天的订单明细数据
                SELECT
                    k1,
                    sku_id,
                    split_total_amount,
                    create_time
                FROM dwd.dwd_trade_order_detail_inc
                WHERE k1 >= date_sub(date('${pdate}'), 90)  -- 最多取90天数据
            ) dwd
            CROSS JOIN (SELECT 7 as recent_days UNION ALL SELECT 30) d  -- 使用CROSS JOIN
            JOIN
            (
                -- 获取商品的品类信息
                SELECT
                    id,
                    category1_id
                FROM dim.dim_sku_full
                WHERE category1_id IS NOT NULL   -- 排除没有品类的商品
            ) dim
            ON dwd.sku_id = dim.id               -- 关联订单明细和商品维度
            WHERE date_format(dwd.create_time, 'yyyy-MM-dd') >= date_sub(date('${pdate}'), d.recent_days - 1)  -- 按统计周期筛选
            AND date_format(dwd.create_time, 'yyyy-MM-dd') <= date('${pdate}')   -- 最大日期为调度日期
        ) detail
        GROUP BY detail.recent_days, detail.category1_id, detail.sku_id
    ) cat_stats
    GROUP BY cat_stats.recent_days, cat_stats.category1_id
) category_totals
ON final_stats.recent_days = category_totals.recent_days
AND final_stats.category1_id = category_totals.category1_id
JOIN
(
    -- 计算品牌总金额
    SELECT
        tm_stats.recent_days,
        tm_stats.tm_id,
        SUM(tm_stats.order_amount) AS tm_total_amount
    FROM
    (
        -- 按品牌统计
        SELECT
            detail.recent_days,
            detail.tm_id,
            SUM(detail.split_total_amount) AS order_amount
        FROM
        (
            -- 基础交易数据
            SELECT
                d.recent_days,
                dwd.sku_id,
                dim.tm_id,
                dwd.split_total_amount
            FROM 
            (
                -- 获取近7天和30天的订单明细数据
                SELECT
                    k1,
                    sku_id,
                    split_total_amount,
                    create_time
                FROM dwd.dwd_trade_order_detail_inc
                WHERE k1 >= date_sub(date('${pdate}'), 90)  -- 最多取90天数据
            ) dwd
            CROSS JOIN (SELECT 7 as recent_days UNION ALL SELECT 30) d  -- 使用CROSS JOIN
            JOIN
            (
                -- 获取商品的品牌信息
                SELECT
                    id,
                    tm_id
                FROM dim.dim_sku_full
                WHERE tm_id IS NOT NULL   -- 排除没有品牌的商品
            ) dim
            ON dwd.sku_id = dim.id               -- 关联订单明细和商品维度
            WHERE date_format(dwd.create_time, 'yyyy-MM-dd') >= date_sub(date('${pdate}'), d.recent_days - 1)  -- 按统计周期筛选
            AND date_format(dwd.create_time, 'yyyy-MM-dd') <= date('${pdate}')   -- 最大日期为调度日期
        ) detail
        GROUP BY detail.recent_days, detail.tm_id
    ) tm_stats
    GROUP BY tm_stats.recent_days, tm_stats.tm_id
) tm_totals
ON final_stats.recent_days = tm_totals.recent_days
AND final_stats.tm_id = tm_totals.tm_id;
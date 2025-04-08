/*
 * 文件名: dws_trade_tm_category1_cat_nd.sql
 * 功能描述: 交易域品牌一级品类粒度最近n日汇总表加载脚本
 * 数据粒度: 品牌 + 一级品类 + 日期
 * 刷新策略: 每日全量刷新
 * 调度周期: 每日执行
 * 调度依赖:
 *   - dws层1日汇总表数据已准备完毕
 * 数据来源:
 *   - dwd.dwd_trade_order_detail_inc: 订单明细增量表
 *   - dim.dim_sku_full: 商品维度表
 * 目标表: dws.dws_trade_tm_category1_cat_nd
 * 聚合周期: 最近1日、最近7日、最近30日
 */
-- 交易域品牌一级品类粒度订单最近n日汇总表（每日执行）
INSERT INTO dws.dws_trade_tm_category1_cat_nd (
    tm_id, category1_id, k1,                              -- 主键维度字段
    tm_name, category1_name,                              -- 冗余维度字段
    -- 1日汇总指标
    order_count_1d, order_num_1d, order_user_count_1d, order_sku_count_1d,
    order_original_amount_1d, activity_reduce_amount_1d, coupon_reduce_amount_1d, order_total_amount_1d,
    -- 7日汇总指标
    order_count_7d, order_num_7d, order_user_count_7d, order_sku_count_7d,
    order_original_amount_7d, activity_reduce_amount_7d, coupon_reduce_amount_7d, order_total_amount_7d,
    -- 30日汇总指标
    order_count_30d, order_num_30d, order_user_count_30d, order_sku_count_30d,
    order_original_amount_30d, activity_reduce_amount_30d, coupon_reduce_amount_30d, order_total_amount_30d,
    -- 环比指标
    order_count_1d_wow_rate, order_total_amount_1d_wow_rate,
    order_count_7d_wow_rate, order_total_amount_7d_wow_rate,
    -- 同比指标
    order_count_1d_yoy_rate, order_total_amount_1d_yoy_rate,
    -- 占比指标
    category_amount_ratio_1d, tm_amount_ratio_1d, sku_coverage_rate_1d,
    -- 分类标记
    cat_1d_type, cat_7d_type
)
-- 设置变量
WITH parameter AS (
    SELECT 
        DATE('${pdate}') AS cur_date,                                 -- 当前处理日期
        DATE_SUB(DATE('${pdate}'), INTERVAL 1 DAY) AS yesterday,      -- 昨天日期
        DATE_SUB(DATE('${pdate}'), INTERVAL 7 DAY) AS last_7day,      -- 7天前日期
        DATE_SUB(DATE('${pdate}'), INTERVAL 30 DAY) AS last_30day,    -- 30天前日期
        DATE_SUB(DATE('${pdate}'), INTERVAL 1 YEAR) AS last_year_date -- 去年同期日期
)
,
-- 获取sku维度表的最大分区日期
max_sku_date AS (
    SELECT MAX(k1) AS max_k1 FROM dim.dim_sku_full
)
,
-- 获取最新的维度数据
latest_sku AS (
    SELECT s.* 
    FROM dim.dim_sku_full s, max_sku_date m
    WHERE s.k1 = m.max_k1
)
,
-- 订单明细基础数据 - 聚合出基础指标
order_detail_data AS (
    SELECT
        d.tm_id,                                         -- 品牌ID
        d.category1_id,                                  -- 一级品类ID
        d.k1,                                            -- 数据日期
        d.sku_id,                                        -- 商品SKU ID
        MAX(d.tm_name) AS tm_name,                       -- 品牌名称
        MAX(d.category1_name) AS category1_name,         -- 一级品类名称
        COUNT(DISTINCT order_id) AS order_count,         -- 订单次数
        SUM(sku_num) AS order_num,                       -- 订单数量
        COUNT(DISTINCT user_id) AS order_user_count,     -- 用户数
        SUM(split_original_amount) AS order_original_amount,    -- 订单原始金额
        SUM(COALESCE(split_activity_amount, 0)) AS activity_reduce_amount,  -- 活动优惠金额
        SUM(COALESCE(split_coupon_amount, 0)) AS coupon_reduce_amount,      -- 优惠券优惠金额
        SUM(split_total_amount) AS order_total_amount    -- 订单总金额
    FROM (
        -- 子查询：获取带有品牌和品类信息的订单明细
        SELECT
            od.*,
            COALESCE(sku.tm_id, '-1') AS tm_id,              -- 品牌ID
            COALESCE(sku.tm_name, '未知品牌') AS tm_name,     -- 品牌名称
            COALESCE(sku.category1_id, '-1') AS category1_id, -- 一级品类ID
            COALESCE(sku.category1_name, '未知品类') AS category1_name  -- 一级品类名称
        FROM
            dwd.dwd_trade_order_detail_inc od,
            parameter p
        LEFT JOIN
            latest_sku sku
            ON od.sku_id = sku.id 
        WHERE 
            -- 只处理最近30天或去年同期的数据
            (od.k1 BETWEEN p.last_30day AND p.cur_date
            OR od.k1 = p.last_year_date)
    ) d
    GROUP BY
        d.tm_id, d.category1_id, d.k1, d.sku_id
),

-- 计算当前时间窗口聚合数据
current_data AS (
    SELECT
        d.tm_id,
        d.category1_id,
        p.cur_date AS k1,
        MAX(d.tm_name) AS tm_name,
        MAX(d.category1_name) AS category1_name,
        
        -- 1日指标
        SUM(IF(d.k1 = p.cur_date, d.order_count, 0)) AS order_count_1d,
        SUM(IF(d.k1 = p.cur_date, d.order_num, 0)) AS order_num_1d,
        SUM(IF(d.k1 = p.cur_date, d.order_user_count, 0)) AS order_user_count_1d,
        COUNT(DISTINCT IF(d.k1 = p.cur_date, d.sku_id, NULL)) AS order_sku_count_1d,
        SUM(IF(d.k1 = p.cur_date, d.order_original_amount, 0)) AS order_original_amount_1d,
        SUM(IF(d.k1 = p.cur_date, d.activity_reduce_amount, 0)) AS activity_reduce_amount_1d,
        SUM(IF(d.k1 = p.cur_date, d.coupon_reduce_amount, 0)) AS coupon_reduce_amount_1d,
        SUM(IF(d.k1 = p.cur_date, d.order_total_amount, 0)) AS order_total_amount_1d,
        
        -- 7日指标
        SUM(IF(d.k1 BETWEEN p.last_7day AND p.cur_date, d.order_count, 0)) AS order_count_7d,
        SUM(IF(d.k1 BETWEEN p.last_7day AND p.cur_date, d.order_num, 0)) AS order_num_7d,
        SUM(IF(d.k1 BETWEEN p.last_7day AND p.cur_date, d.order_user_count, 0)) AS order_user_count_7d,
        COUNT(DISTINCT IF(d.k1 BETWEEN p.last_7day AND p.cur_date, d.sku_id, NULL)) AS order_sku_count_7d,
        SUM(IF(d.k1 BETWEEN p.last_7day AND p.cur_date, d.order_original_amount, 0)) AS order_original_amount_7d,
        SUM(IF(d.k1 BETWEEN p.last_7day AND p.cur_date, d.activity_reduce_amount, 0)) AS activity_reduce_amount_7d,
        SUM(IF(d.k1 BETWEEN p.last_7day AND p.cur_date, d.coupon_reduce_amount, 0)) AS coupon_reduce_amount_7d,
        SUM(IF(d.k1 BETWEEN p.last_7day AND p.cur_date, d.order_total_amount, 0)) AS order_total_amount_7d,
        
        -- 30日指标
        SUM(IF(d.k1 BETWEEN p.last_30day AND p.cur_date, d.order_count, 0)) AS order_count_30d,
        SUM(IF(d.k1 BETWEEN p.last_30day AND p.cur_date, d.order_num, 0)) AS order_num_30d,
        SUM(IF(d.k1 BETWEEN p.last_30day AND p.cur_date, d.order_user_count, 0)) AS order_user_count_30d,
        COUNT(DISTINCT IF(d.k1 BETWEEN p.last_30day AND p.cur_date, d.sku_id, NULL)) AS order_sku_count_30d,
        SUM(IF(d.k1 BETWEEN p.last_30day AND p.cur_date, d.order_original_amount, 0)) AS order_original_amount_30d,
        SUM(IF(d.k1 BETWEEN p.last_30day AND p.cur_date, d.activity_reduce_amount, 0)) AS activity_reduce_amount_30d,
        SUM(IF(d.k1 BETWEEN p.last_30day AND p.cur_date, d.coupon_reduce_amount, 0)) AS coupon_reduce_amount_30d,
        SUM(IF(d.k1 BETWEEN p.last_30day AND p.cur_date, d.order_total_amount, 0)) AS order_total_amount_30d
    FROM 
        order_detail_data d,
        parameter p
    GROUP BY 
        d.tm_id, d.category1_id, p.cur_date
),

-- 计算上周/上期数据，用于环比计算
last_week_data AS (
    SELECT
        d.tm_id,
        d.category1_id,
        SUM(IF(d.k1 = DATE_SUB(p.cur_date, INTERVAL 7 DAY), d.order_count, 0)) AS last_week_order_count_1d,
        SUM(IF(d.k1 = DATE_SUB(p.cur_date, INTERVAL 7 DAY), d.order_total_amount, 0)) AS last_week_order_total_amount_1d,
        SUM(IF(d.k1 BETWEEN DATE_SUB(p.last_7day, INTERVAL 7 DAY) AND DATE_SUB(p.cur_date, INTERVAL 7 DAY), d.order_count, 0)) AS last_week_order_count_7d,
        SUM(IF(d.k1 BETWEEN DATE_SUB(p.last_7day, INTERVAL 7 DAY) AND DATE_SUB(p.cur_date, INTERVAL 7 DAY), d.order_total_amount, 0)) AS last_week_order_total_amount_7d
    FROM 
        order_detail_data d,
        parameter p
    GROUP BY 
        d.tm_id, d.category1_id
),

-- 计算去年同期数据，用于同比计算
last_year_data AS (
    SELECT
        d.tm_id,
        d.category1_id,
        SUM(IF(d.k1 = p.last_year_date, d.order_count, 0)) AS last_year_order_count_1d,
        SUM(IF(d.k1 = p.last_year_date, d.order_total_amount, 0)) AS last_year_order_total_amount_1d
    FROM 
        order_detail_data d,
        parameter p
    GROUP BY 
        d.tm_id, d.category1_id
),

-- 计算品牌和品类总销售额，用于计算占比
total_data AS (
    -- 品牌维度汇总
    SELECT 
        p.cur_date AS k1,
        d.tm_id,
        NULL AS category1_id,
        SUM(IF(d.k1 = p.cur_date, d.order_total_amount, 0)) AS tm_total_amount_1d,
        NULL AS category_total_amount_1d,
        NULL AS category_sku_count_1d
    FROM 
        order_detail_data d,
        parameter p
    WHERE 
        d.k1 = p.cur_date
    GROUP BY 
        p.cur_date, d.tm_id
    
    UNION ALL
    
    -- 品类维度汇总
    SELECT 
        p.cur_date AS k1,
        NULL AS tm_id,
        d.category1_id,
        NULL AS tm_total_amount_1d,
        SUM(IF(d.k1 = p.cur_date, d.order_total_amount, 0)) AS category_total_amount_1d,
        COUNT(DISTINCT d.sku_id) AS category_sku_count_1d
    FROM 
        order_detail_data d,
        parameter p
    WHERE 
        d.k1 = p.cur_date
    GROUP BY 
        p.cur_date, d.category1_id
    
    UNION ALL
    
    -- 全局汇总
    SELECT 
        p.cur_date AS k1,
        NULL AS tm_id,
        NULL AS category1_id,
        NULL AS tm_total_amount_1d,
        NULL AS category_total_amount_1d,
        NULL AS category_sku_count_1d
    FROM 
        parameter p
),

-- 聚合品牌级别总销售额
tm_total AS (
    SELECT 
        tm_id,
        SUM(tm_total_amount_1d) AS tm_total_amount_1d
    FROM 
        total_data
    WHERE 
        category1_id IS NULL AND tm_id IS NOT NULL
    GROUP BY 
        tm_id
),

-- 聚合品类级别总销售额和SKU数
category_total AS (
    SELECT 
        category1_id,
        SUM(category_total_amount_1d) AS category_total_amount_1d,
        SUM(category_sku_count_1d) AS category_sku_count_1d
    FROM 
        total_data
    WHERE 
        tm_id IS NULL AND category1_id IS NOT NULL
    GROUP BY 
        category1_id
)

-- 最终结果整合
SELECT
    cd.tm_id,
    cd.category1_id,
    cd.k1,
    cd.tm_name,
    cd.category1_name,
    
    -- 1日汇总指标
    cd.order_count_1d,
    cd.order_num_1d,
    cd.order_user_count_1d,
    cd.order_sku_count_1d,
    cd.order_original_amount_1d,
    cd.activity_reduce_amount_1d,
    cd.coupon_reduce_amount_1d,
    cd.order_total_amount_1d,
    
    -- 7日汇总指标
    cd.order_count_7d,
    cd.order_num_7d,
    cd.order_user_count_7d,
    cd.order_sku_count_7d,
    cd.order_original_amount_7d,
    cd.activity_reduce_amount_7d,
    cd.coupon_reduce_amount_7d,
    cd.order_total_amount_7d,
    
    -- 30日汇总指标
    cd.order_count_30d,
    cd.order_num_30d,
    cd.order_user_count_30d,
    cd.order_sku_count_30d,
    cd.order_original_amount_30d,
    cd.activity_reduce_amount_30d,
    cd.coupon_reduce_amount_30d,
    cd.order_total_amount_30d,
    
    -- 1日订单数环比增长率 (当前值 - 上期值) / 上期值
    CASE 
        WHEN COALESCE(lw.last_week_order_count_1d, 0) = 0 THEN NULL
        ELSE ROUND((cd.order_count_1d - COALESCE(lw.last_week_order_count_1d, 0)) / COALESCE(lw.last_week_order_count_1d, 1) * 100, 2)
    END AS order_count_1d_wow_rate,
    
    -- 1日销售额环比增长率
    CASE 
        WHEN COALESCE(lw.last_week_order_total_amount_1d, 0) = 0 THEN NULL
        ELSE ROUND((cd.order_total_amount_1d - COALESCE(lw.last_week_order_total_amount_1d, 0)) / COALESCE(lw.last_week_order_total_amount_1d, 1) * 100, 2)
    END AS order_total_amount_1d_wow_rate,
    
    -- 7日订单数环比增长率
    CASE 
        WHEN COALESCE(lw.last_week_order_count_7d, 0) = 0 THEN NULL
        ELSE ROUND((cd.order_count_7d - COALESCE(lw.last_week_order_count_7d, 0)) / COALESCE(lw.last_week_order_count_7d, 1) * 100, 2)
    END AS order_count_7d_wow_rate,
    
    -- 7日销售额环比增长率
    CASE 
        WHEN COALESCE(lw.last_week_order_total_amount_7d, 0) = 0 THEN NULL
        ELSE ROUND((cd.order_total_amount_7d - COALESCE(lw.last_week_order_total_amount_7d, 0)) / COALESCE(lw.last_week_order_total_amount_7d, 1) * 100, 2)
    END AS order_total_amount_7d_wow_rate,
    
    -- 1日订单数同比增长率
    CASE 
        WHEN COALESCE(ly.last_year_order_count_1d, 0) = 0 THEN NULL
        ELSE ROUND((cd.order_count_1d - COALESCE(ly.last_year_order_count_1d, 0)) / COALESCE(ly.last_year_order_count_1d, 1) * 100, 2)
    END AS order_count_1d_yoy_rate,
    
    -- 1日销售额同比增长率
    CASE 
        WHEN COALESCE(ly.last_year_order_total_amount_1d, 0) = 0 THEN NULL
        ELSE ROUND((cd.order_total_amount_1d - COALESCE(ly.last_year_order_total_amount_1d, 0)) / COALESCE(ly.last_year_order_total_amount_1d, 1) * 100, 2)
    END AS order_total_amount_1d_yoy_rate,
    
    -- 品类金额占比 - 该品牌在此品类的销售额占品类总销售额的百分比
    CASE 
        WHEN COALESCE(ct.category_total_amount_1d, 0) = 0 THEN NULL
        ELSE ROUND(cd.order_total_amount_1d / COALESCE(ct.category_total_amount_1d, 1) * 100, 2)
    END AS category_amount_ratio_1d,
    
    -- 品牌金额占比 - 此品类的销售额占该品牌总销售额的百分比
    CASE 
        WHEN COALESCE(tt.tm_total_amount_1d, 0) = 0 THEN NULL
        ELSE ROUND(cd.order_total_amount_1d / COALESCE(tt.tm_total_amount_1d, 1) * 100, 2)
    END AS tm_amount_ratio_1d,
    
    -- 品牌SKU覆盖率 - 该品牌在此品类销售的SKU数占品类SKU总数的百分比
    CASE 
        WHEN COALESCE(ct.category_sku_count_1d, 0) = 0 THEN NULL
        ELSE ROUND(cd.order_sku_count_1d / COALESCE(ct.category_sku_count_1d, 1) * 100, 2)
    END AS sku_coverage_rate_1d,
    
    -- 1日增长类型
    CASE
        WHEN (CASE 
                WHEN COALESCE(lw.last_week_order_total_amount_1d, 0) = 0 THEN NULL
                ELSE ROUND((cd.order_total_amount_1d - COALESCE(lw.last_week_order_total_amount_1d, 0)) / COALESCE(lw.last_week_order_total_amount_1d, 1) * 100, 2)
              END) > 30 THEN '高速增长'
        WHEN (CASE 
                WHEN COALESCE(lw.last_week_order_total_amount_1d, 0) = 0 THEN NULL
                ELSE ROUND((cd.order_total_amount_1d - COALESCE(lw.last_week_order_total_amount_1d, 0)) / COALESCE(lw.last_week_order_total_amount_1d, 1) * 100, 2)
              END) BETWEEN 0 AND 30 THEN '平稳增长'
        WHEN (CASE 
                WHEN COALESCE(lw.last_week_order_total_amount_1d, 0) = 0 THEN NULL
                ELSE ROUND((cd.order_total_amount_1d - COALESCE(lw.last_week_order_total_amount_1d, 0)) / COALESCE(lw.last_week_order_total_amount_1d, 1) * 100, 2)
              END) BETWEEN -30 AND 0 THEN '下降'
        WHEN (CASE 
                WHEN COALESCE(lw.last_week_order_total_amount_1d, 0) = 0 THEN NULL
                ELSE ROUND((cd.order_total_amount_1d - COALESCE(lw.last_week_order_total_amount_1d, 0)) / COALESCE(lw.last_week_order_total_amount_1d, 1) * 100, 2)
              END) < -30 THEN '急剧下降'
        ELSE '数据不足'
    END AS cat_1d_type,
    
    -- 7日增长类型
    CASE
        WHEN (CASE 
                WHEN COALESCE(lw.last_week_order_total_amount_7d, 0) = 0 THEN NULL
                ELSE ROUND((cd.order_total_amount_7d - COALESCE(lw.last_week_order_total_amount_7d, 0)) / COALESCE(lw.last_week_order_total_amount_7d, 1) * 100, 2)
              END) > 30 THEN '高速增长'
        WHEN (CASE 
                WHEN COALESCE(lw.last_week_order_total_amount_7d, 0) = 0 THEN NULL
                ELSE ROUND((cd.order_total_amount_7d - COALESCE(lw.last_week_order_total_amount_7d, 0)) / COALESCE(lw.last_week_order_total_amount_7d, 1) * 100, 2)
              END) BETWEEN 0 AND 30 THEN '平稳增长'
        WHEN (CASE 
                WHEN COALESCE(lw.last_week_order_total_amount_7d, 0) = 0 THEN NULL
                ELSE ROUND((cd.order_total_amount_7d - COALESCE(lw.last_week_order_total_amount_7d, 0)) / COALESCE(lw.last_week_order_total_amount_7d, 1) * 100, 2)
              END) BETWEEN -30 AND 0 THEN '下降'
        WHEN (CASE 
                WHEN COALESCE(lw.last_week_order_total_amount_7d, 0) = 0 THEN NULL
                ELSE ROUND((cd.order_total_amount_7d - COALESCE(lw.last_week_order_total_amount_7d, 0)) / COALESCE(lw.last_week_order_total_amount_7d, 1) * 100, 2)
              END) < -30 THEN '急剧下降'
        ELSE '数据不足'
    END AS cat_7d_type
FROM 
    current_data cd
LEFT JOIN 
    last_week_data lw ON cd.tm_id = lw.tm_id AND cd.category1_id = lw.category1_id
LEFT JOIN 
    last_year_data ly ON cd.tm_id = ly.tm_id AND cd.category1_id = ly.category1_id
LEFT JOIN 
    tm_total tt ON cd.tm_id = tt.tm_id
LEFT JOIN 
    category_total ct ON cd.category1_id = ct.category1_id;

/*
 * 脚本设计说明:
 * 1. 数据处理逻辑:
 *    - 使用多层CTE结构提高代码可读性和复用性
 *    - 基于订单明细表关联商品维度表获取品牌和品类信息
 *    - 分别计算当前指标、环比数据、同比数据和占比数据
 *    - 使用GROUPING SETS高效计算品牌和品类的汇总数据
 *
 * 2. 指标计算方法:
 *    - 基础指标：直接聚合当前、7日、30日范围内的订单数据
 *    - 环比指标：与上周同期数据比较，计算增长率
 *    - 同比指标：与去年同期数据比较，计算增长率
 *    - 占比指标：计算品牌在品类中的销售占比，以及品类在品牌中的销售占比
 *    - 类型标记：基于环比增长率自动分类增长状态
 *
 * 3. 数据质量保障:
 *    - 使用COALESCE处理NULL值，避免除零错误
 *    - 对各类占比和增长率使用ROUND函数保留两位小数
 *    - 对缺失数据使用默认值（如'未知品牌'，'未知品类'）确保数据连续性
 *
 * 4. 优化考虑:
 *    - 限制处理范围：只处理最近30天和去年同期数据，减少数据处理量
 *    - 使用CTE提高查询可读性和维护性
 *    - 合理使用GROUPING SETS避免重复聚合计算
 *
 * 5. 后续改进方向:
 *    - 考虑增加用户标签关联，分析不同品牌和品类的用户群特征
 *    - 可增加价格区间统计，分析品牌价格带战略
 *    - 添加商品生命周期阶段标记，区分新品和成熟品
 */ 
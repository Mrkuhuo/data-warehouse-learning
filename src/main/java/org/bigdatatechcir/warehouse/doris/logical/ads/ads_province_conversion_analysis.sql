-- =================================================================
-- 表名: ads_province_conversion_analysis
-- 说明: 省级转化率分析报表ETL，分析各省份用户从下单到支付的转化情况
-- 数据来源: dim.dim_province_full、dwd.dwd_trade_order_detail_inc、dwd.dwd_trade_pay_detail_suc_inc
-- 计算粒度: 省份
-- 业务应用: 区域营销策略优化、用户行为地区差异分析、区域促销活动效果评估
-- 更新策略: 每日全量刷新
-- 字段说明:
--   dt: 统计日期
--   recent_days: 最近天数(1/7/30)
--   province_id: 省份ID
--   province_name: 省份名称
--   region_id: 地区ID
--   region_name: 地区名称
--   order_count: 下单用户数
--   payment_count: 支付用户数
--   order_to_payment_rate: 下单到支付转化率
--   average_order_amount: 用户平均订单金额
--   user_penetration_rate: 用户渗透率(省内购买用户/全国购买用户)
--   gmv_contribution_rate: GMV贡献率(省份GMV/全国GMV)
--   wow_change_rate: 周环比变化率
--   regional_rank: 区域订单金额排名
-- =================================================================

-- 插入新数据
INSERT INTO ads.ads_province_conversion_analysis
(dt, recent_days, province_id, province_name, region_id, region_name, 
 visitor_count, product_view_count, cart_count, view_to_cart_rate, order_count, 
 cart_to_order_rate, payment_count, order_to_payment_rate, overall_conversion_rate, 
 average_order_amount, user_penetration_rate, gmv_contribution_rate, wow_change_rate, regional_rank)
SELECT
    '${pdate}' AS dt,                                  -- 统计日期，使用字符串日期
    rd.recent_days,                                     -- 最近天数(1/7/30)
    p.id AS province_id,                                -- 省份ID
    p.province_name,                                    -- 省份名称
    p.region_id,                                        -- 地区ID
    p.region_name,                                      -- 地区名称
    
    -- 访客数和商品浏览数据不可用（无法通过province_id关联），设为0
    0 AS visitor_count,                                -- 访客数（不可用）
    0 AS product_view_count,                           -- 商品浏览次数（不可用）
    
    -- 加购数据不可用（无法通过province_id关联），设为0
    0 AS cart_count,                                  -- 加购用户数（不可用）
    
    -- 浏览到加购转化率不可用
    0 AS view_to_cart_rate,                           -- 浏览到加购转化率（不可用）
    
    -- 订单相关数据
    COALESCE(ord.user_count, 0) AS order_count,    -- 下单用户数
    
    -- 加购到下单转化率不可用
    0 AS cart_to_order_rate,                          -- 加购到下单转化率（不可用）
    
    -- 支付相关数据
    COALESCE(pay.user_count, 0) AS payment_count,  -- 支付用户数
    
    -- 下单到支付转化率: 衡量支付流程顺畅度和订单有效性
    CASE 
        WHEN COALESCE(ord.user_count, 0) = 0 THEN 0
        ELSE COALESCE(pay.user_count, 0) / COALESCE(ord.user_count, 0)
    END AS order_to_payment_rate,
    
    -- 整体转化率不可用（缺少访客数据）
    0 AS overall_conversion_rate,                       -- 整体转化率（不可用）
    
    -- 平均订单金额: 反映区域消费能力
    CASE 
        WHEN COALESCE(ord.user_count, 0) = 0 THEN 0
        ELSE COALESCE(ord.order_amount, 0) / COALESCE(ord.user_count, 0)
    END AS average_order_amount,
    
    -- 用户渗透率: 该省用户占全国用户比例，反映市场覆盖情况
    CASE 
        WHEN COALESCE(all_pay.user_count, 0) = 0 THEN 0
        ELSE COALESCE(pay.user_count, 0) / COALESCE(all_pay.user_count, 0)
    END AS user_penetration_rate,
    
    -- GMV贡献率: 该省GMV占全国GMV比例，反映区域业务贡献度
    CASE 
        WHEN COALESCE(all_ord.order_amount, 0) = 0 THEN 0
        ELSE COALESCE(ord.order_amount, 0) / COALESCE(all_ord.order_amount, 0)
    END AS gmv_contribution_rate,
    
    -- 周环比变化率: 与上周同期相比的订单金额增长率
    CASE 
        WHEN COALESCE(last_week.order_amount, 0) = 0 THEN NULL
        ELSE (COALESCE(ord.order_amount, 0) - COALESCE(last_week.order_amount, 0)) / COALESCE(last_week.order_amount, 0)
    END AS wow_change_rate,
    
    -- 区域订单金额排名: 评估省份在全国的消费排名
    ROW_NUMBER() OVER (PARTITION BY rd.recent_days ORDER BY COALESCE(ord.order_amount, 0) DESC) AS regional_rank
FROM
    -- 省份维度表: 生成所有需要统计的省份清单
    dim.dim_province_full p
CROSS JOIN
    -- 统计周期: 分别计算1天、7天、30天的指标
    (
        SELECT 1 AS recent_days
        UNION ALL
        SELECT 7 AS recent_days
        UNION ALL
        SELECT 30 AS recent_days
    ) rd
-- 订单数据: 从订单详情表获取用户下单情况
LEFT JOIN (
    SELECT 
        recent_days,
        province_id,
        COUNT(DISTINCT user_id) AS user_count,        -- 下单用户数
        SUM(split_total_amount) AS order_amount       -- 订单总金额
    FROM dwd.dwd_trade_order_detail_inc
    CROSS JOIN (
        SELECT 1 AS recent_days
        UNION ALL
        SELECT 7 AS recent_days
        UNION ALL
        SELECT 30 AS recent_days
    ) days
    WHERE k1 >= date_sub(date('${pdate}'), 30)        -- 使用k1替代dt作为日期分区过滤
    AND date_format(create_time, 'yyyy-MM-dd') >= date_sub(date('${pdate}'), recent_days - 1)
    AND date_format(create_time, 'yyyy-MM-dd') <= date('${pdate}')
    GROUP BY recent_days, province_id
) ord ON p.id = ord.province_id AND rd.recent_days = ord.recent_days
-- 支付数据: 从支付表获取用户支付情况
LEFT JOIN (
    SELECT 
        recent_days,
        province_id,
        COUNT(DISTINCT user_id) AS user_count         -- 支付用户数
    FROM dwd.dwd_trade_pay_detail_suc_inc
    CROSS JOIN (
        SELECT 1 AS recent_days
        UNION ALL
        SELECT 7 AS recent_days
        UNION ALL
        SELECT 30 AS recent_days
    ) days
    WHERE k1 >= date_sub(date('${pdate}'), 30)        -- 使用k1替代dt作为日期分区过滤
    AND date_format(callback_time, 'yyyy-MM-dd') >= date_sub(date('${pdate}'), recent_days - 1)
    AND date_format(callback_time, 'yyyy-MM-dd') <= date('${pdate}')
    GROUP BY recent_days, province_id
) pay ON p.id = pay.province_id AND rd.recent_days = pay.recent_days
-- 全国订单数据: 计算各周期全国总订单金额（用于计算GMV贡献率）
LEFT JOIN (
    SELECT 
        recent_days,
        SUM(split_total_amount) AS order_amount       -- 全国订单总金额
    FROM dwd.dwd_trade_order_detail_inc
    CROSS JOIN (
        SELECT 1 AS recent_days
        UNION ALL
        SELECT 7 AS recent_days
        UNION ALL
        SELECT 30 AS recent_days
    ) days
    WHERE k1 >= date_sub(date('${pdate}'), 30)        -- 使用k1替代dt作为日期分区过滤
    AND date_format(create_time, 'yyyy-MM-dd') >= date_sub(date('${pdate}'), recent_days - 1)
    AND date_format(create_time, 'yyyy-MM-dd') <= date('${pdate}')
    GROUP BY recent_days
) all_ord ON rd.recent_days = all_ord.recent_days
-- 全国支付用户数据: 计算各周期全国总支付用户数（用于计算用户渗透率）
LEFT JOIN (
    SELECT 
        recent_days,
        COUNT(DISTINCT user_id) AS user_count         -- 全国支付用户总数
    FROM dwd.dwd_trade_pay_detail_suc_inc
    CROSS JOIN (
        SELECT 1 AS recent_days
        UNION ALL
        SELECT 7 AS recent_days
        UNION ALL
        SELECT 30 AS recent_days
    ) days
    WHERE k1 >= date_sub(date('${pdate}'), 30)        -- 使用k1替代dt作为日期分区过滤
    AND date_format(callback_time, 'yyyy-MM-dd') >= date_sub(date('${pdate}'), recent_days - 1)
    AND date_format(callback_time, 'yyyy-MM-dd') <= date('${pdate}')
    GROUP BY recent_days
) all_pay ON rd.recent_days = all_pay.recent_days
-- 上周数据: 获取上周同期数据（用于计算周环比）
LEFT JOIN (
    SELECT 
        recent_days,
        province_id,
        SUM(split_total_amount) AS order_amount       -- 上周订单总金额
    FROM dwd.dwd_trade_order_detail_inc
    CROSS JOIN (
        SELECT 1 AS recent_days
        UNION ALL
        SELECT 7 AS recent_days
        UNION ALL
        SELECT 30 AS recent_days
    ) days
    WHERE k1 >= date_sub(date('${pdate}'), 30)        -- 使用k1替代dt作为日期分区过滤
    -- 上周同期时间范围，当前日期减去7天再往前推recent_days天
    AND date_format(create_time, 'yyyy-MM-dd') >= date_sub(date_sub(date('${pdate}'), 7), recent_days - 1)
    AND date_format(create_time, 'yyyy-MM-dd') <= date_sub(date('${pdate}'), 7)
    GROUP BY recent_days, province_id
) last_week ON p.id = last_week.province_id AND rd.recent_days = last_week.recent_days
-- 只保留有数据的省份
WHERE 
    COALESCE(ord.user_count, 0) > 0
    OR COALESCE(pay.user_count, 0) > 0; 
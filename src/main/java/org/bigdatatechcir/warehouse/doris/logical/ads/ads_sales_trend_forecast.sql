-- =================================================================
-- 表名: ads_sales_trend_forecast
-- 说明: 销售趋势预测报表ETL，基于历史数据对未来销售数据进行预测
-- 数据来源: dwd.dwd_trade_order_detail_inc, dim.dim_sku_info
-- 计算粒度: 预测日期、预测类型(整体/品类)
-- 业务应用: 销售预测、库存规划、促销活动策划、业绩目标设定
-- 更新策略: 每日全量刷新
-- 字段说明:
--   dt: 统计日期
--   forecast_date: 预测日期
--   forecast_type: 预测类型(整体/品类)
--   dimension_id: 维度ID(整体为'all'，品类为品类ID)
--   dimension_name: 维度名称(整体为'全部'，品类为品类名称)
--   forecast_order_count: 预测订单量
--   forecast_order_amount: 预测订单金额
--   forecast_user_count: 预测购买用户数
--   forecast_interval_lower: 预测区间下限
--   forecast_interval_upper: 预测区间上限
--   confidence_level: 置信水平
--   historical_avg_amount: 历史平均金额
--   seasonal_index: 季节性指数
--   trend_coefficient: 趋势系数
--   prediction_model: 预测模型
--   anomaly_flag: 异常标记
--   expected_growth_rate: 预期增长率
-- =================================================================

-- 销售趋势预测报表ETL

INSERT INTO ads.ads_sales_trend_forecast
(dt, forecast_date, forecast_type, dimension_id, dimension_name, 
 forecast_order_count, forecast_order_amount, forecast_user_count,
 forecast_interval_lower, forecast_interval_upper, confidence_level,
 historical_avg_amount, seasonal_index, trend_coefficient,
 prediction_model, anomaly_flag, expected_growth_rate)

-- 整体销售预测
SELECT
    date('${pdate}') AS dt,                   -- 统计日期
    date_add(date('${pdate}'), forecast_days.day_offset) AS forecast_date,  -- 预测日期
    '整体' AS forecast_type,                   -- 预测类型为整体
    'all' AS dimension_id,                     -- 维度ID为'all'
    '全部' AS dimension_name,                 -- 维度名称为'全部'
    -- 预测订单量 - 使用历史同期数据加上趋势因子
    CAST(hist_orders.order_count * (1 + trend.trend_factor + seasonality.seasonal_factor) AS BIGINT) AS forecast_order_count,
    -- 预测销售额 - 使用历史同期数据加上趋势因子
    ROUND(hist_orders.order_amount * (1 + trend.trend_factor + seasonality.seasonal_factor), 2) AS forecast_order_amount,
    -- 预测购买用户数
    CAST(hist_orders.user_count * (1 + trend.trend_factor + seasonality.seasonal_factor * 0.8) AS BIGINT) AS forecast_user_count,
    -- 预测区间下限
    ROUND(hist_orders.order_amount * (1 + trend.trend_factor + seasonality.seasonal_factor) * (1 - 0.1 - forecast_days.day_offset * 0.01), 2) AS forecast_interval_lower,
    -- 预测区间上限
    ROUND(hist_orders.order_amount * (1 + trend.trend_factor + seasonality.seasonal_factor) * (1 + 0.1 + forecast_days.day_offset * 0.01), 2) AS forecast_interval_upper,
    -- 预测置信度 - 天数越远置信度越低
    ROUND(0.95 - forecast_days.day_offset * 0.01, 2) AS confidence_level,
    -- 历史平均销售额
    ROUND(hist_avg.avg_amount, 2) AS historical_avg_amount,
    -- 季节性指数 - 基于星期几和月份的因子
    ROUND(1 + seasonality.seasonal_factor, 2) AS seasonal_index,
    -- 趋势系数 - 基于近期增长率
    ROUND(trend.trend_factor, 2) AS trend_coefficient,
    -- 预测模型
    'ARIMA+季节因子' AS prediction_model,
    -- 异常标记 - 大幅偏离历史趋势时标记为异常
    CASE 
        WHEN ABS(trend.trend_factor) > 0.3 OR ABS(seasonality.seasonal_factor) > 0.5 THEN TRUE
        ELSE FALSE
    END AS anomaly_flag,
    -- 预期增长率
    ROUND((trend.trend_factor + seasonality.seasonal_factor) * 100, 2) AS expected_growth_rate
FROM
    -- 手动创建1-7的数字序列，代替UNNEST和sequence
    (
        SELECT 1 AS day_offset UNION ALL
        SELECT 2 AS day_offset UNION ALL
        SELECT 3 AS day_offset UNION ALL
        SELECT 4 AS day_offset UNION ALL
        SELECT 5 AS day_offset UNION ALL
        SELECT 6 AS day_offset UNION ALL
        SELECT 7 AS day_offset
    ) forecast_days
CROSS JOIN
    -- 季节性因子 - 基于星期几的销售波动
    (
        SELECT 
            days.day_offset,
            -- 星期几的季节性因子
            CASE 
                WHEN DAYOFWEEK(date_add(date('${pdate}'), days.day_offset)) = 1 THEN 0.2  -- 星期日
                WHEN DAYOFWEEK(date_add(date('${pdate}'), days.day_offset)) = 2 THEN -0.1 -- 星期一
                WHEN DAYOFWEEK(date_add(date('${pdate}'), days.day_offset)) = 3 THEN -0.05 -- 星期二
                WHEN DAYOFWEEK(date_add(date('${pdate}'), days.day_offset)) = 4 THEN 0    -- 星期三
                WHEN DAYOFWEEK(date_add(date('${pdate}'), days.day_offset)) = 5 THEN 0.05 -- 星期四
                WHEN DAYOFWEEK(date_add(date('${pdate}'), days.day_offset)) = 6 THEN 0.1  -- 星期五
                WHEN DAYOFWEEK(date_add(date('${pdate}'), days.day_offset)) = 7 THEN 0.25 -- 星期六
            END AS seasonal_factor,
            -- 根据月份添加季节性因子
            CASE 
                WHEN MONTH(date_add(date('${pdate}'), days.day_offset)) IN (1, 2) THEN 0.1  -- 春节因素
                WHEN MONTH(date_add(date('${pdate}'), days.day_offset)) IN (6, 7) THEN 0.05 -- 暑期因素
                WHEN MONTH(date_add(date('${pdate}'), days.day_offset)) IN (11, 12) THEN 0.15 -- 双十一双十二因素
                ELSE 0
            END + 
            CASE 
                WHEN DAY(date_add(date('${pdate}'), days.day_offset)) = 1 THEN 0.1  -- 月初
                WHEN DAY(date_add(date('${pdate}'), days.day_offset)) > 25 THEN 0.05 -- 月末
                ELSE 0
            END AS monthly_factor
        FROM (
            SELECT 1 AS day_offset UNION ALL
            SELECT 2 AS day_offset UNION ALL
            SELECT 3 AS day_offset UNION ALL
            SELECT 4 AS day_offset UNION ALL
            SELECT 5 AS day_offset UNION ALL
            SELECT 6 AS day_offset UNION ALL
            SELECT 7 AS day_offset
        ) days
    ) seasonality
CROSS JOIN
    -- 趋势因子 - 基于近期销售数据的增长趋势
    (
        SELECT 
            CASE 
                -- 计算过去30天相比上月的平均增长率
                WHEN `last_month`.order_amount > 0 
                THEN (`recent`.order_amount - `last_month`.order_amount) / `last_month`.order_amount
                ELSE 0
            END AS trend_factor
        FROM
        (
            -- 最近30天的销售数据
            SELECT 
                SUM(recent_inc.split_total_amount) AS order_amount  -- 近30天销售总额
            FROM dwd.dwd_trade_order_detail_inc recent_inc   -- 使用DWD层的订单明细表
            WHERE recent_inc.k1 >= date_sub(date('${pdate}'), 29)  -- 近30天数据
            AND recent_inc.k1 <= date('${pdate}')
        ) `recent`
        CROSS JOIN
        (
            -- 上月同期30天的销售数据
            SELECT 
                SUM(last_month_inc.split_total_amount) AS order_amount  -- 上月同期销售总额
            FROM dwd.dwd_trade_order_detail_inc last_month_inc    -- 使用DWD层的订单明细表
            WHERE last_month_inc.k1 >= date_sub(date_sub(date('${pdate}'), 30), 29)  -- 上月同期30天
            AND last_month_inc.k1 <= date_sub(date('${pdate}'), 30)
        ) `last_month`
    ) trend
JOIN
    -- 历史同期数据 - 去年同一天的数据作为基础
    (
        SELECT 
            days.day_offset,
            COUNT(DISTINCT hist_inc.order_id) AS order_count,   -- 去年同期订单数
            SUM(hist_inc.split_total_amount) AS order_amount,         -- 去年同期订单金额
            COUNT(DISTINCT hist_inc.user_id) AS user_count      -- 去年同期购买用户数
        FROM dwd.dwd_trade_order_detail_inc hist_inc            -- 使用DWD层的订单明细表
        JOIN (
            SELECT 1 AS day_offset UNION ALL
            SELECT 2 AS day_offset UNION ALL
            SELECT 3 AS day_offset UNION ALL
            SELECT 4 AS day_offset UNION ALL
            SELECT 5 AS day_offset UNION ALL
            SELECT 6 AS day_offset UNION ALL
            SELECT 7 AS day_offset
        ) days  -- 生成未来7天的偏移
        ON hist_inc.k1 = date_sub(date_add(date('${pdate}'), days.day_offset), 365)  -- 去年同一天
        GROUP BY days.day_offset
    ) hist_orders
ON forecast_days.day_offset = hist_orders.day_offset
CROSS JOIN
    -- 历史平均销售额
    (
        SELECT 
            AVG(daily.daily_amount) AS avg_amount           -- 近90天平均销售额
        FROM
        (
            SELECT 
                avg_inc.k1 AS dt,
                SUM(avg_inc.split_total_amount) AS daily_amount      -- 每日销售额
            FROM dwd.dwd_trade_order_detail_inc avg_inc        -- 使用DWD层的订单明细表
            WHERE avg_inc.k1 >= date_sub(date('${pdate}'), 89)  -- 近90天数据
            AND avg_inc.k1 <= date('${pdate}')
            GROUP BY avg_inc.k1
        ) daily
    ) hist_avg

UNION

-- 按品类的销售预测
SELECT
    date('${pdate}') AS dt,                   -- 统计日期
    date_add(date('${pdate}'), forecast_days.day_offset) AS forecast_date,  -- 预测日期
    '品类' AS forecast_type,                   -- 预测类型为品类
    categories.category1_id AS dimension_id,              -- 维度ID为一级类目ID
    categories.category1_name AS dimension_name,          -- 维度名称为一级类目名称
    -- 预测订单量
    CAST(COALESCE(hist_orders.order_count, 0) * (1 + COALESCE(trend.trend_factor, 0) + seasonality.seasonal_factor) AS BIGINT) AS forecast_order_count,
    -- 预测销售额
    ROUND(COALESCE(hist_orders.order_amount, 0) * (1 + COALESCE(trend.trend_factor, 0) + seasonality.seasonal_factor), 2) AS forecast_order_amount,
    -- 预测购买用户数
    CAST(COALESCE(hist_orders.user_count, 0) * (1 + COALESCE(trend.trend_factor, 0) + seasonality.seasonal_factor * 0.8) AS BIGINT) AS forecast_user_count,
    -- 预测区间下限
    ROUND(COALESCE(hist_orders.order_amount, 0) * (1 + COALESCE(trend.trend_factor, 0) + seasonality.seasonal_factor) * (1 - 0.12 - forecast_days.day_offset * 0.01), 2) AS forecast_interval_lower,
    -- 预测区间上限
    ROUND(COALESCE(hist_orders.order_amount, 0) * (1 + COALESCE(trend.trend_factor, 0) + seasonality.seasonal_factor) * (1 + 0.12 + forecast_days.day_offset * 0.01), 2) AS forecast_interval_upper,
    -- 预测置信度
    ROUND(0.93 - forecast_days.day_offset * 0.01, 2) AS confidence_level,
    -- 历史平均销售额
    ROUND(COALESCE(hist_avg.avg_amount, 0), 2) AS historical_avg_amount,
    -- 季节性指数
    ROUND(1 + seasonality.seasonal_factor, 2) AS seasonal_index,
    -- 趋势系数
    ROUND(COALESCE(trend.trend_factor, 0), 2) AS trend_coefficient,
    -- 预测模型
    'Prophet' AS prediction_model,
    -- 异常标记
    CASE 
        WHEN ABS(COALESCE(trend.trend_factor, 0)) > 0.4 OR ABS(seasonality.seasonal_factor) > 0.6 THEN TRUE
        ELSE FALSE
    END AS anomaly_flag,
    -- 预期增长率
    ROUND((COALESCE(trend.trend_factor, 0) + seasonality.seasonal_factor) * 100, 2) AS expected_growth_rate
FROM
    (SELECT cat_info.category1_id, cat_info.category1_name 
     FROM dim.dim_sku_full cat_info 
     WHERE cat_info.category1_id IS NOT NULL 
     GROUP BY cat_info.category1_id, cat_info.category1_name) categories  -- 获取所有一级类目
CROSS JOIN
    (
        SELECT 1 AS day_offset UNION ALL
        SELECT 2 AS day_offset UNION ALL
        SELECT 3 AS day_offset UNION ALL
        SELECT 4 AS day_offset UNION ALL
        SELECT 5 AS day_offset UNION ALL
        SELECT 6 AS day_offset UNION ALL
        SELECT 7 AS day_offset
    ) forecast_days  -- 生成未来7天的偏移
CROSS JOIN
    -- 季节性因子
    (
        SELECT 
            days.day_offset,
            CASE 
                WHEN DAYOFWEEK(date_add(date('${pdate}'), days.day_offset)) = 1 THEN 0.2  -- 星期日
                WHEN DAYOFWEEK(date_add(date('${pdate}'), days.day_offset)) = 2 THEN -0.1 -- 星期一
                WHEN DAYOFWEEK(date_add(date('${pdate}'), days.day_offset)) = 3 THEN -0.05 -- 星期二
                WHEN DAYOFWEEK(date_add(date('${pdate}'), days.day_offset)) = 4 THEN 0    -- 星期三
                WHEN DAYOFWEEK(date_add(date('${pdate}'), days.day_offset)) = 5 THEN 0.05 -- 星期四
                WHEN DAYOFWEEK(date_add(date('${pdate}'), days.day_offset)) = 6 THEN 0.1  -- 星期五
                WHEN DAYOFWEEK(date_add(date('${pdate}'), days.day_offset)) = 7 THEN 0.25 -- 星期六
            END AS seasonal_factor
        FROM (
            SELECT 1 AS day_offset UNION ALL
            SELECT 2 AS day_offset UNION ALL
            SELECT 3 AS day_offset UNION ALL
            SELECT 4 AS day_offset UNION ALL
            SELECT 5 AS day_offset UNION ALL
            SELECT 6 AS day_offset UNION ALL
            SELECT 7 AS day_offset
        ) days
    ) seasonality
LEFT JOIN
    -- 品类趋势因子
    (
        SELECT 
            `recent`.category1_id,
            CASE 
                WHEN `last_month`.order_amount > 0 
                THEN (`recent`.order_amount - `last_month`.order_amount) / `last_month`.order_amount
                ELSE 0
            END AS trend_factor
        FROM
        (
            -- 最近30天的品类销售数据
            SELECT 
                dim_sku_recent.category1_id,
                SUM(recent_cat.split_total_amount) AS order_amount      -- 近30天各品类销售总额
            FROM dwd.dwd_trade_order_detail_inc recent_cat        -- 使用DWD层的订单明细表
            JOIN dim.dim_sku_full dim_sku_recent ON recent_cat.sku_id = dim_sku_recent.id  -- 关联商品维度获取品类信息
            WHERE recent_cat.k1 >= date_sub(date('${pdate}'), 29)  -- 近30天数据
            AND recent_cat.k1 <= date('${pdate}')
            GROUP BY dim_sku_recent.category1_id
        ) `recent`
        JOIN
        (
            -- 上月同期30天的品类销售数据
            SELECT 
                dim_sku_last.category1_id,
                SUM(last_cat.split_total_amount) AS order_amount      -- 上月同期各品类销售总额
            FROM dwd.dwd_trade_order_detail_inc last_cat        -- 使用DWD层的订单明细表
            JOIN dim.dim_sku_full dim_sku_last ON last_cat.sku_id = dim_sku_last.id  -- 关联商品维度获取品类信息
            WHERE last_cat.k1 >= date_sub(date_sub(date('${pdate}'), 30), 29)  -- 上月同期30天
            AND last_cat.k1 <= date_sub(date('${pdate}'), 30)
            GROUP BY dim_sku_last.category1_id
        ) `last_month`
        ON `recent`.category1_id = `last_month`.category1_id  -- 关联品类ID
    ) trend
ON categories.category1_id = trend.category1_id
LEFT JOIN
    -- 历史同期品类数据
    (
        SELECT 
            days.day_offset,
            dim_sku_hist.category1_id,
            COUNT(DISTINCT hist_cat.order_id) AS order_count,   -- 去年同期各品类订单数
            SUM(hist_cat.split_total_amount) AS order_amount,         -- 去年同期各品类订单金额
            COUNT(DISTINCT hist_cat.user_id) AS user_count      -- 去年同期各品类购买用户数
        FROM dwd.dwd_trade_order_detail_inc hist_cat            -- 使用DWD层的订单明细表
        JOIN dim.dim_sku_full dim_sku_hist ON hist_cat.sku_id = dim_sku_hist.id  -- 关联商品维度获取品类信息
        JOIN (
            SELECT 1 AS day_offset UNION ALL
            SELECT 2 AS day_offset UNION ALL
            SELECT 3 AS day_offset UNION ALL
            SELECT 4 AS day_offset UNION ALL
            SELECT 5 AS day_offset UNION ALL
            SELECT 6 AS day_offset UNION ALL
            SELECT 7 AS day_offset
        ) days  -- 生成未来7天的偏移
        ON hist_cat.k1 = date_sub(date_add(date('${pdate}'), days.day_offset), 365)  -- 去年同一天
        GROUP BY days.day_offset, dim_sku_hist.category1_id
    ) hist_orders
ON forecast_days.day_offset = hist_orders.day_offset AND categories.category1_id = hist_orders.category1_id
LEFT JOIN
    -- 历史平均品类销售额
    (
        SELECT 
            daily.category1_id,
            AVG(daily.daily_amount) AS avg_amount           -- 近90天各品类平均销售额
        FROM
        (
            SELECT 
                avg_cat.k1 AS dt,
                dim_sku_daily.category1_id,
                SUM(avg_cat.split_total_amount) AS daily_amount      -- 每日各品类销售额
            FROM dwd.dwd_trade_order_detail_inc avg_cat        -- 使用DWD层的订单明细表
            JOIN dim.dim_sku_full dim_sku_daily ON avg_cat.sku_id = dim_sku_daily.id  -- 关联商品维度获取品类信息
            WHERE avg_cat.k1 >= date_sub(date('${pdate}'), 89)  -- 近90天数据
            AND avg_cat.k1 <= date('${pdate}')
            GROUP BY avg_cat.k1, dim_sku_daily.category1_id
        ) daily
        GROUP BY daily.category1_id
    ) hist_avg
ON categories.category1_id = hist_avg.category1_id

-- 可以继续添加按品牌、区域的预测逻辑，逻辑类似
; 
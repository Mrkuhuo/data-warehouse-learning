-- =================================================================
-- 表名: ads_user_value_analysis
-- 说明: 用户价值分层分析报表ETL，基于RFM模型计算用户价值
-- 数据来源: dws.dws_trade_user_order_td, dws.dws_user_user_login_td
-- 计算粒度: 用户粒度
-- 业务应用: 用户分层运营、精准营销、会员价值评估、用户画像构建
-- 更新策略: 每日全量刷新
-- 字段说明:
--   dt: 统计日期
--   user_id: 用户ID
--   order_count_td: 累计下单次数
--   order_amount_td: 累计下单金额
--   order_last_date: 最近下单日期
--   order_first_date: 首次下单日期
--   login_count_td: 累计登录次数
--   login_last_date: 最近登录日期
--   average_order_amount: 平均客单价
--   purchase_cycle_days: 平均购买周期(天)
--   account_days: 账号存续天数
--   life_time_value: 生命周期价值(LTV)
--   recency_score: 最近活跃度评分(R)
--   frequency_score: 活动频次评分(F)
--   monetary_score: 消费金额评分(M)
--   rfm_score: RFM总分
--   user_value_level: 用户价值分层
--   active_status: 活跃状态
--   life_cycle_status: 生命周期状态
--   shopping_preference: 购物偏好
--   growth_trend: 价值发展趋势
-- =================================================================

INSERT INTO ads.ads_user_value_analysis
(dt, user_id, order_count_td, order_amount_td, order_last_date, order_first_date, 
login_count_td, login_last_date, average_order_amount, purchase_cycle_days, account_days, 
life_time_value, recency_score, frequency_score, monetary_score, rfm_score, 
user_value_level, active_status, life_cycle_status, shopping_preference, growth_trend)
SELECT * FROM ads.ads_user_value_analysis
UNION
SELECT
    -- 基础日期统计
    date('${pdate}') AS dt,       -- 统计日期，使用调度日期参数
    t1.user_id AS user_id,        -- 用户ID 
    t1.order_count_td,            -- 累计下单次数，来自交易宽表
    t1.total_amount_td,           -- 累计下单金额，来自交易宽表
    -- 格式化日期为yyyy-MM-dd格式
    date_format(t1.order_last_date, '%Y-%m-%d') AS order_last_date,  -- 最近下单日期
    date_format(t1.order_first_date, '%Y-%m-%d') AS order_first_date, -- 首次下单日期
    t2.login_count_td,            -- 累计登录次数，来自用户登录宽表
    date_format(t2.login_last_date, '%Y-%m-%d') AS login_last_date,  -- 最近登录日期
    
    -- 计算衍生指标
    -- 计算平均客单价 = 总金额/订单数
    CASE WHEN t1.order_count_td > 0 THEN t1.total_amount_td/t1.order_count_td ELSE 0 END AS average_order_amount,
    -- 计算平均购买周期(天) = (最后订单日期-首次订单日期)/(订单数-1)
    CASE WHEN t1.order_count_td > 1 
         THEN datediff(t1.order_last_date, t1.order_first_date)/(t1.order_count_td-1) 
         ELSE NULL END AS purchase_cycle_days,
    -- 计算账号存续天数 = 当前日期-注册日期
    datediff(current_date(), t2.register_date) AS account_days,
    
    -- 计算生命周期价值(LTV) = 平均客单价 * 年购买频率 * 预期客户生命周期（年）
    -- 年购买频率计算方式: 总购买次数*365/账号存续天数，即年化购买频率
    -- 预期客户生命周期取3年作为默认预估
    CASE WHEN t1.order_count_td > 0 AND datediff(current_date(), t2.register_date) > 0 
         THEN (t1.total_amount_td/t1.order_count_td) * (t1.order_count_td*365/datediff(current_date(), t2.register_date)) * 3 
         ELSE 0 END AS life_time_value,
    
    -- RFM模型计算 - 为每个维度打分(1-5分)
    -- Recency(最近购买时间)评分: 越近分数越高
    CASE 
        WHEN datediff(current_date(), t1.order_last_date) <= 30 THEN 5  -- 30天内
        WHEN datediff(current_date(), t1.order_last_date) <= 60 THEN 4  -- 31-60天
        WHEN datediff(current_date(), t1.order_last_date) <= 90 THEN 3  -- 61-90天
        WHEN datediff(current_date(), t1.order_last_date) <= 180 THEN 2 -- 91-180天
        ELSE 1                                                          -- 180天以上
    END AS recency_score,
    
    -- Frequency(购买频率)评分: 购买次数越多分数越高
    CASE
        WHEN t1.order_count_td >= 20 THEN 5  -- 20次及以上
        WHEN t1.order_count_td >= 10 THEN 4  -- 10-19次
        WHEN t1.order_count_td >= 5 THEN 3   -- 5-9次
        WHEN t1.order_count_td >= 2 THEN 2   -- 2-4次
        ELSE 1                               -- 1次
    END AS frequency_score,
    
    -- Monetary(消费金额)评分: 总消费金额越高分数越高
    CASE
        WHEN t1.total_amount_td >= 10000 THEN 5  -- 1万元及以上
        WHEN t1.total_amount_td >= 5000 THEN 4   -- 5千-1万元
        WHEN t1.total_amount_td >= 2000 THEN 3   -- 2千-5千元
        WHEN t1.total_amount_td >= 500 THEN 2    -- 500-2千元
        ELSE 1                                   -- 500元以下
    END AS monetary_score,
    
    -- 计算RFM总分 = R分 + F分 + M分
    (CASE 
        WHEN datediff(current_date(), t1.order_last_date) <= 30 THEN 5
        WHEN datediff(current_date(), t1.order_last_date) <= 60 THEN 4
        WHEN datediff(current_date(), t1.order_last_date) <= 90 THEN 3
        WHEN datediff(current_date(), t1.order_last_date) <= 180 THEN 2
        ELSE 1
    END) + 
    (CASE
        WHEN t1.order_count_td >= 20 THEN 5
        WHEN t1.order_count_td >= 10 THEN 4
        WHEN t1.order_count_td >= 5 THEN 3
        WHEN t1.order_count_td >= 2 THEN 2
        ELSE 1
    END) + 
    (CASE
        WHEN t1.total_amount_td >= 10000 THEN 5
        WHEN t1.total_amount_td >= 5000 THEN 4
        WHEN t1.total_amount_td >= 2000 THEN 3
        WHEN t1.total_amount_td >= 500 THEN 2
        ELSE 1
    END) AS rfm_score,
    
    -- 用户价值分层: 根据RFM总分(3-15分)进行分层
    CASE
        WHEN (CASE 
            WHEN datediff(current_date(), t1.order_last_date) <= 30 THEN 5
            WHEN datediff(current_date(), t1.order_last_date) <= 60 THEN 4
            WHEN datediff(current_date(), t1.order_last_date) <= 90 THEN 3
            WHEN datediff(current_date(), t1.order_last_date) <= 180 THEN 2
            ELSE 1
        END + 
        CASE
            WHEN t1.order_count_td >= 20 THEN 5
            WHEN t1.order_count_td >= 10 THEN 4
            WHEN t1.order_count_td >= 5 THEN 3
            WHEN t1.order_count_td >= 2 THEN 2
            ELSE 1
        END + 
        CASE
            WHEN t1.total_amount_td >= 10000 THEN 5
            WHEN t1.total_amount_td >= 5000 THEN 4
            WHEN t1.total_amount_td >= 2000 THEN 3
            WHEN t1.total_amount_td >= 500 THEN 2
            ELSE 1
        END) >= 13 THEN '高价值'      -- 13-15分
        WHEN (CASE 
            WHEN datediff(current_date(), t1.order_last_date) <= 30 THEN 5
            WHEN datediff(current_date(), t1.order_last_date) <= 60 THEN 4
            WHEN datediff(current_date(), t1.order_last_date) <= 90 THEN 3
            WHEN datediff(current_date(), t1.order_last_date) <= 180 THEN 2
            ELSE 1
        END + 
        CASE
            WHEN t1.order_count_td >= 20 THEN 5
            WHEN t1.order_count_td >= 10 THEN 4
            WHEN t1.order_count_td >= 5 THEN 3
            WHEN t1.order_count_td >= 2 THEN 2
            ELSE 1
        END + 
        CASE
            WHEN t1.total_amount_td >= 10000 THEN 5
            WHEN t1.total_amount_td >= 5000 THEN 4
            WHEN t1.total_amount_td >= 2000 THEN 3
            WHEN t1.total_amount_td >= 500 THEN 2
            ELSE 1
        END) >= 10 THEN '中高价值'    -- 10-12分
        WHEN (CASE 
            WHEN datediff(current_date(), t1.order_last_date) <= 30 THEN 5
            WHEN datediff(current_date(), t1.order_last_date) <= 60 THEN 4
            WHEN datediff(current_date(), t1.order_last_date) <= 90 THEN 3
            WHEN datediff(current_date(), t1.order_last_date) <= 180 THEN 2
            ELSE 1
        END + 
        CASE
            WHEN t1.order_count_td >= 20 THEN 5
            WHEN t1.order_count_td >= 10 THEN 4
            WHEN t1.order_count_td >= 5 THEN 3
            WHEN t1.order_count_td >= 2 THEN 2
            ELSE 1
        END + 
        CASE
            WHEN t1.total_amount_td >= 10000 THEN 5
            WHEN t1.total_amount_td >= 5000 THEN 4
            WHEN t1.total_amount_td >= 2000 THEN 3
            WHEN t1.total_amount_td >= 500 THEN 2
            ELSE 1
        END) >= 7 THEN '中价值'       -- 7-9分
        WHEN (CASE 
            WHEN datediff(current_date(), t1.order_last_date) <= 30 THEN 5
            WHEN datediff(current_date(), t1.order_last_date) <= 60 THEN 4
            WHEN datediff(current_date(), t1.order_last_date) <= 90 THEN 3
            WHEN datediff(current_date(), t1.order_last_date) <= 180 THEN 2
            ELSE 1
        END + 
        CASE
            WHEN t1.order_count_td >= 20 THEN 5
            WHEN t1.order_count_td >= 10 THEN 4
            WHEN t1.order_count_td >= 5 THEN 3
            WHEN t1.order_count_td >= 2 THEN 2
            ELSE 1
        END + 
        CASE
            WHEN t1.total_amount_td >= 10000 THEN 5
            WHEN t1.total_amount_td >= 5000 THEN 4
            WHEN t1.total_amount_td >= 2000 THEN 3
            WHEN t1.total_amount_td >= 500 THEN 2
            ELSE 1
        END) >= 4 THEN '低价值'       -- 4-6分
        ELSE '流失风险'                                                                 -- 3分
    END AS user_value_level,
    
    -- 活跃状态: 基于最近交易和登录时间
    CASE
        WHEN datediff(current_date(), t1.order_last_date) <= 30 OR datediff(current_date(), t2.login_last_date) <= 7 THEN '活跃'   -- 30天内有交易或7天内有登录
        WHEN datediff(current_date(), t1.order_last_date) <= 90 OR datediff(current_date(), t2.login_last_date) <= 30 THEN '沉默'  -- 90天内有交易或30天内有登录
        ELSE '流失'                                                                                                               -- 超过90天未交易且超过30天未登录
    END AS active_status,
    
    -- 生命周期状态: 基于交易行为和订单历史
    CASE
        WHEN datediff(current_date(), t1.order_first_date) <= 30 AND t1.order_count_td <= 2 THEN '新用户'     -- 30天内首次购买且购买次数<=2次
        WHEN t1.order_count_td >= 3 AND (CASE 
            WHEN datediff(current_date(), t1.order_last_date) <= 30 THEN 5
            WHEN datediff(current_date(), t1.order_last_date) <= 60 THEN 4
            WHEN datediff(current_date(), t1.order_last_date) <= 90 THEN 3
            WHEN datediff(current_date(), t1.order_last_date) <= 180 THEN 2
            ELSE 1
        END) >= 4 THEN '成长期'                                      -- 购买>=3次且近期活跃(60天内)
        WHEN t1.order_count_td >= 5 AND (CASE 
            WHEN datediff(current_date(), t1.order_last_date) <= 30 THEN 5
            WHEN datediff(current_date(), t1.order_last_date) <= 60 THEN 4
            WHEN datediff(current_date(), t1.order_last_date) <= 90 THEN 3
            WHEN datediff(current_date(), t1.order_last_date) <= 180 THEN 2
            ELSE 1
        END) >= 3 THEN '成熟期'                                      -- 购买>=5次且90天内有购买
        WHEN (CASE 
            WHEN datediff(current_date(), t1.order_last_date) <= 30 THEN 5
            WHEN datediff(current_date(), t1.order_last_date) <= 60 THEN 4
            WHEN datediff(current_date(), t1.order_last_date) <= 90 THEN 3
            WHEN datediff(current_date(), t1.order_last_date) <= 180 THEN 2
            ELSE 1
        END) <= 2 AND (CASE
            WHEN t1.order_count_td >= 20 THEN 5
            WHEN t1.order_count_td >= 10 THEN 4
            WHEN t1.order_count_td >= 5 THEN 3
            WHEN t1.order_count_td >= 2 THEN 2
            ELSE 1
        END) >= 3 THEN '衰退期'                                        -- 购买次数>=5但超过90天未购买
        WHEN (CASE 
            WHEN datediff(current_date(), t1.order_last_date) <= 30 THEN 5
            WHEN datediff(current_date(), t1.order_last_date) <= 60 THEN 4
            WHEN datediff(current_date(), t1.order_last_date) <= 90 THEN 3
            WHEN datediff(current_date(), t1.order_last_date) <= 180 THEN 2
            ELSE 1
        END) >= 3 AND datediff(t1.order_last_date, t0.lag_order_date) > 90 THEN '回流'          -- 最近90天内有购买但之前超过90天未购买
        ELSE '新用户'                                                                                         -- 默认为新用户
    END AS life_cycle_status,
    
    -- 购物偏好: 基于购买频率和金额
    CASE
        WHEN (CASE
            WHEN t1.order_count_td >= 20 THEN 5
            WHEN t1.order_count_td >= 10 THEN 4
            WHEN t1.order_count_td >= 5 THEN 3
            WHEN t1.order_count_td >= 2 THEN 2
            ELSE 1
        END) >= 4 AND (CASE
            WHEN t1.total_amount_td >= 10000 THEN 5
            WHEN t1.total_amount_td >= 5000 THEN 4
            WHEN t1.total_amount_td >= 2000 THEN 3
            WHEN t1.total_amount_td >= 500 THEN 2
            ELSE 1
        END) <= 3 THEN '高频低额'    -- 高频率低金额: 购买频繁但单价较低
        WHEN (CASE
            WHEN t1.order_count_td >= 20 THEN 5
            WHEN t1.order_count_td >= 10 THEN 4
            WHEN t1.order_count_td >= 5 THEN 3
            WHEN t1.order_count_td >= 2 THEN 2
            ELSE 1
        END) <= 3 AND (CASE
            WHEN t1.total_amount_td >= 10000 THEN 5
            WHEN t1.total_amount_td >= 5000 THEN 4
            WHEN t1.total_amount_td >= 2000 THEN 3
            WHEN t1.total_amount_td >= 500 THEN 2
            ELSE 1
        END) >= 4 THEN '低频高额'    -- 低频率高金额: 购买较少但大额消费 
        WHEN (CASE
            WHEN t1.order_count_td >= 20 THEN 5
            WHEN t1.order_count_td >= 10 THEN 4
            WHEN t1.order_count_td >= 5 THEN 3
            WHEN t1.order_count_td >= 2 THEN 2
            ELSE 1
        END) >= 4 AND (CASE
            WHEN t1.total_amount_td >= 10000 THEN 5
            WHEN t1.total_amount_td >= 5000 THEN 4
            WHEN t1.total_amount_td >= 2000 THEN 3
            WHEN t1.total_amount_td >= 500 THEN 2
            ELSE 1
        END) >= 4 THEN '高频高额'    -- 高频率高金额: 高价值客户，频繁且大额
        ELSE '低频低额'                                                     -- 低频率低金额: 低价值客户
    END AS shopping_preference,
    
    -- 价值发展趋势: 比较当前RFM评分与30天前的评分
    CASE
        WHEN (CASE 
            WHEN datediff(current_date(), t1.order_last_date) <= 30 THEN 5
            WHEN datediff(current_date(), t1.order_last_date) <= 60 THEN 4
            WHEN datediff(current_date(), t1.order_last_date) <= 90 THEN 3
            WHEN datediff(current_date(), t1.order_last_date) <= 180 THEN 2
            ELSE 1
        END + 
        CASE
            WHEN t1.order_count_td >= 20 THEN 5
            WHEN t1.order_count_td >= 10 THEN 4
            WHEN t1.order_count_td >= 5 THEN 3
            WHEN t1.order_count_td >= 2 THEN 2
            ELSE 1
        END + 
        CASE
            WHEN t1.total_amount_td >= 10000 THEN 5
            WHEN t1.total_amount_td >= 5000 THEN 4
            WHEN t1.total_amount_td >= 2000 THEN 3
            WHEN t1.total_amount_td >= 500 THEN 2
            ELSE 1
        END) > COALESCE(t3.previous_rfm_score, 0) THEN '上升'    -- 当前分数高于30天前，趋势上升
        WHEN (CASE 
            WHEN datediff(current_date(), t1.order_last_date) <= 30 THEN 5
            WHEN datediff(current_date(), t1.order_last_date) <= 60 THEN 4
            WHEN datediff(current_date(), t1.order_last_date) <= 90 THEN 3
            WHEN datediff(current_date(), t1.order_last_date) <= 180 THEN 2
            ELSE 1
        END + 
        CASE
            WHEN t1.order_count_td >= 20 THEN 5
            WHEN t1.order_count_td >= 10 THEN 4
            WHEN t1.order_count_td >= 5 THEN 3
            WHEN t1.order_count_td >= 2 THEN 2
            ELSE 1
        END + 
        CASE
            WHEN t1.total_amount_td >= 10000 THEN 5
            WHEN t1.total_amount_td >= 5000 THEN 4
            WHEN t1.total_amount_td >= 2000 THEN 3
            WHEN t1.total_amount_td >= 500 THEN 2
            ELSE 1
        END) < COALESCE(t3.previous_rfm_score, 0) THEN '下降'    -- 当前分数低于30天前，趋势下降
        ELSE '稳定'                                                      -- 分数相等，趋势稳定
    END AS growth_trend
FROM
(
    -- 订单数据: 获取用户交易相关信息
    SELECT 
        user_id,
        k1,
        order_date_last,
        LAG(order_date_last, 1, NULL) OVER(PARTITION BY user_id ORDER BY k1) AS lag_order_date  -- 获取上一次最近下单日期，用于计算回流状态
    FROM dws.dws_trade_user_order_td
    WHERE k1 = date('${pdate}')  -- 取当天分区数据
) t0
JOIN
(
    -- 订单数据: 获取用户交易相关信息
    SELECT 
        user_id,
        SUM(order_count_td) AS order_count_td,           -- 累计下单次数
        SUM(total_amount_td) AS total_amount_td,         -- 累计下单金额
        MAX(order_date_last) AS order_last_date,         -- 最近下单日期
        MIN(order_date_first) AS order_first_date        -- 首次下单日期
    FROM dws.dws_trade_user_order_td
    WHERE k1 = date('${pdate}')  -- 取当天分区数据
    GROUP BY user_id
) t1
ON t0.user_id = t1.user_id
JOIN
(
    -- 登录数据: 获取用户登录相关信息
    SELECT 
        user_id,
        SUM(login_count_td) AS login_count_td,         -- 累计登录次数
        MAX(login_date_last) AS login_last_date,       -- 最近登录日期
        date('2020-01-01') AS register_date            -- 注册日期，使用默认值
    FROM dws.dws_user_user_login_td
    WHERE k1 = date('${pdate}')  -- 取当天分区数据
    GROUP BY user_id
) t2
ON t1.user_id = t2.user_id
LEFT JOIN
(
    -- 上月RFM评分数据: 用于计算价值发展趋势
    SELECT 
        user_id,
        recency_score + frequency_score + monetary_score AS previous_rfm_score  -- 30天前的RFM总分
    FROM ads.ads_user_value_analysis
    WHERE dt = date_sub(date('${pdate}'), 30)  -- 取30天前的数据
) t3
ON t1.user_id = t3.user_id; 
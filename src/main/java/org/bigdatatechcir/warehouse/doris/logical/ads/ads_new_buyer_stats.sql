-- =================================================================
-- 表名: ads_new_buyer_stats
-- 说明: 新增交易用户统计报表ETL，统计不同时间周期内的新增下单和支付用户
-- 数据来源: dws.dws_trade_user_order_td, dws.dws_trade_user_payment_td
-- 计算粒度: 时间周期(1/7/30天)
-- 业务应用: 用户增长分析、营销效果评估、获客质量分析
-- 更新策略: 每日全量刷新
-- 字段说明:
--   dt: 统计日期
--   recent_days: 最近天数(1/7/30)
--   new_order_user_count: 新增下单用户数
--   new_payment_user_count: 新增支付用户数
-- =================================================================

-- 新增交易用户统计
INSERT INTO ads.ads_new_buyer_stats(dt, recent_days, new_order_user_count, new_payment_user_count)
select * from ads.ads_new_buyer_stats
union
select
    date('${pdate}'),                   -- 统计日期
    odr.recent_days,                    -- 时间周期
    new_order_user_count,               -- 新增下单用户数
    new_payment_user_count              -- 新增支付用户数
from
    (
    -- 统计新增下单用户数: 首次下单日期在指定时间周期内的用户数
    select
    recent_days,
    sum(if(order_date_first>=date_add('2020-06-14',-recent_days+1),1,0)) new_order_user_count
    from dws.dws_trade_user_order_td lateral view explode(array(1,7,30)) tmp as recent_days  -- 展开成1天、7天、30天三行
    where k1 = date('${pdate}')         -- 取当天分区数据
    group by recent_days
    )odr
    join
    (
    -- 统计新增支付用户数: 首次支付日期在指定时间周期内的用户数
    select
    recent_days,
    sum(if(payment_date_first>=date_add('2020-06-14',-recent_days+1),1,0)) new_payment_user_count
    from dws.dws_trade_user_payment_td lateral view explode(array(1,7,30)) tmp as recent_days  -- 展开成1天、7天、30天三行
    where k1 = date('${pdate}')         -- 取当天分区数据
    group by recent_days
    )pay
on odr.recent_days=pay.recent_days;     -- 按时间周期关联下单和支付数据
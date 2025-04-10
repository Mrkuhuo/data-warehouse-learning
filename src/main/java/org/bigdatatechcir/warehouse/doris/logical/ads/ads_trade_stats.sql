-- =================================================================
-- 表名: ads_trade_stats
-- 说明: 交易综合统计报表ETL，提供不同时间周期的总体交易情况
-- 数据来源: dws.dws_trade_user_order_1d, dws.dws_trade_user_order_nd,
--           dws.dws_trade_user_order_refund_1d, dws.dws_trade_user_order_refund_nd
-- 计算粒度: 时间周期(1/7/30天)
-- 业务应用: 整体交易趋势分析、销售业绩评估、退款率监控
-- 更新策略: 每日全量刷新
-- 字段说明:
--   dt: 统计日期
--   recent_days: 最近天数(1/7/30)
--   order_total_amount: 订单总金额
--   order_count: 订单数
--   order_user_count: 下单用户数
--   order_refund_count: 退单数
--   order_refund_user_count: 退单用户数
-- =================================================================

-- 交易综合统计
INSERT INTO ads.ads_trade_stats(dt, recent_days, order_total_amount, order_count, order_user_count, order_refund_count, order_refund_user_count)
select * from ads.ads_trade_stats
union
select
    date('${pdate}'),                          -- 统计日期
    odr.recent_days,                           -- 统计周期(1/7/30天)
    order_total_amount,                        -- 订单总金额
    order_count,                               -- 订单数
    order_user_count,                          -- 下单用户数
    order_refund_count,                        -- 退单数
    order_refund_user_count                    -- 退单用户数
from
    (
    -- 1天订单统计: 获取昨日的订单金额、订单数和下单人数
    select
    1 recent_days,                             -- 1天周期
    sum(order_total_amount_1d) order_total_amount,  -- 订单总金额
    sum(order_count_1d) order_count,           -- 订单总数
    count(*) order_user_count                  -- 下单用户数
    from dws.dws_trade_user_order_1d           -- 使用DWS层的用户订单1日汇总表
    where k1=date('${pdate}')                  -- 取当天分区数据
    union all
    -- 7/30天订单统计: 获取近7天/30天的订单金额、订单数和下单人数
    select
    recent_days,                               -- 7天或30天
    sum(order_total_amount),                   -- 订单总金额
    sum(order_count),                          -- 订单总数
    sum(if(order_count>0,1,0))                 -- 有订单的用户数
    from
    (
    -- 从DWS层获取7天和30天的用户订单数据
    select
    recent_days,
    case recent_days
    when 7 then order_total_amount_7d          -- 7天订单金额
    when 30 then order_total_amount_30d        -- 30天订单金额
    end order_total_amount,
    case recent_days
    when 7 then order_count_7d                 -- 7天订单数
    when 30 then order_count_30d               -- 30天订单数
    end order_count
    from dws.dws_trade_user_order_nd lateral view explode(array(7,30)) tmp as recent_days  -- 展开成7天、30天两行
    where k1=date('${pdate}')                  -- 取当天分区数据
    )t1
    group by recent_days                       -- 按时间周期分组汇总
    )odr
    join
    (
    -- 1天退单统计: 获取昨日的退单数和退单人数
    select
    1 recent_days,                             -- 1天周期
    sum(order_refund_count_1d) order_refund_count,  -- 退单总数
    count(*) order_refund_user_count           -- 退单用户数
    from dws.dws_trade_user_order_refund_1d    -- 使用DWS层的用户退单1日汇总表
    where k1=date('${pdate}')                  -- 取当天分区数据
    union all
    -- 7/30天退单统计: 获取近7天/30天的退单数和退单人数
    select
    recent_days,                               -- 7天或30天
    sum(order_refund_count),                   -- 退单总数
    sum(if(order_refund_count>0,1,0))          -- 有退单的用户数
    from
    (
    -- 从DWS层获取7天和30天的用户退单数据
    select
    recent_days,
    case recent_days
    when 7 then order_refund_count_7d          -- 7天退单数
    when 30 then order_refund_count_30d        -- 30天退单数
    end order_refund_count
    from dws.dws_trade_user_order_refund_nd lateral view explode(array(7,30)) tmp as recent_days  -- 展开成7天、30天两行
    where k1=date('${pdate}')                  -- 取当天分区数据
    )t1
    group by recent_days                       -- 按时间周期分组汇总
    )refund
on odr.recent_days=refund.recent_days;         -- 按时间周期关联订单数据和退单数据

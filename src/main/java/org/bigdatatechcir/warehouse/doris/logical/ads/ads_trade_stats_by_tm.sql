-- =================================================================
-- 表名: ads_trade_stats_by_tm
-- 说明: 各品牌商品交易统计报表ETL，分析不同品牌的订单和退单情况
-- 数据来源: dws.dws_trade_user_sku_order_1d, dws.dws_trade_user_sku_order_nd,
--           dws.dws_trade_user_sku_order_refund_1d, dws.dws_trade_user_sku_order_refund_nd
-- 计算粒度: 品牌(商标)
-- 业务应用: 品牌销售分析、品牌合作评估、品牌退款问题分析
-- 更新策略: 每日全量刷新
-- 字段说明:
--   dt: 统计日期
--   recent_days: 最近天数(1/7/30)
--   tm_id: 品牌(商标)ID
--   tm_name: 品牌(商标)名称
--   order_count: 订单数
--   order_user_count: 下单用户数
--   order_refund_count: 退单数
--   order_refund_user_count: 退单用户数
-- =================================================================

-- 各品牌商品交易统计
INSERT INTO ads.ads_trade_stats_by_tm(dt, recent_days, tm_id, tm_name, order_count, order_user_count, order_refund_count, order_refund_user_count)
select * from ads.ads_trade_stats_by_tm
union
select
    date('${pdate}') dt,                       -- 统计日期
    nvl(odr.recent_days,refund.recent_days),   -- 统计周期(取订单或退单的周期)
    nvl(odr.tm_id,refund.tm_id),               -- 品牌ID
    nvl(odr.tm_name,refund.tm_name),           -- 品牌名称
    nvl(order_count,0),                        -- 订单数(如果为空则取0)
    nvl(order_user_count,0),                   -- 下单用户数
    nvl(order_refund_count,0),                 -- 退单数
    nvl(order_refund_user_count,0)             -- 退单用户数
from
    (
    -- 1天订单统计: 获取昨日各品牌的订单量和下单人数
    select
    1 recent_days,                             -- 1天周期
    tm_id,                                     -- 品牌ID
    tm_name,                                   -- 品牌名称
    sum(order_count_1d) order_count,           -- 订单总数
    count(distinct(user_id)) order_user_count  -- 下单用户数(去重)
    from dws.dws_trade_user_sku_order_1d       -- 使用DWS层的用户商品订单1日汇总表
    where k1 = date('${pdate}')                -- 取当前分区数据
    group by tm_id,tm_name                     -- 按品牌分组
    union all
    -- 7/30天订单统计: 获取近7天/30天各品牌的订单量和下单人数
    select
    recent_days,                               -- 7天或30天
    tm_id,                                     -- 品牌ID
    tm_name,                                   -- 品牌名称
    sum(CAST(order_count AS INT)),             -- 订单总数
    count(distinct(if(CAST(order_count AS INT)>0,user_id,null)))  -- 有订单的用户数
    from
    (
    -- 从DWS层获取7天和30天的用户商品订单数据
    select
    recent_days,
    user_id,
    tm_id,
    tm_name,
    case recent_days
    when 7 then order_count_7d                 -- 7天订单数
    when 30 then order_count_30d               -- 30天订单数
    end order_count
    from dws.dws_trade_user_sku_order_nd lateral view explode(array(7,30)) tmp as recent_days  -- 展开成7天、30天两行
    where k1 = date('${pdate}')                -- 取当前分区数据
    )t1
    group by recent_days,tm_id,tm_name         -- 按时间周期和品牌分组
    )odr
    full outer join
    (
    -- 1天退单统计: 获取昨日各品牌的退单量和退单人数
    select
    1 recent_days,                             -- 1天周期
    tm_id,                                     -- 品牌ID
    tm_name,                                   -- 品牌名称
    sum(order_refund_count_1d) order_refund_count,          -- 退单总数
    count(distinct(user_id)) order_refund_user_count        -- 退单用户数(去重)
    from dws.dws_trade_user_sku_order_refund_1d             -- 使用DWS层的用户商品退单1日汇总表
    where k1 = date('${pdate}')                             -- 取当前分区数据
    group by tm_id,tm_name                                  -- 按品牌分组
    union all
    -- 7/30天退单统计: 获取近7天/30天各品牌的退单量和退单人数
    select
    recent_days,                                            -- 7天或30天
    tm_id,                                                  -- 品牌ID
    tm_name,                                                -- 品牌名称
    sum(order_refund_count),                                -- 退单总数
    count(if(order_refund_count>0,user_id,null))            -- 有退单的用户数
    from
    (
    -- 从DWS层获取7天和30天的用户商品退单数据
    select
    recent_days,
    user_id,
    tm_id,
    tm_name,
    case recent_days
    when 7 then order_refund_count_7d                      -- 7天退单数
    when 30 then order_refund_count_30d                    -- 30天退单数
    end order_refund_count
    from dws.dws_trade_user_sku_order_refund_nd lateral view explode(array(7,30)) tmp as recent_days  -- 展开成7天、30天两行
    where k1 = date('${pdate}')                            -- 取当前分区数据
    )t1
    group by recent_days,tm_id,tm_name                     -- 按时间周期和品牌分组
    )refund
on odr.recent_days=refund.recent_days                      -- 按时间周期关联
    and odr.tm_id=refund.tm_id                             -- 按品牌ID关联
    and odr.tm_name=refund.tm_name;                        -- 按品牌名称关联
-- =================================================================
-- 表名: ads_trade_stats_by_cate
-- 说明: 各品类商品交易统计报表ETL，分析不同品类的订单和退单情况
-- 数据来源: dws.dws_trade_user_sku_order_1d, dws.dws_trade_user_sku_order_nd,
--           dws.dws_trade_user_sku_order_refund_1d, dws.dws_trade_user_sku_order_refund_nd
-- 计算粒度: 品类(一级、二级、三级类目)
-- 业务应用: 品类销售分析、商品结构优化、品类退款问题分析
-- 更新策略: 每日全量刷新
-- 字段说明:
--   dt: 统计日期
--   recent_days: 最近天数(1/7/30)
--   category1_id: 一级类目ID
--   category1_name: 一级类目名称
--   category2_id: 二级类目ID
--   category2_name: 二级类目名称
--   category3_id: 三级类目ID
--   category3_name: 三级类目名称
--   order_count: 订单数
--   order_user_count: 下单用户数
--   order_refund_count: 退单数
--   order_refund_user_count: 退单用户数
-- =================================================================

-- 各品类商品交易统计
INSERT INTO  ads.ads_trade_stats_by_cate(dt, recent_days, category1_id, category1_name, category2_id, category2_name, category3_id, category3_name, order_count, order_user_count, order_refund_count, order_refund_user_count)
select * from ads.ads_trade_stats_by_cate
union
select
    date('${pdate}') dt,                      -- 统计日期
    nvl(odr.recent_days,refund.recent_days),  -- 统计周期(取订单或退单的周期)
    nvl(odr.category1_id,refund.category1_id),    -- 一级类目ID
    nvl(odr.category1_name,refund.category1_name),-- 一级类目名称
    nvl(odr.category2_id,refund.category2_id),    -- 二级类目ID
    nvl(odr.category2_name,refund.category2_name),-- 二级类目名称
    nvl(odr.category3_id,refund.category3_id),    -- 三级类目ID
    nvl(odr.category3_name,refund.category3_name),-- 三级类目名称
    nvl(order_count,0),                       -- 订单数(如果为空则取0)
    nvl(order_user_count,0),                  -- 下单用户数
    nvl(order_refund_count,0),                -- 退单数
    nvl(order_refund_user_count,0)            -- 退单用户数
from
    (
    -- 1天订单统计: 获取昨日各品类的订单量和下单人数
    select
    1 recent_days,                           -- 1天周期
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    sum(order_count_1d) order_count,         -- 订单总数
    count(distinct(user_id)) order_user_count-- 下单用户数(去重)
    from dws.dws_trade_user_sku_order_1d     -- 使用DWS层的用户商品订单1日汇总表
    where k1 = date('${pdate}')              -- 取当前分区数据
    group by category1_id,category1_name,category2_id,category2_name,category3_id,category3_name
    union all
    -- 7/30天订单统计: 获取近7天/30天各品类的订单量和下单人数
    select
    recent_days,                             -- 7天或30天
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    sum(CAST(order_count AS INT)),           -- 订单总数
    count(distinct(if(CAST(order_count AS INT)>0,user_id,null)))  -- 有订单的用户数
    from
    (
    -- 从DWS层获取7天和30天的用户商品订单数据
    select
    recent_days,
    user_id,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    case recent_days
    when 7 then order_count_7d               -- 7天订单数
    when 30 then order_count_30d             -- 30天订单数
    end order_count
    from dws.dws_trade_user_sku_order_nd lateral view explode(array(7,30)) tmp as recent_days  -- 展开成7天、30天两行
    where k1 = date('${pdate}')              -- 取当前分区数据
    )t1
    group by recent_days,category1_id,category1_name,category2_id,category2_name,category3_id,category3_name
    )odr
    full outer join
    (
    -- 1天退单统计: 获取昨日各品类的退单量和退单人数
    select
    1 recent_days,                           -- 1天周期
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    sum(order_refund_count_1d) order_refund_count,        -- 退单总数
    count(distinct(user_id)) order_refund_user_count      -- 退单用户数(去重)
    from dws.dws_trade_user_sku_order_refund_1d           -- 使用DWS层的用户商品退单1日汇总表
    where k1 = date('${pdate}')                           -- 取当前分区数据
    group by category1_id,category1_name,category2_id,category2_name,category3_id,category3_name
    union all
    -- 7/30天退单统计: 获取近7天/30天各品类的退单量和退单人数
    select
    recent_days,                                          -- 7天或30天
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    sum(order_refund_count),                              -- 退单总数
    count(distinct(if(order_refund_count>0,user_id,null)))-- 有退单的用户数
    from
    (
    -- 从DWS层获取7天和30天的用户商品退单数据
    select
    recent_days,
    user_id,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    case recent_days
    when 7 then order_refund_count_7d                    -- 7天退单数
    when 30 then order_refund_count_30d                  -- 30天退单数
    end order_refund_count
    from dws.dws_trade_user_sku_order_refund_nd lateral view explode(array(7,30)) tmp as recent_days  -- 展开成7天、30天两行
    where k1 = date('${pdate}')                          -- 取当前分区数据
    )t1
    group by recent_days,category1_id,category1_name,category2_id,category2_name,category3_id,category3_name
    )refund
on odr.recent_days=refund.recent_days
    and odr.category1_id=refund.category1_id
    and odr.category1_name=refund.category1_name
    and odr.category2_id=refund.category2_id
    and odr.category2_name=refund.category2_name
    and odr.category3_id=refund.category3_id
    and odr.category3_name=refund.category3_name;        -- 按时间周期和品类信息进行关联
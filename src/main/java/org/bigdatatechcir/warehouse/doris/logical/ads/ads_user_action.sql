-- =================================================================
-- 表名: ads_user_action
-- 说明: 用户行为漏斗分析报表ETL，分析用户从浏览到支付的转化过程
-- 数据来源: dws.dws_traffic_page_visitor_page_view_1d, dws.dws_traffic_page_visitor_page_view_nd,
--           dws.dws_trade_user_cart_add_1d, dws.dws_trade_user_cart_add_nd,
--           dws.dws_trade_user_order_1d, dws.dws_trade_user_order_nd,
--           dws.dws_trade_user_payment_1d, dws.dws_trade_user_payment_nd
-- 计算粒度: 时间周期(1/7/30天)
-- 业务应用: 转化率分析、用户行为分析、电商漏斗优化、营销策略调整
-- 更新策略: 每日全量刷新
-- 字段说明:
--   dt: 统计日期
--   recent_days: 最近天数(1/7/30)
--   home_count: 首页访问人数
--   good_detail_count: 商品详情页访问人数
--   cart_count: 加购人数
--   order_count: 下单人数
--   payment_count: 支付人数
-- =================================================================

-- 用户行为漏斗分析
INSERT INTO ads.ads_user_action(dt, recent_days, home_count, good_detail_count, cart_count, order_count, payment_count)
select * from ads.ads_user_action
union
select
    date('${pdate}') dt,                       -- 统计日期
    page.recent_days,                          -- 统计周期(1/7/30天)
    home_count,                                -- 首页访问人数
    good_detail_count,                         -- 商品详情页访问人数
    cart_count,                                -- 加购人数
    order_count,                               -- 下单人数
    payment_count                              -- 支付人数
from
    (
    -- 1天页面访问统计: 获取昨日的首页和商品详情页访问人数
    select
    1 recent_days,                             -- 1天周期
    sum(if(page_id='home',1,0)) home_count,    -- 首页访问人数
    sum(if(page_id='good_detail',1,0)) good_detail_count  -- 商品详情页访问人数
    from dws.dws_traffic_page_visitor_page_view_1d  -- 使用DWS层的页面访问1日汇总表
    WHERE k1 = date('${pdate}')                -- 取当天分区数据
    and page_id in ('home','good_detail')      -- 只统计首页和商品详情页
    union all
    -- 7/30天页面访问统计: 获取近7天/30天的首页和商品详情页访问人数
    select
    recent_days,                               -- 7天或30天
    sum(if(page_id='home' and view_count>0,1,0)),  -- 首页访问人数
    sum(if(page_id='good_detail' and view_count>0,1,0))  -- 商品详情页访问人数
    from
    (
    -- 从DWS层获取7天和30天的页面访问数据
    select
    recent_days,
    page_id,
    case recent_days
    when 7 then view_count_7d                 -- 7天浏览次数
    when 30 then view_count_30d               -- 30天浏览次数
    end view_count
    from dws.dws_traffic_page_visitor_page_view_nd lateral view explode(array(7,30)) tmp as recent_days  -- 展开成7天、30天两行
    WHERE k1 = date('${pdate}')               -- 取当天分区数据
    and page_id in ('home','good_detail')     -- 只统计首页和商品详情页
    )t1
    group by recent_days                      -- 按时间周期分组汇总
    )page
    join
    (
    -- 1天加购统计: 获取昨日的加购人数
    select
    1 recent_days,                            -- 1天周期
    count(*) cart_count                       -- 加购人数
    from dws.dws_trade_user_cart_add_1d       -- 使用DWS层的用户加购1日汇总表
    where k1 = date('${pdate}')               -- 取当天分区数据
    union all
    -- 7/30天加购统计: 获取近7天/30天的加购人数
    select
    recent_days,                              -- 7天或30天
    sum(if(cart_count>0,1,0))                 -- 有加购行为的用户数
    from
    (
    -- 从DWS层获取7天和30天的用户加购数据
    select
    recent_days,
    case recent_days
    when 7 then cart_add_count_7d             -- 7天加购次数
    when 30 then cart_add_count_30d           -- 30天加购次数
    end cart_count
    from dws.dws_trade_user_cart_add_nd lateral view explode(array(7,30)) tmp as recent_days  -- 展开成7天、30天两行
    where k1 = date('${pdate}')               -- 取当天分区数据
    )t1
    group by recent_days                      -- 按时间周期分组汇总
    )cart
on page.recent_days=cart.recent_days           -- 按时间周期关联页面访问数据和加购数据
    join
    (
    -- 1天下单统计: 获取昨日的下单人数
    select
    1 recent_days,                            -- 1天周期
    count(*) order_count                      -- 下单人数
    from dws.dws_trade_user_order_1d          -- 使用DWS层的用户订单1日汇总表
    where k1 = date('${pdate}')               -- 取当天分区数据
    union all
    -- 7/30天下单统计: 获取近7天/30天的下单人数
    select
    recent_days,                              -- 7天或30天
    sum(if(order_count>0,1,0))                -- 有下单行为的用户数
    from
    (
    -- 从DWS层获取7天和30天的用户订单数据
    select
    recent_days,
    case recent_days
    when 7 then order_count_7d                -- 7天下单次数
    when 30 then order_count_30d              -- 30天下单次数
    end order_count
    from dws.dws_trade_user_order_nd lateral view explode(array(7,30)) tmp as recent_days  -- 展开成7天、30天两行
    where k1 = date('${pdate}')               -- 取当天分区数据
    )t1
    group by recent_days                      -- 按时间周期分组汇总
    )ord
    on page.recent_days=ord.recent_days        -- 按时间周期关联页面访问数据和订单数据
    join
    (
    -- 1天支付统计: 获取昨日的支付人数
    select
    1 recent_days,                            -- 1天周期
    count(*) payment_count                    -- 支付人数
    from dws.dws_trade_user_payment_1d        -- 使用DWS层的用户支付1日汇总表
    where k1 = date('${pdate}')               -- 取当天分区数据
    union all
    -- 7/30天支付统计: 获取近7天/30天的支付人数
    select
    recent_days,                              -- 7天或30天
    sum(if(order_count>0,1,0))                -- 有支付行为的用户数
    from
    (
    -- 从DWS层获取7天和30天的用户支付数据
    select
    recent_days,
    case recent_days
    when 7 then payment_count_7d              -- 7天支付次数
    when 30 then payment_count_30d            -- 30天支付次数
    end order_count
    from dws.dws_trade_user_payment_nd lateral view explode(array(7,30)) tmp as recent_days  -- 展开成7天、30天两行
    where k1 = date('${pdate}')               -- 取当天分区数据
    )t1
    group by recent_days                      -- 按时间周期分组汇总
    )pay
    on page.recent_days=pay.recent_days;        -- 按时间周期关联页面访问数据和支付数据
-- 用户行为漏斗分析
INSERT INTO ads.ads_user_action(dt, recent_days, home_count, good_detail_count, cart_count, order_count, payment_count)
select * from ads.ads_user_action
union
select
    date('${pdate}') dt,
    page.recent_days,
    home_count,
    good_detail_count,
    cart_count,
    order_count,
    payment_count
from
    (
    select
    1 recent_days,
    sum(if(page_id='home',1,0)) home_count,
    sum(if(page_id='good_detail',1,0)) good_detail_count
    from dws.dws_traffic_page_visitor_page_view_1d
    WHERE k1 = date('${pdate}')
    and page_id in ('home','good_detail')
    union all
    select
    recent_days,
    sum(if(page_id='home' and view_count>0,1,0)),
    sum(if(page_id='good_detail' and view_count>0,1,0))
    from
    (
    select
    recent_days,
    page_id,
    case recent_days
    when 7 then view_count_7d
    when 30 then view_count_30d
    end view_count
    from dws.dws_traffic_page_visitor_page_view_nd lateral view explode(array(7,30)) tmp as recent_days
    WHERE k1 = date('${pdate}')
    and page_id in ('home','good_detail')
    )t1
    group by recent_days
    )page
    join
    (
    select
    1 recent_days,
    count(*) cart_count
    from dws.dws_trade_user_cart_add_1d
    where k1 = date('${pdate}')
    union all
    select
    recent_days,
    sum(if(cart_count>0,1,0))
    from
    (
    select
    recent_days,
    case recent_days
    when 7 then cart_add_count_7d
    when 30 then cart_add_count_30d
    end cart_count
    from dws.dws_trade_user_cart_add_nd lateral view explode(array(7,30)) tmp as recent_days
    where k1 = date('${pdate}')
    )t1
    group by recent_days
    )cart
on page.recent_days=cart.recent_days
    join
    (
    select
    1 recent_days,
    count(*) order_count
    from dws.dws_trade_user_order_1d
    where k1 = date('${pdate}')
    union all
    select
    recent_days,
    sum(if(order_count>0,1,0))
    from
    (
    select
    recent_days,
    case recent_days
    when 7 then order_count_7d
    when 30 then order_count_30d
    end order_count
    from dws.dws_trade_user_order_nd lateral view explode(array(7,30)) tmp as recent_days
    where k1 = date('${pdate}')
    )t1
    group by recent_days
    )ord
    on page.recent_days=ord.recent_days
    join
    (
    select
    1 recent_days,
    count(*) payment_count
    from dws.dws_trade_user_payment_1d
    where k1 = date('${pdate}')
    union all
    select
    recent_days,
    sum(if(order_count>0,1,0))
    from
    (
    select
    recent_days,
    case recent_days
    when 7 then payment_count_7d
    when 30 then payment_count_30d
    end order_count
    from dws.dws_trade_user_payment_nd lateral view explode(array(7,30)) tmp as recent_days
    where k1 = date('${pdate}')
    )t1
    group by recent_days
    )pay
    on page.recent_days=pay.recent_days;
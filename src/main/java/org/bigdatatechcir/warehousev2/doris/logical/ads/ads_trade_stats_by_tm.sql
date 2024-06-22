-- 各品牌商品交易统计
INSERT INTO ads.ads_trade_stats_by_tm(dt, recent_days, tm_id, tm_name, order_count, order_user_count, order_refund_count, order_refund_user_count)
select * from ads.ads_trade_stats_by_tm
union
select
    date('${pdate}') dt,
    nvl(odr.recent_days,refund.recent_days),
    nvl(odr.tm_id,refund.tm_id),
    nvl(odr.tm_name,refund.tm_name),
    nvl(order_count,0),
    nvl(order_user_count,0),
    nvl(order_refund_count,0),
    nvl(order_refund_user_count,0)
from
    (
    select
    1 recent_days,
    tm_id,
    tm_name,
    sum(order_count_1d) order_count,
    count(distinct(user_id)) order_user_count
    from dws.dws_trade_user_sku_order_1d
    where k1 = date('${pdate}')
    group by tm_id,tm_name
    union all
    select
    recent_days,
    tm_id,
    tm_name,
    sum(CAST(order_count AS INT)),
    count(distinct(if(CAST(order_count AS INT)>0,user_id,null)))
    from
    (
    select
    recent_days,
    user_id,
    tm_id,
    tm_name,
    case recent_days
    when 7 then order_count_7d
    when 30 then order_count_30d
    end order_count
    from dws.dws_trade_user_sku_order_nd lateral view explode(array(7,30)) tmp as recent_days
    where k1 = date('${pdate}')
    )t1
    group by recent_days,tm_id,tm_name
    )odr
    full outer join
    (
    select
    1 recent_days,
    tm_id,
    tm_name,
    sum(order_refund_count_1d) order_refund_count,
    count(distinct(user_id)) order_refund_user_count
    from dws.dws_trade_user_sku_order_refund_1d
    where k1 = date('${pdate}')
    group by tm_id,tm_name
    union all
    select
    recent_days,
    tm_id,
    tm_name,
    sum(order_refund_count),
    count(if(order_refund_count>0,user_id,null))
    from
    (
    select
    recent_days,
    user_id,
    tm_id,
    tm_name,
    case recent_days
    when 7 then order_refund_count_7d
    when 30 then order_refund_count_30d
    end order_refund_count
    from dws.dws_trade_user_sku_order_refund_nd lateral view explode(array(7,30)) tmp as recent_days
    where k1 = date('${pdate}')
    )t1
    group by recent_days,tm_id,tm_name
    )refund
on odr.recent_days=refund.recent_days
    and odr.tm_id=refund.tm_id
    and odr.tm_name=refund.tm_name;
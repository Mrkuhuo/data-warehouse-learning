-- 交易综合统计
INSERT INTO ads.ads_trade_stats(dt, recent_days, order_total_amount, order_count, order_user_count, order_refund_count, order_refund_user_count)
select * from ads.ads_trade_stats
union
select
    date('${pdate}'),
    odr.recent_days,
    order_total_amount,
    order_count,
    order_user_count,
    order_refund_count,
    order_refund_user_count
from
    (
    select
    1 recent_days,
    sum(order_total_amount_1d) order_total_amount,
    sum(order_count_1d) order_count,
    count(*) order_user_count
    from dws.dws_trade_user_order_1d
    where k1=date('${pdate}')
    union all
    select
    recent_days,
    sum(order_total_amount),
    sum(order_count),
    sum(if(order_count>0,1,0))
    from
    (
    select
    recent_days,
    case recent_days
    when 7 then order_total_amount_7d
    when 30 then order_total_amount_30d
    end order_total_amount,
    case recent_days
    when 7 then order_count_7d
    when 30 then order_count_30d
    end order_count
    from dws.dws_trade_user_order_nd lateral view explode(array(7,30)) tmp as recent_days
    where k1=date('${pdate}')
    )t1
    group by recent_days
    )odr
    join
    (
    select
    1 recent_days,
    sum(order_refund_count_1d) order_refund_count,
    count(*) order_refund_user_count
    from dws.dws_trade_user_order_refund_1d
    where k1=date('${pdate}')
    union all
    select
    recent_days,
    sum(order_refund_count),
    sum(if(order_refund_count>0,1,0))
    from
    (
    select
    recent_days,
    case recent_days
    when 7 then order_refund_count_7d
    when 30 then order_refund_count_30d
    end order_refund_count
    from dws.dws_trade_user_order_refund_nd lateral view explode(array(7,30)) tmp as recent_days
    where k1=date('${pdate}')
    )t1
    group by recent_days
    )refund
on odr.recent_days=refund.recent_days;

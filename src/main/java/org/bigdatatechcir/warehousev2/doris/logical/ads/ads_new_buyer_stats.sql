-- 新增交易用户统计
INSERT INTO ads.ads_new_buyer_stats(dt, recent_days, new_order_user_count, new_payment_user_count)
select * from ads.ads_new_buyer_stats
union
select
    date('${pdate}'),
    odr.recent_days,
    new_order_user_count,
    new_payment_user_count
from
    (
    select
    recent_days,
    sum(if(order_date_first>=date_add('2020-06-14',-recent_days+1),1,0)) new_order_user_count
    from dws.dws_trade_user_order_td lateral view explode(array(1,7,30)) tmp as recent_days
    where k1 = date('${pdate}')
    group by recent_days
    )odr
    join
    (
    select
    recent_days,
    sum(if(payment_date_first>=date_add('2020-06-14',-recent_days+1),1,0)) new_payment_user_count
    from dws.dws_trade_user_payment_td lateral view explode(array(1,7,30)) tmp as recent_days
    where k1 = date('${pdate}')
    group by recent_days
    )pay
on odr.recent_days=pay.recent_days;
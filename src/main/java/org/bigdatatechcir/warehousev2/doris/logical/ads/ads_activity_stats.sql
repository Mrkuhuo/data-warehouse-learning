-- 最近30天发布的活动的补贴率
INSERT INTO ads.ads_activity_stats(dt, activity_id, activity_name, start_date, reduce_rate)
select * from ads.ads_activity_stats
union
select
    date('${pdate}') dt,
    activity_id,
    activity_name,
    start_date,
    cast(activity_reduce_amount_30d/original_amount_30d as decimal(16,2))
from dws.dws_trade_activity_order_nd
where k1=date('${pdate}');
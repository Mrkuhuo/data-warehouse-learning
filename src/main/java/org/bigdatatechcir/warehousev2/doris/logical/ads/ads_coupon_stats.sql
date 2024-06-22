-- 最近30天发布的优惠券的补贴率
INSERT INTO ads.ads_coupon_stats(dt, coupon_id, coupon_name, start_date, rule_name, reduce_rate)
select * from ads.ads_coupon_stats
union
select
    date('${pdate}') dt,
    coupon_id,
    coupon_name,
    start_date,
    coupon_rule,
    cast(coupon_reduce_amount_30d/original_amount_30d as decimal(16,2))
from dws.dws_trade_coupon_order_nd
where k1=date('${pdate}');
-- 交易域省份粒度订单最近n日汇总表
INSERT INTO dws.dws_trade_province_order_nd(province_id, k1, province_name, area_code, iso_code, iso_3166_2, order_count_7d, order_original_amount_7d, activity_reduce_amount_7d, coupon_reduce_amount_7d, order_total_amount_7d, order_count_30d, order_original_amount_30d, activity_reduce_amount_30d, coupon_reduce_amount_30d, order_total_amount_30d)
select
    province_id,
    k1,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    sum(if(k1>=date_add(date('${pdate}'),-6),order_count_1d,0)),
    sum(if(k1>=date_add(date('${pdate}'),-6),order_original_amount_1d,0)),
    sum(if(k1>=date_add(date('${pdate}'),-6),activity_reduce_amount_1d,0)),
    sum(if(k1>=date_add(date('${pdate}'),-6),coupon_reduce_amount_1d,0)),
    sum(if(k1>=date_add(date('${pdate}'),-6),order_total_amount_1d,0)),
    sum(order_count_1d),
    sum(order_original_amount_1d),
    sum(activity_reduce_amount_1d),
    sum(coupon_reduce_amount_1d),
    sum(order_total_amount_1d)
from dws.dws_trade_province_order_1d
where k1>=date_add(date('${pdate}'),-29)
  and k1<=date('${pdate}')
group by province_id,k1,province_name,area_code,iso_code,iso_3166_2;
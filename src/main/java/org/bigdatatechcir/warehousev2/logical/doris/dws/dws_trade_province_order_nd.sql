INSERT INTO dws.dws_trade_province_order_nd(province_id, k1, province_name, area_code, iso_code, iso_3166_2, order_count_7d, order_original_amount_7d, activity_reduce_amount_7d, coupon_reduce_amount_7d, order_total_amount_7d, order_count_30d, order_original_amount_30d, activity_reduce_amount_30d, coupon_reduce_amount_30d, order_total_amount_30d)
select
    province_id,
    CURRENT_DATE(),
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    sum(order_count_1d),
    sum(order_original_amount_1d),
    sum(activity_reduce_amount_1d),
    sum(coupon_reduce_amount_1d),
    sum(order_total_amount_1d),
    sum(order_count_1d),
    sum(order_original_amount_1d),
    sum(activity_reduce_amount_1d),
    sum(coupon_reduce_amount_1d),
    sum(order_total_amount_1d)
from dws.dws_trade_province_order_1d
group by province_id,province_name,area_code,iso_code,iso_3166_2;
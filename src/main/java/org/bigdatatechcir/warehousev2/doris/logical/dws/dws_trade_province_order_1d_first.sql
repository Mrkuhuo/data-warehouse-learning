-- 交易域省份粒度订单最近1日汇总表
INSERT INTO dws.dws_trade_province_order_1d(province_id, k1, province_name, area_code, iso_code, iso_3166_2, order_count_1d, order_original_amount_1d, activity_reduce_amount_1d, coupon_reduce_amount_1d, order_total_amount_1d)
select
    province_id,
    k1,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    order_count_1d,
    order_original_amount_1d,
    activity_reduce_amount_1d,
    coupon_reduce_amount_1d,
    order_total_amount_1d
from
    (
        select
            province_id,
            k1,
            count(distinct(order_id)) order_count_1d,
            sum(split_original_amount) order_original_amount_1d,
            sum(nvl(split_activity_amount,0)) activity_reduce_amount_1d,
            sum(nvl(split_coupon_amount,0)) coupon_reduce_amount_1d,
            sum(split_total_amount) order_total_amount_1d
        from dwd.dwd_trade_order_detail_inc
        group by province_id,k1
    )o
        left join
    (
        select
            id,
            province_name,
            area_code,
            iso_code,
            iso_3166_2
        from dim.dim_province_full
    )p
    on o.province_id=p.id;
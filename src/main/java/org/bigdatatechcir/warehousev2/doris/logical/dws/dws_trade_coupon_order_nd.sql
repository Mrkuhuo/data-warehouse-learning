-- 交易域优惠券粒度订单最近n日汇总表
INSERT INTO dws.dws_trade_coupon_order_nd(coupon_id, k1, coupon_name, coupon_type_code, coupon_type_name, coupon_rule, start_date, original_amount_30d, coupon_reduce_amount_30d)
select
    id,
    CURRENT_DATE(),
    coupon_name,
    coupon_type_code,
    coupon_type_name,
    benefit_rule,
    start_date,
    sum(split_original_amount),
    sum(split_coupon_amount)
from
    (
        select
            id,
            coupon_name,
            coupon_type_code,
            coupon_type_name,
            benefit_rule,
            date_format(start_time,'yyyy-MM-dd') start_date
        from dim.dim_coupon_full
    )cou
        left join
    (
        select
            coupon_id,
            order_id,
            split_original_amount,
            split_coupon_amount
        from dwd.dwd_trade_order_detail_inc
        where coupon_id is not null
    )od
    on cou.id=od.coupon_id
group by id,coupon_name,coupon_type_code,coupon_type_name,benefit_rule,start_date;
-- 交易域用户粒度订单历史至今汇总表
INSERT INTO dws.dws_trade_user_order_td(user_id, k1, order_date_first, order_date_last, order_count_td, order_num_td, original_amount_td, activity_reduce_amount_td, coupon_reduce_amount_td, total_amount_td)
select
    user_id,
    k1,
    min(k1) login_date_first,
    max(k1) login_date_last,
    sum(order_count_1d) order_count,
    sum(order_num_1d) order_num,
    sum(order_original_amount_1d) original_amount,
    sum(activity_reduce_amount_1d) activity_reduce_amount,
    sum(coupon_reduce_amount_1d) coupon_reduce_amount,
    sum(order_total_amount_1d) total_amount
from dws.dws_trade_user_order_1d
group by user_id,k1;
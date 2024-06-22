-- 交易域用户粒度订单最近1日汇总表
INSERT INTO dws.dws_trade_user_order_1d(user_id, k1, order_count_1d, order_num_1d, order_original_amount_1d, activity_reduce_amount_1d, coupon_reduce_amount_1d, order_total_amount_1d)
select
    user_id,
    k1,
    count(distinct(order_id)),
    sum(sku_num),
    sum(split_original_amount),
    sum(nvl(split_activity_amount,0)),
    sum(nvl(split_coupon_amount,0)),
    sum(split_total_amount)
from dwd.dwd_trade_order_detail_inc
where k1=date('${pdate}')
group by user_id ,k1;
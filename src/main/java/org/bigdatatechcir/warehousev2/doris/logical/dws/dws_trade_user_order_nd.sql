-- 交易域用户粒度订单最近n日汇总表
INSERT INTO dws.dws_trade_user_order_nd(user_id, k1, order_count_7d, order_num_7d, order_original_amount_7d, activity_reduce_amount_7d, coupon_reduce_amount_7d, order_total_amount_7d, order_count_30d, order_num_30d, order_original_amount_30d, activity_reduce_amount_30d, coupon_reduce_amount_30d, order_total_amount_30d)
select
    user_id,
    k1,
    sum(if(k1>=date_add(date('${pdate}'),-6),order_count_1d,0)),
    sum(if(k1>=date_add(date('${pdate}'),-6),order_num_1d,0)),
    sum(if(k1>=date_add(date('${pdate}'),-6),order_original_amount_1d,0)),
    sum(if(k1>=date_add(date('${pdate}'),-6),activity_reduce_amount_1d,0)),
    sum(if(k1>=date_add(date('${pdate}'),-6),coupon_reduce_amount_1d,0)),
    sum(if(k1>=date_add(date('${pdate}'),-6),order_total_amount_1d,0)),
    sum(order_count_1d),
    sum(order_num_1d),
    sum(order_original_amount_1d),
    sum(activity_reduce_amount_1d),
    sum(coupon_reduce_amount_1d),
    sum(order_total_amount_1d)
from dws.dws_trade_user_order_1d
where k1>=date_add(date('${pdate}'),-29)
  and k1<=date('${pdate}')
group by user_id,k1;
INSERT INTO dws.dws_trade_user_order_refund_1d(user_id, k1, order_refund_count_1d, order_refund_num_1d, order_refund_amount_1d)
select
    user_id,
    CURRENT_DATE(),
    count(*) order_refund_count,
    sum(refund_num) order_refund_num,
    sum(refund_amount) order_refund_amount
from dwd.dwd_trade_order_refund_inc
group by user_id;
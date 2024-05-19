INSERT INTO dws.dws_trade_user_payment_1d(user_id, k1, payment_count_1d, payment_num_1d, payment_amount_1d)
select
    user_id,
    CURRENT_DATE(),
    count(distinct(order_id)),
    sum(sku_num),
    sum(split_payment_amount)
from dwd_trade_pay_detail_suc_inc
group by user_id;
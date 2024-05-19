INSERT INTO dws.dws_trade_user_payment_nd(user_id, k1, payment_count_7d, payment_num_7d, payment_amount_7d, payment_count_30d, payment_num_30d, payment_amount_30d)
select user_id,
       CURRENT_DATE(),
       sum(payment_count_1d),
       sum(payment_num_1d),
       sum(payment_amount_1d),
       sum(payment_count_1d),
       sum(payment_num_1d),
       sum(payment_amount_1d)
from dws.dws_trade_user_payment_1d
group by user_id;
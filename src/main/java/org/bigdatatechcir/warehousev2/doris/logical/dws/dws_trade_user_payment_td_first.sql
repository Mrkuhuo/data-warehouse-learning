-- 交易域用户粒度支付历史至今汇总表
INSERT INTO dws.dws_trade_user_payment_td(user_id, k1, payment_date_first, payment_date_last, payment_count_td, payment_num_td, payment_amount_td)
select
    user_id,
    k1,
    min(k1) payment_date_first,
    max(k1) payment_date_last,
    sum(payment_count_1d) payment_count,
    sum(payment_num_1d) payment_num,
    sum(payment_amount_1d) payment_amount
from dws.dws_trade_user_payment_1d
group by user_id,k1;
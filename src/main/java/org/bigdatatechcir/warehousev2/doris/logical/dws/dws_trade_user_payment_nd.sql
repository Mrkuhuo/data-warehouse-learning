-- 交易域用户粒度支付最近n日汇总表
INSERT INTO dws.dws_trade_user_payment_nd(user_id, k1, payment_count_7d, payment_num_7d, payment_amount_7d, payment_count_30d, payment_num_30d, payment_amount_30d)
select user_id,
       k1,
       sum(if(k1 >= date_add(date('${pdate}'), -6), payment_count_1d, 0)),
       sum(if(k1 >= date_add(date('${pdate}'), -6), payment_num_1d, 0)),
       sum(if(k1 >= date_add(date('${pdate}'), -6), payment_amount_1d, 0)),
       sum(payment_count_1d),
       sum(payment_num_1d),
       sum(payment_amount_1d)
from dws.dws_trade_user_payment_1d
where k1>=date_add(date('${pdate}'),-29)
  and k1<=date('${pdate}')
group by user_id,k1;
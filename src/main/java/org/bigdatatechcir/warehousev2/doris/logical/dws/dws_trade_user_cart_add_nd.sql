-- 交易域用户粒度加购最近n日汇总表
INSERT INTO dws.dws_trade_user_cart_add_nd(user_id, k1, cart_add_count_7d, cart_add_num_7d, cart_add_count_30d, cart_add_num_30d)
select
    user_id,
    k1,
    sum(if(k1>=date_add(date('${pdate}'),-6),cart_add_count_1d,0)),
    sum(if(k1>=date_add(date('${pdate}'),-6),cart_add_num_1d,0)),
    sum(cart_add_count_1d),
    sum(cart_add_num_1d)
from dws.dws_trade_user_cart_add_1d
where k1>=date_add(date('${pdate}'),-29)
  and k1<=date('${pdate}')
group by user_id,k1;
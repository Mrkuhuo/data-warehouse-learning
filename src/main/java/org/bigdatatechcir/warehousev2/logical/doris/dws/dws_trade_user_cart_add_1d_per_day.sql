INSERT INTO  dws.dws_trade_user_cart_add_1d(user_id, k1, cart_add_count_1d, cart_add_num_1d)
select
    user_id,
    CURRENT_DATE(),
    count(*),
    sum(sku_num)
from dwd.dwd_trade_cart_add_inc
group by user_id;
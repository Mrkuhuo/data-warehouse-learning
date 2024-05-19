INSERT INTO dws.dws_trade_user_cart_add_nd(user_id, k1, cart_add_count_7d, cart_add_num_7d, cart_add_count_30d, cart_add_num_30d)
select
    user_id,
    CURRENT_DATE(),
    sum(cart_add_count_1d ),
    sum(cart_add_num_1d ),
    sum(cart_add_count_1d),
    sum(cart_add_num_1d)
from dws.dws_trade_user_cart_add_1d
group by user_id;
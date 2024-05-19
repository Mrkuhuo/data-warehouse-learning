INSERT INTO dwd.dwd_trade_cart_full(id, user_id, sku_id, sku_name, sku_num)
select
    id,
    current_date() as k1,
    user_id,
    sku_id,
    sku_name,
    sku_num
from ods_cart_info_full
where is_ordered='0';
-- 交易域购物车周期快照事实表
INSERT INTO dwd.dwd_trade_cart_full(id, k1, user_id, sku_id, sku_name, sku_num)
select
    id,
    k1,
    user_id,
    sku_id,
    sku_name,
    sku_num
from ods.ods_cart_info_full
where is_ordered='0'
  and k1=date('${pdate}');
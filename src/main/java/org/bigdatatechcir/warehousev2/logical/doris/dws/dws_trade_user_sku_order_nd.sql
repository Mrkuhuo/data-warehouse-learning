INSERT INTO dws.dws_trade_user_sku_order_nd(user_id, sku_id, k1, sku_name, category1_id, category1_name, category2_id, category2_name, category3_id, category3_name, tm_id, tm_name, order_count_7d, order_num_7d, order_original_amount_7d, activity_reduce_amount_7d, coupon_reduce_amount_7d, order_total_amount_7d, order_count_30d, order_num_30d, order_original_amount_30d, activity_reduce_amount_30d, coupon_reduce_amount_30d, order_total_amount_30d)
select
    user_id,
    sku_id,
    CURRENT_DATE(),
    sku_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    tm_id,
    tm_name,
    sum(order_count_1d),
    sum(order_num_1d),
    sum(order_original_amount_1d),
    sum(activity_reduce_amount_1d),
    sum(coupon_reduce_amount_1d),
    sum(order_total_amount_1d),
    sum(order_count_1d),
    sum(order_num_1d),
    sum(order_original_amount_1d),
    sum(activity_reduce_amount_1d),
    sum(coupon_reduce_amount_1d),
    sum(order_total_amount_1d)
from dws.dws_trade_user_sku_order_1d
group by  user_id,sku_id,sku_name,category1_id,category1_name,category2_id,category2_name,category3_id,category3_name,tm_id,tm_name;
-- 交易域用户商品粒度退单最近n日汇总表
INSERT INTO dws.dws_trade_user_sku_order_refund_nd(user_id, sku_id, k1, sku_name, category1_id, category1_name, category2_id, category2_name, category3_id, category3_name, tm_id, tm_name, order_refund_count_7d, order_refund_num_7d, order_refund_amount_7d, order_refund_count_30d, order_refund_num_30d, order_refund_amount_30d)
select
    user_id,
    sku_id,
    k1,
    sku_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    tm_id,
    tm_name,
    sum(if(k1>=date_add(date('${pdate}'),-6),order_refund_count_1d,0)),
    sum(if(k1>=date_add(date('${pdate}'),-6),order_refund_num_1d,0)),
    sum(if(k1>=date_add(date('${pdate}'),-6),order_refund_amount_1d,0)),
    sum(order_refund_count_1d),
    sum(order_refund_num_1d),
    sum(order_refund_amount_1d)
from dws.dws_trade_user_sku_order_refund_1d
where k1>=date_add(date('${pdate}'),-29)
  and k1<=date('${pdate}')
group by user_id,sku_id,k1,sku_name,category1_id,category1_name,category2_id,category2_name,category3_id,category3_name,tm_id,tm_name;
-- 交易域用户商品粒度退单最近1日汇总表
INSERT INTO dws.dws_trade_user_sku_order_refund_1d(user_id, sku_id, k1, sku_name, category1_id, category1_name, category2_id, category2_name, category3_id, category3_name, tm_id, tm_name, order_refund_count_1d, order_refund_num_1d, order_refund_amount_1d)
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
    order_refund_count,
    order_refund_num,
    order_refund_amount
from
    (
        select
            user_id,
            sku_id,
            k1,
            count(*) order_refund_count,
            sum(refund_num) order_refund_num,
            sum(refund_amount) order_refund_amount
        from dwd.dwd_trade_order_refund_inc
        where k1=date('${pdate}')
group by user_id,sku_id,k1
    )od
        left join
    (
        select
            id,
            sku_name,
            category1_id,
            category1_name,
            category2_id,
            category2_name,
            category3_id,
            category3_name,
            tm_id,
            tm_name
        from dim.dim_sku_full
    )sku
on od.sku_id=sku.id;
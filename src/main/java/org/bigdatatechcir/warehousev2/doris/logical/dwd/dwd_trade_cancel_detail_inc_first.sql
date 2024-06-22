-- 交易域取消订单事务事实表
INSERT INTO dwd.dwd_trade_cancel_detail_inc(id, k1, order_id, user_id, sku_id, province_id, activity_id, activity_rule_id, coupon_id, date_id, cancel_time, source_id, source_type_code, source_type_name, sku_num, split_original_amount, split_activity_amount, split_coupon_amount, split_total_amount)
select
    od.id,
    k1,
    order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    date_format(canel_time,'yyyy-MM-dd') date_id,
    canel_time,
    source_id,
    source_type,
    dic_name,
    sku_num,
    split_original_amount,
    split_activity_amount,
    split_coupon_amount,
    split_total_amount
from
    (
        select
            id,
            k1,
            order_id,
            sku_id,
            source_id,
            source_type,
            sku_num,
            sku_num * order_price split_original_amount,
            split_total_amount,
            split_activity_amount,
            split_coupon_amount
        from ods.ods_order_detail_inc
    ) od
        join
    (
        select
            id,
            user_id,
            province_id,
            operate_time canel_time
        from ods.ods_order_info_inc
        where order_status='1003'
    ) oi
    on od.order_id = oi.id
        left join
    (
        select
            order_detail_id,
            activity_id,
            activity_rule_id
        from ods.ods_order_detail_activity_inc
    ) act
    on od.id = act.order_detail_id
        left join
    (
        select
            order_detail_id,
            coupon_id
        from ods.ods_order_detail_coupon_inc
    ) cou
    on od.id = cou.order_detail_id
        left join
    (
        select
            dic_code,
            dic_name
        from ods.ods_base_dic_full
        where parent_code='24'
    )dic
    on od.source_type=dic.dic_code;
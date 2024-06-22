-- 交易域支付成功事务事实表
INSERT INTO dwd.dwd_trade_pay_detail_suc_inc(id, k1, order_id, user_id, sku_id, province_id, activity_id, activity_rule_id, coupon_id, payment_type_code, payment_type_name, date_id, callback_time, source_id, source_type_code, source_type_name, sku_num, split_original_amount, split_activity_amount, split_coupon_amount,split_payment_amount)
select
    od.id,
    k1,
    od.order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    payment_type,
    pay_dic.dic_name,
    date_format(callback_time,'yyyy-MM-dd') date_id,
    callback_time,
    source_id,
    source_type,
    src_dic.dic_name,
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
        where k1=date('${pdate}')
    ) od
        join
    (
        select
            user_id,
            order_id,
            payment_type,
            callback_time
        from ods.ods_payment_info_inc
        where payment_status='1602'
        and k1=date('${pdate}')
    ) pi
on od.order_id=pi.order_id
    left join
    (
    select
    id,
    province_id
    from ods.ods_order_info_inc
    where k1=date('${pdate}')
    ) oi
    on od.order_id = oi.id
    left join
    (
    select
    order_detail_id,
    activity_id,
    activity_rule_id
    from ods.ods_order_detail_activity_inc
    where k1=date('${pdate}')
    ) act
    on od.id = act.order_detail_id
    left join
    (
    select
    order_detail_id,
    coupon_id
    from ods.ods_order_detail_coupon_inc
    where k1=date('${pdate}')
    ) cou
    on od.id = cou.order_detail_id
    left join
    (
    select
    dic_code,
    dic_name
    from ods.ods_base_dic_full
    where k1=date('${pdate}')
    and parent_code='11'
    ) pay_dic
    on pi.payment_type=pay_dic.dic_code
    left join
    (
    select
    dic_code,
    dic_name
    from ods.ods_base_dic_full
    where parent_code='24'
    and k1=date('${pdate}')
    )src_dic
    on od.source_type=src_dic.dic_code;
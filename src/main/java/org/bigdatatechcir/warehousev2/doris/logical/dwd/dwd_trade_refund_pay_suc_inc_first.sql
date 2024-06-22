-- 交易域退款成功事务事实表
INSERT INTO dwd.dwd_trade_refund_pay_suc_inc(id, k1, user_id, order_id, sku_id, province_id, payment_type_code, payment_type_name, date_id, callback_time, refund_num, refund_amount)
select
    rp.id,
    k1,
    user_id,
    rp.order_id,
    rp.sku_id,
    province_id,
    payment_type,
    dic_name,
    date_format(callback_time,'yyyy-MM-dd') date_id,
    callback_time,
    refund_num,
    total_amount
from
    (
        select
            id,
            k1,
            order_id,
            sku_id,
            payment_type,
            callback_time,
            total_amount
        from ods.ods_refund_payment_inc
        where refund_status='1602'
    )rp
        left join
    (
        select
            id,
            user_id,
            province_id
        from ods.ods_order_info_inc
    )oi
    on rp.order_id=oi.id
        left join
    (
        select
            order_id,
            sku_id,
            refund_num
        from ods.ods_order_refund_info_inc
    )ri
    on rp.order_id=ri.order_id
        and rp.sku_id=ri.sku_id
        left join
    (
        select
            dic_code,
            dic_name
        from ods.ods_base_dic_full
        where parent_code='11'
    )dic
    on rp.payment_type=dic.dic_code;
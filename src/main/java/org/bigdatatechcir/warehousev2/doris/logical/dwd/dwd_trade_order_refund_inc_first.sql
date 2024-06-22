-- 交易域退单事务事实表
INSERT INTO dwd.dwd_trade_order_refund_inc(id, k1, user_id, order_id, sku_id, province_id, date_id, create_time, refund_type_code, refund_type_name, refund_reason_type_code, refund_reason_type_name, refund_reason_txt, refund_num, refund_amount)
select
    ri.id,
    k1,
    user_id,
    order_id,
    sku_id,
    province_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time,
    refund_type,
    type_dic.dic_name,
    refund_reason_type,
    reason_dic.dic_name,
    refund_reason_txt,
    refund_num,
    refund_amount
from
    (
        select
            id,
            k1,
            user_id,
            order_id,
            sku_id,
            refund_type,
            refund_num,
            refund_amount,
            refund_reason_type,
            refund_reason_txt,
            create_time
        from ods.ods_order_refund_info_inc
    )ri
        left join
    (
        select
            id,
            province_id
        from ods.ods_order_info_inc
    )oi
    on ri.order_id=oi.id
        left join
    (
        select
            dic_code,
            dic_name
        from ods.ods_base_dic_full
        where parent_code = '15'
    )type_dic
    on ri.refund_type=type_dic.dic_code
        left join
    (
        select
            dic_code,
            dic_name
        from ods.ods_base_dic_full
        where  parent_code = '13'
    )reason_dic
    on ri.refund_reason_type=reason_dic.dic_code;
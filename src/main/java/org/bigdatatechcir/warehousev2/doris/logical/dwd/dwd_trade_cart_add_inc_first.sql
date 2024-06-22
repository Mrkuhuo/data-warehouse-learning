-- 交易域加购事务事实表
INSERT INTO dwd.dwd_trade_cart_add_inc(id, k1, user_id, sku_id, date_id, create_time, source_id, source_type_code, source_type_name, sku_num)
select
    id,
    current_date() as k1,
    user_id,
    sku_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time,
    source_id,
    source_type,
    dic.dic_name,
    sku_num
from
    (
        select
            id,
            user_id,
            sku_id,
            create_time,
            source_id,
            source_type,
            sku_num
        from ods.ods_cart_info_inc
    )ci
        left join
    (
        select
            dic_code,
            dic_name
        from ods.ods_base_dic_full
        where parent_code='24'
    )dic
    on ci.source_type=dic.dic_code;
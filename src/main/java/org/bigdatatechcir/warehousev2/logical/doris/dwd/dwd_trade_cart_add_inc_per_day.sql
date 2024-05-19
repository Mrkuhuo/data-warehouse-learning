INSERT INTO dwd.dwd_trade_cart_add_inc(id, k1, user_id, sku_id, date_id, create_time, source_id, source_type_code, source_type_name, sku_num)
select
    id,
    current_date() as k1,
    user_id,
    sku_id,
    date_id,
    create_time,
    source_id,
    source_type_code,
    source_type_name,
    sku_num
from
    (
        select
            id,
            user_id,
            sku_id,
            date_format(create_time,'yyyy-MM-dd') date_id,
            date_format(create_time,'yyyy-MM-dd HH:mm:ss') create_time,
            source_id,
            source_type source_type_code,
            sku_num
        from ods_cart_info_inc
    )cart
        left join
    (
        select
            dic_code,
            dic_name source_type_name
        from ods_base_dic_full
        where parent_code='24'
    )dic
    on cart.source_type_code=dic.dic_code;
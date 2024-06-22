-- 互动域评价事务事实表
INSERT INTO dwd.dwd_interaction_comment_inc(id, k1, user_id, sku_id, order_id, date_id, create_time, appraise_code, appraise_name)
select
    id,
    k1,
    user_id,
    sku_id,
    order_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time,
    appraise,
    dic_name
from
    (
        select
            id,
            k1,
            user_id,
            sku_id,
            order_id,
            create_time,
            appraise
        from ods.ods_comment_info_inc
    )ci
        left join
    (
        select
            dic_code,
            dic_name
        from ods.ods_base_dic_full
    )dic
    on ci.appraise=dic.dic_code;
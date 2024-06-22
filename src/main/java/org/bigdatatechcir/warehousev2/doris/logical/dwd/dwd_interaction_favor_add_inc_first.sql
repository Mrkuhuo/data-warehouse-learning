-- 互动域收藏商品事务事实表
INSERT INTO dwd.dwd_interaction_favor_add_inc(id, k1, user_id, sku_id, date_id, create_time)
select
    id,
    k1,
    user_id,
    sku_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time
from ods.ods_favor_info_inc
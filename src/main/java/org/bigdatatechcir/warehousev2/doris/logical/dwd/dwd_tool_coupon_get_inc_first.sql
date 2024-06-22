-- 工具域优惠券领取事务事实表
INSERT INTO dwd.dwd_tool_coupon_get_inc(id, k1, coupon_id, user_id, date_id, get_time)
select
    id,
    k1,
    coupon_id,
    user_id,
    date_format(get_time,'yyyy-MM-dd') date_id,
    get_time
from ods.ods_coupon_use_inc
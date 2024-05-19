INSERT INTO  dwd.dwd_tool_coupon_order_inc(id, k1, coupon_id, user_id, order_id, date_id, order_time)
select
    id,
    current_date() as k1,
    coupon_id,
    user_id,
    order_id,
    date_format(using_time,'yyyy-MM-dd') date_id,
    using_time
from ods_coupon_use_inc
where using_time is not null;
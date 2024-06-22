-- 工具域优惠券使用(支付)事务事实表
INSERT INTO dwd.dwd_tool_coupon_pay_inc(id, k1, coupon_id, user_id, order_id, date_id, payment_time)
select
    id,
    k1,
    coupon_id,
    user_id,
    order_id,
    date_format(used_time,'yyyy-MM-dd') date_id,
    used_time
from ods.ods_coupon_use_inc
where used_time is not null;
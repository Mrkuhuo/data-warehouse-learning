-- 交易域活动粒度订单最近n日汇总表
INSERT INTO dws.dws_trade_activity_order_nd(activity_id, k1, activity_name, activity_type_code, activity_type_name, start_date, original_amount_30d, activity_reduce_amount_30d)
select
    act.activity_id,
    CURRENT_DATE(),
    activity_name,
    activity_type_code,
    activity_type_name,
    date_format(start_time,'yyyy-MM-dd'),
    sum(split_original_amount),
    sum(split_activity_amount)
from
    (
        select
            activity_id,
            activity_name,
            activity_type_code,
            activity_type_name,
            start_time
        from dim.dim_activity_full
        group by activity_id, activity_name, activity_type_code, activity_type_name,start_time
    )act
        left join
    (
        select
            activity_id,
            order_id,
            split_original_amount,
            split_activity_amount
        from dwd.dwd_trade_order_detail_inc
        where activity_id is not null
    )od
    on act.activity_id=od.activity_id
group by act.activity_id,activity_name,activity_type_code,activity_type_name,start_time;
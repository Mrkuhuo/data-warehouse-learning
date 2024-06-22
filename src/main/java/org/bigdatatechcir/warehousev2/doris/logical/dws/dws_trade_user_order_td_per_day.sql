-- 交易域用户粒度订单历史至今汇总表
INSERT INTO dws.dws_trade_user_order_td(user_id, k1, order_date_first, order_date_last, order_count_td, order_num_td, original_amount_td, activity_reduce_amount_td, coupon_reduce_amount_td, total_amount_td)
select
    nvl(old.user_id,new.user_id),
    k1,
    if(new.user_id is not null and old.user_id is null,date('${pdate}'),old.order_date_first),
    if(new.user_id is not null,date('${pdate}'),old.order_date_last),
    nvl(old.order_count_td,0)+nvl(new.order_count_1d,0),
    nvl(old.order_num_td,0)+nvl(new.order_num_1d,0),
    nvl(old.original_amount_td,0)+nvl(new.order_original_amount_1d,0),
    nvl(old.activity_reduce_amount_td,0)+nvl(new.activity_reduce_amount_1d,0),
    nvl(old.coupon_reduce_amount_td,0)+nvl(new.coupon_reduce_amount_1d,0),
    nvl(old.total_amount_td,0)+nvl(new.order_total_amount_1d,0)
from
    (
        select
            user_id,
            k1,
            order_date_first,
            order_date_last,
            order_count_td,
            order_num_td,
            original_amount_td,
            activity_reduce_amount_td,
            coupon_reduce_amount_td,
            total_amount_td
        from dws.dws_trade_user_order_td
        where k1=date_add(date('${pdate}'),-1)
    )old
        full outer join
    (
        select
            user_id,
            order_count_1d,
            order_num_1d,
            order_original_amount_1d,
            activity_reduce_amount_1d,
            coupon_reduce_amount_1d,
            order_total_amount_1d
        from dws.dws_trade_user_order_1d
        where k1=date('${pdate}')
    )new
on old.user_id=new.user_id;
-- 交易域用户粒度支付历史至今汇总表
INSERT INTO dws.dws_trade_user_payment_td(user_id, k1, payment_date_first, payment_date_last, payment_count_td, payment_num_td, payment_amount_td)
select
    nvl(old.user_id,new.user_id),
    k1,
    if(old.user_id is null and new.user_id is not null,date('${pdate}'),old.payment_date_first),
    if(new.user_id is not null,date('${pdate}'),old.payment_date_last),
    nvl(old.payment_count_td,0)+nvl(new.payment_count_1d,0),
    nvl(old.payment_num_td,0)+nvl(new.payment_num_1d,0),
    nvl(old.payment_amount_td,0)+nvl(new.payment_amount_1d,0)
from
    (
        select
            user_id,
            k1,
            payment_date_first,
            payment_date_last,
            payment_count_td,
            payment_num_td,
            payment_amount_td
        from dws.dws_trade_user_payment_td
        where k1=date_add(date('${pdate}'),-1)
    )old
        full outer join
    (
        select
            user_id,
            payment_count_1d,
            payment_num_1d,
            payment_amount_1d
        from dws.dws_trade_user_payment_1d
        where k1=date('${pdate}')
    )new
on old.user_id=new.user_id;
-- 最近7/30日各品牌复购率
INSERT INTO ads.ads_repeat_purchase_by_tm(dt, recent_days, tm_id, tm_name, order_repeat_rate)
select * from ads.ads_repeat_purchase_by_tm
union
select
    date('${pdate}') dt,
    recent_days,
    tm_id,
    tm_name,
    cast(sum(if(order_count>=2,1,0))/sum(if(order_count>=1,1,0)) as decimal(16,2))
from
    (
    select
    date('${pdate}') dt,
    recent_days,
    user_id,
    tm_id,
    tm_name,
    sum(CAST(order_count as int)) order_count
    from
    (
    select
    recent_days,
    user_id,
    tm_id,
    tm_name,
    case recent_days
    when 7 then order_count_7d
    when 30 then order_count_30d
    end order_count
    from dws.dws_trade_user_sku_order_nd lateral view explode(array(7,30)) tmp as recent_days
    where k1 = date('${pdate}')
    )t1
    group by recent_days,user_id,tm_id,tm_name
    )t2
group by recent_days,tm_id,tm_name;
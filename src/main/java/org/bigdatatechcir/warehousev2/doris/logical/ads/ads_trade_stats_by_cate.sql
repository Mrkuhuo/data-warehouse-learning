-- 各品类商品交易统计
INSERT INTO  ads.ads_trade_stats_by_cate(dt, recent_days, category1_id, category1_name, category2_id, category2_name, category3_id, category3_name, order_count, order_user_count, order_refund_count, order_refund_user_count)
select * from ads.ads_trade_stats_by_cate
union
select
    date('${pdate}') dt,
    nvl(odr.recent_days,refund.recent_days),
    nvl(odr.category1_id,refund.category1_id),
    nvl(odr.category1_name,refund.category1_name),
    nvl(odr.category2_id,refund.category2_id),
    nvl(odr.category2_name,refund.category2_name),
    nvl(odr.category3_id,refund.category3_id),
    nvl(odr.category3_name,refund.category3_name),
    nvl(order_count,0),
    nvl(order_user_count,0),
    nvl(order_refund_count,0),
    nvl(order_refund_user_count,0)
from
    (
    select
    1 recent_days,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    sum(order_count_1d) order_count,
    count(distinct(user_id)) order_user_count
    from dws.dws_trade_user_sku_order_1d
    where k1 = date('${pdate}')
    group by category1_id,category1_name,category2_id,category2_name,category3_id,category3_name
    union all
    select
    recent_days,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    sum(CAST(order_count AS INT)),
    count(distinct(if(CAST(order_count AS INT)>0,user_id,null)))
    from
    (
    select
    recent_days,
    user_id,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    case recent_days
    when 7 then order_count_7d
    when 30 then order_count_30d
    end order_count
    from dws.dws_trade_user_sku_order_nd lateral view explode(array(7,30)) tmp as recent_days
    where k1 = date('${pdate}')
    )t1
    group by recent_days,category1_id,category1_name,category2_id,category2_name,category3_id,category3_name
    )odr
    full outer join
    (
    select
    1 recent_days,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    sum(order_refund_count_1d) order_refund_count,
    count(distinct(user_id)) order_refund_user_count
    from dws.dws_trade_user_sku_order_refund_1d
    where k1 = date('${pdate}')
    group by category1_id,category1_name,category2_id,category2_name,category3_id,category3_name
    union all
    select
    recent_days,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    sum(order_refund_count),
    count(distinct(if(order_refund_count>0,user_id,null)))
    from
    (
    select
    recent_days,
    user_id,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    case recent_days
    when 7 then order_refund_count_7d
    when 30 then order_refund_count_30d
    end order_refund_count
    from dws.dws_trade_user_sku_order_refund_nd lateral view explode(array(7,30)) tmp as recent_days
    where k1 = date('${pdate}')
    )t1
    group by recent_days,category1_id,category1_name,category2_id,category2_name,category3_id,category3_name
    )refund
on odr.recent_days=refund.recent_days
    and odr.category1_id=refund.category1_id
    and odr.category1_name=refund.category1_name
    and odr.category2_id=refund.category2_id
    and odr.category2_name=refund.category2_name
    and odr.category3_id=refund.category3_id
    and odr.category3_name=refund.category3_name;
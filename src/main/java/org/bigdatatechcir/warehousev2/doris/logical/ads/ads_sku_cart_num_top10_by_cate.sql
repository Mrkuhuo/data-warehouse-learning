--  各分类商品购物车存量Top10
INSERT INTO ads.ads_sku_cart_num_top10_by_cate(dt, category1_id, category1_name, category2_id, category2_name, category3_id, category3_name, sku_id, sku_name, cart_num, rk)
select * from ads.ads_sku_cart_num_top10_by_cate
union
select
    date('${pdate}') dt,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    sku_id,
    sku_name,
    cart_num,
    rk
from
    (
    select
    sku_id,
    sku_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    cart_num,
    rank() over (partition by category1_id,category2_id,category3_id order by cart_num desc) rk
    from
    (
    select
    sku_id,
    sum(sku_num) cart_num
    from dwd.dwd_trade_cart_full
    where k1 = date('${pdate}')
    group by sku_id
    )cart
    left join
    (
    select
    id,
    sku_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name
    from dim.dim_sku_full
    )sku
    on cart.sku_id=sku.id
    )t1
where rk<=3;
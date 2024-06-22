-- 商品维度表
insert into dim.dim_sku_full(id, k1, price, sku_name, sku_desc, weight, is_sale, spu_id, spu_name, category3_id, category3_name, category2_id, category2_name, category1_id, category1_name, tm_id, tm_name, attr_ids, sale_attr_ids, create_time)
with
    sku as
        (
            select
                id,
                k1,
                price,
                sku_name,
                sku_desc,
                weight,
                is_sale,
                spu_id,
                category3_id,
                tm_id,
                create_time
            from ods.ods_sku_info_full
            where k1=date('${pdate}')
    ),
    spu as
   (
select
    id,
    spu_name
from ods.ods_spu_info_full
    ),
    c3 as
    (
select
    id,
    name,
    category2_id
from ods.ods_base_category3_full
    ),
    c2 as
    (
select
    id,
    name,
    category1_id
from ods.ods_base_category2_full
    ),
    c1 as
    (
select
    id,
    name
from ods.ods_base_category1_full
    ),
    tm as
    (
select
    id,
    tm_name
from ods.ods_base_trademark_full
    ),
    attr as
    (
select
    sku_id,
    collect_set(id) as ids
from ods.ods_sku_attr_value_full
group by sku_id
    ),
    sale_attr as
    (
select
    sku_id,
    collect_set(id) as ids
from ods.ods_sku_sale_attr_value_full
group by sku_id
    )
select
    sku.id,
    sku.k1,
    sku.price,
    sku.sku_name,
    sku.sku_desc,
    sku.weight,
    sku.is_sale,
    sku.spu_id,
    spu.spu_name,
    sku.category3_id,
    c3.name,
    c3.category2_id,
    c2.name,
    c2.category1_id,
    c1.name,
    sku.tm_id,
    tm.tm_name,
    attr.ids attr_ids,
    sale_attr.ids sale_attr_ids,
    sku.create_time
from sku
         left join spu on sku.spu_id=spu.id
         left join c3 on sku.category3_id=c3.id
         left join c2 on c3.category2_id=c2.id
         left join c1 on c2.category1_id=c1.id
         left join tm on sku.tm_id=tm.id
         left join attr on sku.id=attr.sku_id
         left join sale_attr on sku.id=sale_attr.sku_id;
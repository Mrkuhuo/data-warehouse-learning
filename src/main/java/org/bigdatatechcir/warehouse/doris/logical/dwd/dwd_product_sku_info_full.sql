-- 商品域SKU商品全量表
INSERT INTO dwd.dwd_product_sku_info_full(id, k1, sku_id, spu_id, price, sku_name, sku_desc, weight, tm_id, tm_name, category1_id, category1_name, category2_id, category2_name, category3_id, category3_name, default_img, create_time, attr_values, sale_attr_values, sku_cost)
with
    sku as
    (
        select
            id,
            k1,
            spu_id,
            price,
            sku_name,
            sku_desc,
            weight,
            tm_id,
            category3_id,
            sku_default_img as default_img,
            create_time
        from ods.ods_sku_info_full
        where k1=date('${pdate}')
    ),
    tm as
    (
        select
            id,
            tm_name
        from ods.ods_base_trademark_full
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
    attr as
    (
        select
            sku_id,
            GROUP_CONCAT(CONCAT(attr_name, ':', value_name), ';') as attr_values
        from ods.ods_sku_attr_value_full
        group by sku_id
    ),
    sale_attr as
    (
        select
            sku_id,
            GROUP_CONCAT(CONCAT(sale_attr_name, ':', sale_attr_value_name), ';') as sale_attr_values
        from ods.ods_sku_sale_attr_value_full
        group by sku_id
    ),
    cost as
    (
        select
            sku_id,
            sku_cost
        from ods.ods_financial_sku_cost_full
        where k1=date('${pdate}')
    )
select
    sku.id,
    sku.k1,
    sku.id as sku_id,
    sku.spu_id,
    sku.price,
    sku.sku_name,
    sku.sku_desc,
    sku.weight,
    sku.tm_id,
    tm.tm_name,
    c1.id as category1_id,
    c1.name as category1_name,
    c2.id as category2_id,
    c2.name as category2_name,
    c3.id as category3_id,
    c3.name as category3_name,
    sku.default_img,
    sku.create_time,
    attr.attr_values,
    sale_attr.sale_attr_values,
    cost.sku_cost
from sku
    left join tm on sku.tm_id = tm.id
    left join c3 on sku.category3_id = c3.id
    left join c2 on c3.category2_id = c2.id
    left join c1 on c2.category1_id = c1.id
    left join attr on sku.id = attr.sku_id
    left join sale_attr on sku.id = sale_attr.sku_id
    left join cost on sku.id = cost.sku_id; 
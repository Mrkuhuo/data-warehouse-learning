-- 商品域SPU商品全量表
INSERT INTO dwd.dwd_product_spu_info_full(id, k1, spu_id, spu_name, description, category3_id, category3_name, category2_id, category2_name, category1_id, category1_name, tm_id, tm_name, create_time, images, sale_attrs)
with 
    spu as
    (
        select
            id,
            current_date() as k1,
            spu_name,
            description,
            category3_id,
            tm_id,
            now() as create_time
        from ods.ods_spu_info_full
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
    img as
    (
        select
            spu_id,
            GROUP_CONCAT(img_url, ';') as images
        from ods.ods_spu_image_full
        group by spu_id
    ),
    sale_attr as
    (
        select
            spu_id,
            GROUP_CONCAT(CONCAT(sale_attr_name, ':', sale_attr_value), ';') as sale_attrs
        from (
            select
                sa.spu_id,
                sa.sale_attr_name,
                GROUP_CONCAT(sav.sale_attr_value_name) as sale_attr_value
            from ods.ods_spu_sale_attr_full sa
            left join ods.ods_spu_sale_attr_value_full sav
            on sa.id = sav.id and sa.spu_id = sav.spu_id 
                and sa.sale_attr_name = sav.sale_attr_name 
                and sa.base_sale_attr_id = sav.base_sale_attr_id
            group by sa.spu_id, sa.sale_attr_name
        ) t
        group by spu_id
    )
select
    spu.id,
    spu.k1,
    spu.id as spu_id,
    spu.spu_name,
    spu.description,
    spu.category3_id,
    c3.name as category3_name,
    c2.id as category2_id,
    c2.name as category2_name,
    c1.id as category1_id,
    c1.name as category1_name,
    spu.tm_id,
    tm.tm_name,
    spu.create_time,
    img.images,
    sale_attr.sale_attrs
from spu
    left join tm on spu.tm_id = tm.id
    left join c3 on spu.category3_id = c3.id
    left join c2 on c3.category2_id = c2.id
    left join c1 on c2.category1_id = c1.id
    left join img on spu.id = img.spu_id
    left join sale_attr on spu.id = sale_attr.spu_id; 
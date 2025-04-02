-- 商品域分类全量表
INSERT INTO dwd.dwd_product_category_full(id, k1, category_id, category_name, category_level, parent_id, parent_name, category_path, category_path_name, create_time)

-- 一级分类
select
    id,
    date('${pdate}') as k1,
    id as category_id,
    name as category_name,
    1 as category_level,
    NULL as parent_id,
    NULL as parent_name,
    id as category_path,
    name as category_path_name,
    CURRENT_TIMESTAMP() as create_time
from ods.ods_base_category1_full

union all

-- 二级分类
select
    c2.id,
    date('${pdate}') as k1,
    c2.id as category_id,
    c2.name as category_name,
    2 as category_level,
    c2.category1_id as parent_id,
    c1.name as parent_name,
    concat(c2.category1_id, '-', c2.id) as category_path,
    concat(c1.name, '-', c2.name) as category_path_name,
    CURRENT_TIMESTAMP() as create_time
from ods.ods_base_category2_full c2
left join ods.ods_base_category1_full c1
on c2.category1_id = c1.id

union all

-- 三级分类
select
    c3.id,
    date('${pdate}') as k1,
    c3.id as category_id,
    c3.name as category_name,
    3 as category_level,
    c3.category2_id as parent_id,
    c2.name as parent_name,
    concat(c2.category1_id, '-', c3.category2_id, '-', c3.id) as category_path,
    concat(c1.name, '-', c2.name, '-', c3.name) as category_path_name,
    CURRENT_TIMESTAMP() as create_time
from ods.ods_base_category3_full c3
left join ods.ods_base_category2_full c2
on c3.category2_id = c2.id
left join ods.ods_base_category1_full c1
on c2.category1_id = c1.id; 
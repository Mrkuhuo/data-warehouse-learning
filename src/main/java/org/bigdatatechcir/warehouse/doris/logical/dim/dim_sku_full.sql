-- ===============================================================================
-- 商品维度表ETL逻辑(dim_sku_full)
-- 功能描述：整合商品相关的所有维度信息，构建商品全维度视图
-- 数据来源：ods_sku_info_full(主表)、ods_spu_info_full、ods_base_category3_full等
-- 调度策略：每日全量刷新
-- 依赖关系：依赖ODS层相关商品维度表的数据准备完成
-- ===============================================================================

-- 商品维度表数据插入
insert into dim.dim_sku_full(id, k1, price, sku_name, sku_desc, weight, is_sale, spu_id, spu_name, category3_id, category3_name, category2_id, category2_name, category1_id, category1_name, tm_id, tm_name, attr_ids, sale_attr_ids, create_time)
with 
    -- 获取当日SKU基础信息(商品维度主表)
    sku as
    (
        select
            id,             -- SKU ID
            k1,             -- 分区日期
            price,          -- 商品价格
            sku_name,       -- 商品名称
            sku_desc,       -- 商品描述
            weight,         -- 商品重量
            is_sale,        -- 是否在售
            spu_id,         -- SPU ID
            category3_id,   -- 三级分类ID
            tm_id,          -- 品牌ID
            create_time     -- 创建时间
        from ods.ods_sku_info_full
        where k1=date('${pdate}') -- 只处理当日分区数据
    ),
    
    -- 获取SPU信息(标准化产品单元)
    spu as
    (
        select
            id,             -- SPU ID
            spu_name        -- SPU名称
        from ods.ods_spu_info_full
    ),
    
    -- 获取三级分类信息(商品最细粒度分类)
    c3 as
    (
        select
            id,             -- 三级分类ID
            name,           -- 三级分类名称
            category2_id    -- 关联的二级分类ID
        from ods.ods_base_category3_full
    ),
    
    -- 获取二级分类信息(商品中间层级分类)
    c2 as
    (
        select
            id,             -- 二级分类ID
            name,           -- 二级分类名称
            category1_id    -- 关联的一级分类ID
        from ods.ods_base_category2_full
    ),
    
    -- 获取一级分类信息(商品顶层分类)
    c1 as
    (
        select
            id,             -- 一级分类ID
            name            -- 一级分类名称
        from ods.ods_base_category1_full
    ),
    
    -- 获取品牌信息
    tm as
    (
        select
            id,             -- 品牌ID
            tm_name         -- 品牌名称
        from ods.ods_base_trademark_full
    ),
    
    -- 获取SKU平台属性集合(商品通用特征)
    attr as
    (
        select
            sku_id,                 -- SKU ID
            collect_set(id) as ids  -- 平台属性ID的集合，使用collect_set聚合
        from ods.ods_sku_attr_value_full
        group by sku_id             -- 按SKU ID分组聚合
    ),
    
    -- 获取SKU销售属性集合(商品特有销售特征)
    sale_attr as
    (
        select
            sku_id,                 -- SKU ID
            collect_set(id) as ids  -- 销售属性ID的集合，使用collect_set聚合
        from ods.ods_sku_sale_attr_value_full
        group by sku_id             -- 按SKU ID分组聚合
    )

-- 主查询：整合所有维度信息构建完整的商品维度表
select
    sku.id,                 -- SKU ID
    sku.k1,                 -- 分区日期
    sku.price,              -- 商品价格
    sku.sku_name,           -- 商品名称
    sku.sku_desc,           -- 商品描述
    sku.weight,             -- 商品重量
    sku.is_sale,            -- 是否在售
    sku.spu_id,             -- SPU ID
    spu.spu_name,           -- SPU名称
    sku.category3_id,       -- 三级分类ID
    c3.name,                -- 三级分类名称
    c3.category2_id,        -- 二级分类ID
    c2.name,                -- 二级分类名称
    c2.category1_id,        -- 一级分类ID
    c1.name,                -- 一级分类名称
    sku.tm_id,              -- 品牌ID
    tm.tm_name,             -- 品牌名称
    attr.ids attr_ids,      -- 平台属性ID集合
    sale_attr.ids sale_attr_ids, -- 销售属性ID集合
    sku.create_time         -- 创建时间
from sku
    -- 关联SPU信息表，获取SPU名称
    left join spu on sku.spu_id=spu.id
    -- 关联三级分类表，获取三级分类名称和二级分类ID
    left join c3 on sku.category3_id=c3.id
    -- 关联二级分类表，获取二级分类名称和一级分类ID
    left join c2 on c3.category2_id=c2.id
    -- 关联一级分类表，获取一级分类名称
    left join c1 on c2.category1_id=c1.id
    -- 关联品牌表，获取品牌名称
    left join tm on sku.tm_id=tm.id
    -- 关联平台属性集合表
    left join attr on sku.id=attr.sku_id
    -- 关联销售属性集合表
    left join sale_attr on sku.id=sale_attr.sku_id;
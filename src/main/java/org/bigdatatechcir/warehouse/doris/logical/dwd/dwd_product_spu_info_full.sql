/*
 * 文件名: dwd_product_spu_info_full.sql
 * 功能描述: 商品域SPU商品全量表 - 整合SPU基本信息与相关维度数据
 * 数据粒度: SPU商品
 * 刷新策略: 全量刷新
 * 调度周期: 每日
 * 依赖表: 
 *   - ods.ods_spu_info_full: SPU基本信息表
 *   - ods.ods_base_trademark_full: 品牌基础信息表
 *   - ods.ods_base_category3_full: 三级品类表
 *   - ods.ods_base_category2_full: 二级品类表
 *   - ods.ods_base_category1_full: 一级品类表
 *   - ods.ods_spu_image_full: SPU图片表
 *   - ods.ods_spu_sale_attr_full: SPU销售属性表
 *   - ods.ods_spu_sale_attr_value_full: SPU销售属性值表
 * 目标表: dwd.dwd_product_spu_info_full
 * 主要功能: 
 *   1. 整合SPU商品基本信息与各维度数据
 *   2. 补充品牌、品类、图片和销售属性信息
 *   3. 构建完整的商品SPU信息视图
 *   4. 为商品分析和管理提供数据支持
 */

-- 商品域SPU商品全量表
INSERT INTO dwd.dwd_product_spu_info_full(
    id,              -- 记录唯一标识
    k1,              -- 数据日期分区
    spu_id,          -- SPU ID
    spu_name,        -- SPU名称
    description,     -- 商品描述
    category3_id,    -- 三级分类ID
    category3_name,  -- 三级分类名称
    category2_id,    -- 二级分类ID
    category2_name,  -- 二级分类名称
    category1_id,    -- 一级分类ID
    category1_name,  -- 一级分类名称
    tm_id,           -- 品牌ID
    tm_name,         -- 品牌名称
    create_time,     -- 创建时间
    images,          -- 商品图片列表
    sale_attrs       -- 销售属性列表
)
with 
    -- CTE 1: SPU基本信息
    spu as
    (
        select
            id,                        -- SPU ID
            current_date() as k1,      -- 使用当前日期作为分区
            spu_name,                  -- SPU名称
            description,               -- 商品描述
            category3_id,              -- 三级分类ID
            tm_id,                     -- 品牌ID
            now() as create_time       -- 使用当前时间作为创建时间
        from ods.ods_spu_info_full
    ),
    -- CTE 2: 品牌信息
    tm as
    (
        select
            id,                        -- 品牌ID
            tm_name                    -- 品牌名称
        from ods.ods_base_trademark_full
    ),
    -- CTE 3: 三级品类信息
    c3 as
    (
        select
            id,                        -- 三级分类ID
            name,                      -- 三级分类名称
            category2_id               -- 关联的二级分类ID
        from ods.ods_base_category3_full
    ),
    -- CTE 4: 二级品类信息
    c2 as
    (
        select
            id,                        -- 二级分类ID
            name,                      -- 二级分类名称
            category1_id               -- 关联的一级分类ID
        from ods.ods_base_category2_full
    ),
    -- CTE 5: 一级品类信息
    c1 as
    (
        select
            id,                        -- 一级分类ID
            name                       -- 一级分类名称
        from ods.ods_base_category1_full
    ),
    -- CTE 6: 商品图片信息聚合
    img as
    (
        select
            spu_id,                    -- SPU ID
            -- 将所有图片URL聚合为分号分隔的字符串
            GROUP_CONCAT(img_url, ';') as images
        from ods.ods_spu_image_full
        group by spu_id                -- 按SPU ID分组聚合
    ),
    -- CTE 7: 销售属性信息聚合
    sale_attr as
    (
        select
            spu_id,                    -- SPU ID
            -- 将所有销售属性聚合为格式化字符串："属性名:属性值;属性名:属性值"
            GROUP_CONCAT(CONCAT(sale_attr_name, ':', sale_attr_value), ';') as sale_attrs
        from (
            -- 内层子查询：先聚合每个销售属性的所有属性值
            select
                sa.spu_id,                                -- SPU ID
                sa.sale_attr_name,                        -- 销售属性名称
                -- 聚合同一属性名下的所有属性值
                GROUP_CONCAT(sav.sale_attr_value_name) as sale_attr_value
            from ods.ods_spu_sale_attr_full sa
            left join ods.ods_spu_sale_attr_value_full sav
            -- 通过多个条件关联销售属性值表
            on sa.id = sav.id and sa.spu_id = sav.spu_id 
                and sa.sale_attr_name = sav.sale_attr_name 
                and sa.base_sale_attr_id = sav.base_sale_attr_id
            group by sa.spu_id, sa.sale_attr_name         -- 按SPU ID和属性名分组
        ) t
        group by spu_id                -- 按SPU ID分组，进一步聚合所有属性
    )
-- 主查询：整合所有维度信息
select
    spu.id,                           -- 记录ID
    spu.k1,                           -- 分区字段
    spu.id as spu_id,                 -- SPU ID (与id字段相同)
    spu.spu_name,                     -- SPU名称
    spu.description,                  -- 商品描述
    spu.category3_id,                 -- 三级分类ID
    c3.name as category3_name,        -- 三级分类名称
    c2.id as category2_id,            -- 二级分类ID
    c2.name as category2_name,        -- 二级分类名称
    c1.id as category1_id,            -- 一级分类ID
    c1.name as category1_name,        -- 一级分类名称
    spu.tm_id,                        -- 品牌ID
    tm.tm_name,                       -- 品牌名称
    spu.create_time,                  -- 创建时间
    img.images,                       -- 图片列表(分号分隔)
    sale_attr.sale_attrs              -- 销售属性列表(格式化字符串)
from spu
    left join tm on spu.tm_id = tm.id                -- 关联品牌信息
    left join c3 on spu.category3_id = c3.id         -- 关联三级分类
    left join c2 on c3.category2_id = c2.id          -- 关联二级分类
    left join c1 on c2.category1_id = c1.id          -- 关联一级分类
    left join img on spu.id = img.spu_id             -- 关联图片信息
    left join sale_attr on spu.id = sale_attr.spu_id; -- 关联销售属性信息

/*
 * 设计说明:
 * 1. CTE模块化设计:
 *    - 使用7个CTE分别处理不同维度的数据，使SQL结构清晰
 *    - SPU基本信息、品牌、三级分类、二级分类、一级分类、图片和销售属性
 *    - 每个CTE专注于处理单一维度的数据，降低复杂度
 *    
 * 2. 品类层级关联:
 *    - 通过SKU关联三级分类，再通过三级分类关联二级分类，最后通过二级分类关联一级分类
 *    - 这种层级关联设计遵循商品分类的实际业务结构
 *    - 可以完整展示商品的分类层级信息，便于分析商品在不同分类层级的分布
 *
 * 3. 复杂数据聚合:
 *    - 图片URL通过GROUP_CONCAT聚合为分号分隔的字符串，便于存储和解析
 *    - 销售属性采用二级聚合设计：先按属性名聚合属性值，再按SPU ID聚合所有属性
 *    - 最终形成"属性名:属性值;属性名:属性值"格式的字符串
 *
 * 4. 缺失数据处理:
 *    - 使用left join确保即使某些维度数据缺失，也不会丢失SPU记录
 *    - 对于可能缺失的维度，如图片和销售属性，不会影响主记录的生成
 *
 * 5. 数据应用场景:
 *    - 商品分析：评估不同类别、品牌的商品分布
 *    - 商品展示：为前端提供完整的商品SPU信息，包括名称、分类、品牌等
 *    - 商品搜索：支持按不同维度(品类、品牌、属性)的商品搜索和筛选
 *    - 数据管理：为商品管理和库存管理提供基础数据支持
 */ 
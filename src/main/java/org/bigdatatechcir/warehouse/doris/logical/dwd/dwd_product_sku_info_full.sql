/*
 * 文件名: dwd_product_sku_info_full.sql
 * 功能描述: 商品域SKU商品全量表 - 整合SKU基本信息与多个维度数据
 * 数据粒度: SKU商品
 * 刷新策略: 全量刷新
 * 调度周期: 每日
 * 依赖表: 
 *   - ods.ods_sku_info_full: SKU基本信息
 *   - ods.ods_base_trademark_full: 品牌信息
 *   - ods.ods_base_category3_full: 三级分类信息
 *   - ods.ods_base_category2_full: 二级分类信息
 *   - ods.ods_base_category1_full: 一级分类信息
 *   - ods.ods_sku_attr_value_full: SKU属性值信息
 *   - ods.ods_sku_sale_attr_value_full: SKU销售属性值信息
 *   - ods.ods_financial_sku_cost_full: SKU成本信息
 * 目标表: dwd.dwd_product_sku_info_full
 * 主要功能: 
 *   1. 整合SKU基本信息与品牌、分类维度数据
 *   2. 聚合SKU属性和销售属性为字符串
 *   3. 关联SKU成本信息
 *   4. 构建完整的商品维度视图
 */

-- 商品域SKU商品全量表
INSERT INTO dwd.dwd_product_sku_info_full(
    id,               -- SKU的唯一标识
    k1,               -- 数据日期分区
    sku_id,           -- SKU ID(冗余字段，与id相同)
    spu_id,           -- 所属SPU ID
    price,            -- SKU价格
    sku_name,         -- SKU名称
    sku_desc,         -- SKU描述
    weight,           -- 重量
    tm_id,            -- 品牌ID
    tm_name,          -- 品牌名称
    category1_id,     -- 一级分类ID
    category1_name,   -- 一级分类名称
    category2_id,     -- 二级分类ID
    category2_name,   -- 二级分类名称
    category3_id,     -- 三级分类ID
    category3_name,   -- 三级分类名称
    default_img,      -- 默认图片URL
    create_time,      -- 创建时间
    attr_values,      -- 属性值字符串，格式：属性名:属性值;属性名:属性值
    sale_attr_values, -- 销售属性值字符串，格式：销售属性名:属性值;销售属性名:属性值
    sku_cost          -- SKU成本
)
with
    -- CTE定义区：使用WITH子句定义多个公共表表达式，简化复杂查询
    
    -- SKU基本信息CTE：获取SKU的基本信息字段
    sku as
    (
        select
            id,                    -- SKU ID
            k1,                    -- 分区日期
            spu_id,                -- 所属SPU ID
            price,                 -- 价格
            sku_name,              -- 名称
            sku_desc,              -- 描述
            weight,                -- 重量
            tm_id,                 -- 品牌ID
            category3_id,          -- 三级分类ID
            sku_default_img as default_img, -- 默认图片
            create_time            -- 创建时间
        from ods.ods_sku_info_full
        where k1=date('${pdate}')  -- 按日期分区过滤
    ),
    
    -- 品牌维度CTE：获取品牌信息
    tm as
    (
        select
            id,       -- 品牌ID
            tm_name   -- 品牌名称
        from ods.ods_base_trademark_full
        -- 注：维度表通常不需要分区过滤
    ),
    
    -- 三级分类维度CTE：获取三级分类信息
    c3 as
    (
        select
            id,           -- 三级分类ID
            name,         -- 三级分类名称
            category2_id  -- 对应的二级分类ID
        from ods.ods_base_category3_full
        -- 注：维度表通常不需要分区过滤
    ),
    
    -- 二级分类维度CTE：获取二级分类信息
    c2 as
    (
        select
            id,           -- 二级分类ID
            name,         -- 二级分类名称
            category1_id  -- 对应的一级分类ID
        from ods.ods_base_category2_full
        -- 注：维度表通常不需要分区过滤
    ),
    
    -- 一级分类维度CTE：获取一级分类信息
    c1 as
    (
        select
            id,    -- 一级分类ID
            name   -- 一级分类名称
        from ods.ods_base_category1_full
        -- 注：维度表通常不需要分区过滤
    ),
    
    -- SKU属性CTE：聚合SKU的所有属性为字符串
    attr as
    (
        select
            sku_id,
            GROUP_CONCAT(CONCAT(attr_name, ':', value_name), ';') as attr_values -- 将多行属性聚合为单一字符串
        from ods.ods_sku_attr_value_full
        group by sku_id -- 按SKU分组聚合
    ),
    
    -- SKU销售属性CTE：聚合SKU的所有销售属性为字符串
    sale_attr as
    (
        select
            sku_id,
            GROUP_CONCAT(CONCAT(sale_attr_name, ':', sale_attr_value_name), ';') as sale_attr_values -- 将多行销售属性聚合为单一字符串
        from ods.ods_sku_sale_attr_value_full
        group by sku_id -- 按SKU分组聚合
    ),
    
    -- SKU成本CTE：获取SKU成本信息
    cost as
    (
        select
            sku_id,    -- SKU ID
            sku_cost   -- SKU成本
        from ods.ods_financial_sku_cost_full
        where k1=date('${pdate}') -- 按日期分区过滤
    )

-- 主查询：整合所有CTE数据
select
    sku.id,                  -- SKU ID
    sku.k1,                  -- 分区日期
    sku.id as sku_id,        -- SKU ID(冗余字段，方便使用)
    sku.spu_id,              -- 所属SPU ID
    sku.price,               -- 价格
    sku.sku_name,            -- 名称
    sku.sku_desc,            -- 描述
    sku.weight,              -- 重量
    sku.tm_id,               -- 品牌ID
    tm.tm_name,              -- 品牌名称(关联自品牌维度)
    c1.id as category1_id,   -- 一级分类ID(关联自分类维度)
    c1.name as category1_name, -- 一级分类名称
    c2.id as category2_id,   -- 二级分类ID
    c2.name as category2_name, -- 二级分类名称
    c3.id as category3_id,   -- 三级分类ID
    c3.name as category3_name, -- 三级分类名称
    sku.default_img,         -- 默认图片
    sku.create_time,         -- 创建时间
    attr.attr_values,        -- 属性值字符串
    sale_attr.sale_attr_values, -- 销售属性值字符串
    cost.sku_cost            -- SKU成本
from sku
    -- 关联品牌维度
    left join tm on sku.tm_id = tm.id
    -- 关联三级分类维度
    left join c3 on sku.category3_id = c3.id
    -- 关联二级分类维度
    left join c2 on c3.category2_id = c2.id
    -- 关联一级分类维度
    left join c1 on c2.category1_id = c1.id
    -- 关联SKU属性
    left join attr on sku.id = attr.sku_id
    -- 关联SKU销售属性
    left join sale_attr on sku.id = sale_attr.sku_id
    -- 关联SKU成本
    left join cost on sku.id = cost.sku_id;

/*
 * 设计说明:
 * 1. CTE(公共表表达式)的使用:
 *    - 使用WITH子句定义了8个CTE，每个CTE负责一个特定的数据域
 *    - 这种方式使查询更加模块化，易于理解和维护
 *    - 相比于多层嵌套子查询，CTE提供了更好的代码可读性
 *    
 * 2. 分类数据的多级关联:
 *    - 实现了从三级分类到一级分类的层级关联(c3->c2->c1)
 *    - 这种链式关联确保了分类数据的完整性和层级正确性
 *    - 使用left join保证即使某级分类数据缺失，也不会丢失SKU记录
 *
 * 3. 属性数据的字符串聚合:
 *    - 使用GROUP_CONCAT函数将多行属性记录聚合为单一字符串
 *    - 格式化为"属性名:属性值"的键值对，多个键值对之间用分号分隔
 *    - 这种设计简化了数据结构，便于存储和展示，但牺牲了数据的关系性
 *
 * 4. 数据过滤策略:
 *    - 在sku和cost CTE中使用k1分区过滤，确保只处理当天数据
 *    - 维度表(品牌、分类)不使用分区过滤，获取全量数据
 *
 * 5. 数据质量考虑:
 *    - 使用left join确保核心SKU数据不会因为关联失败而丢失
 *    - 对于可能缺失的维度信息(品牌、分类、属性等)，相关字段将为NULL
 *
 * 6. 冗余设计:
 *    - 保留了sku_id字段(与id相同)，这种冗余设计便于下游使用
 *    - 存储了分类和品牌的ID与名称，减少后续查询中的表关联需求
 */ 
/*
 * 文件名: dwd_product_category_full.sql
 * 功能描述: 商品域分类全量表 - 整合所有层级的商品分类信息
 * 数据粒度: 商品分类
 * 刷新策略: 全量刷新
 * 调度周期: 每日
 * 依赖表: 
 *   - ods.ods_base_category1_full: 一级分类数据
 *   - ods.ods_base_category2_full: 二级分类数据
 *   - ods.ods_base_category3_full: 三级分类数据
 * 目标表: dwd.dwd_product_category_full
 * 主要功能: 
 *   1. 整合一级、二级、三级商品分类数据到单一表中
 *   2. 生成分类层级路径和路径名称，便于展示和查询
 *   3. 构建完整的商品分类层次结构
 */

-- 商品域分类全量表
INSERT INTO dwd.dwd_product_category_full(
    id,                 -- 记录唯一标识
    k1,                 -- 数据日期，用于分区
    category_id,        -- 分类ID
    category_name,      -- 分类名称
    category_level,     -- 分类层级(1/2/3级)
    parent_id,          -- 父级分类ID
    parent_name,        -- 父级分类名称
    category_path,      -- 分类ID路径
    category_path_name, -- 分类名称路径
    create_time         -- 创建时间
)

-- 一级分类数据处理
select
    id,                                  -- 主键ID，直接使用一级分类ID
    date('${pdate}') as k1,              -- 使用参数日期作为k1值
    id as category_id,                   -- 分类ID，与id相同
    name as category_name,               -- 分类名称
    1 as category_level,                 -- 固定为1级分类
    NULL as parent_id,                   -- 一级分类无父级，设为NULL
    NULL as parent_name,                 -- 一级分类无父级名称，设为NULL
    id as category_path,                 -- 路径仅包含自身ID
    name as category_path_name,          -- 路径名仅包含自身名称
    CURRENT_TIMESTAMP() as create_time   -- 使用当前时间作为创建时间
from ods.ods_base_category1_full         -- 数据来源：一级分类表

union all

-- 二级分类数据处理
select
    c2.id,                                        -- 主键ID，使用二级分类ID
    date('${pdate}') as k1,                       -- 使用参数日期作为k1值
    c2.id as category_id,                         -- 分类ID
    c2.name as category_name,                     -- 分类名称
    2 as category_level,                          -- 固定为2级分类
    c2.category1_id as parent_id,                 -- 父级ID为对应的一级分类ID
    c1.name as parent_name,                       -- 父级名称为一级分类名称
    concat(c2.category1_id, '-', c2.id) as category_path,  -- 路径格式：一级ID-二级ID
    concat(c1.name, '-', c2.name) as category_path_name,   -- 路径名称格式：一级名称-二级名称
    CURRENT_TIMESTAMP() as create_time            -- 使用当前时间作为创建时间
from ods.ods_base_category2_full c2               -- 数据来源：二级分类表
left join ods.ods_base_category1_full c1          -- 关联一级分类表获取父级信息
on c2.category1_id = c1.id                        -- 通过一级分类ID关联

union all

-- 三级分类数据处理
select
    c3.id,                                                           -- 主键ID，使用三级分类ID
    date('${pdate}') as k1,                                          -- 使用参数日期作为k1值
    c3.id as category_id,                                            -- 分类ID
    c3.name as category_name,                                        -- 分类名称
    3 as category_level,                                             -- 固定为3级分类
    c3.category2_id as parent_id,                                    -- 父级ID为对应的二级分类ID
    c2.name as parent_name,                                          -- 父级名称为二级分类名称
    concat(c2.category1_id, '-', c3.category2_id, '-', c3.id) as category_path,  -- 路径格式：一级ID-二级ID-三级ID
    concat(c1.name, '-', c2.name, '-', c3.name) as category_path_name,           -- 路径名称格式：一级名称-二级名称-三级名称
    CURRENT_TIMESTAMP() as create_time                               -- 使用当前时间作为创建时间
from ods.ods_base_category3_full c3                                  -- 数据来源：三级分类表
left join ods.ods_base_category2_full c2                             -- 关联二级分类表
on c3.category2_id = c2.id                                           -- 通过二级分类ID关联
left join ods.ods_base_category1_full c1                             -- 再关联一级分类表
on c2.category1_id = c1.id;                                          -- 通过一级分类ID关联

/*
 * 设计说明:
 * 1. 统一分类表设计:
 *    - 将三个分类层级的数据统一存储在一个表中
 *    - 通过category_level字段区分不同层级的分类
 *    - 这种设计简化了分类数据的查询和管理
 *    
 * 2. 路径字段设计:
 *    - category_path字段存储ID路径，用于程序处理
 *    - category_path_name字段存储名称路径，用于显示
 *    - 路径使用连字符("-")分隔，便于分割和解析
 *
 * 3. 关联逻辑:
 *    - 二级分类关联一级分类，获取父级名称和完整路径
 *    - 三级分类关联二级分类和一级分类，构建完整的分类体系
 *    - 使用left join确保即使父级数据缺失也不会丢失子级记录
 *
 * 4. 数据应用场景:
 *    - 商品分类导航：提供完整的分类层次结构
 *    - 分类统计分析：按不同层级分析商品分布
 *    - 商品检索：支持按分类层级和路径检索商品
 *
 * 5. 时间处理:
 *    - 所有创建时间使用CURRENT_TIMESTAMP()获取当前时间
 *    - 实际应用中，可考虑保留原始创建时间
 */ 
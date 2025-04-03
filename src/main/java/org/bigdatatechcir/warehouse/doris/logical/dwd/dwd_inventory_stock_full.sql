/*
 * 文件名: dwd_inventory_stock_full.sql
 * 功能描述: 库存域库存事实表 - 整合各仓库商品库存信息
 * 数据粒度: 商品-仓库
 * 刷新策略: 全量刷新
 * 调度周期: 每日
 * 依赖表: 
 *   - ods.ods_ware_sku_full: 仓库商品库存数据
 *   - ods.ods_ware_info_full: 仓库基本信息
 *   - ods.ods_sku_info_full: 商品SKU信息
 * 目标表: dwd.dwd_inventory_stock_full
 * 主要功能: 
 *   1. 整合商品库存数据与仓库维度信息
 *   2. 提供完整的库存现状视图，包括库存量和仓库位置
 *   3. 为库存管理和供应链决策提供基础数据
 */

-- 库存域库存事实表
INSERT INTO dwd.dwd_inventory_stock_full(
    id,               -- 记录唯一标识
    k1,               -- 数据日期，用于分区
    sku_id,           -- 商品SKU ID
    sku_name,         -- 商品SKU名称
    warehouse_id,     -- 仓库ID
    warehouse_name,   -- 仓库名称
    warehouse_address,-- 仓库地址
    warehouse_areacode,-- 仓库区域编码
    stock,            -- 库存数量
    stock_name,       -- 库存数量状态描述
    is_default,       -- 是否默认仓库
    create_time       -- 创建时间
)
select 
    wse.id,                          -- 库存记录ID
    wse.k1,                          -- 分区日期
    wse.sku_id,                      -- 商品SKU ID，关联商品维度
    si.sku_name,                     -- 商品SKU名称，从商品表获取
    wse.warehouse_id,                -- 仓库ID，关联仓库维度
    wi.name as warehouse_name,       -- 仓库名称，从仓库信息表获取
    wi.address as warehouse_address, -- 仓库地址，从仓库信息表获取 
    wi.areacode as warehouse_areacode, -- 仓库所在区域编码，从仓库信息表获取
    wse.stock,                       -- 库存数量
    case                             -- 根据库存数量生成库存状态描述
        when wse.stock = 0 then '无货'
        when wse.stock < 10 then '低库存'
        when wse.stock < 50 then '库存正常'
        else '库存充足'
    end as stock_name,               -- 库存状态描述
    wse.is_default,                  -- 是否为默认仓库
    wse.create_time                  -- 记录创建时间
from 
    (
        -- 库存数据子查询
        select
            id,                      -- 库存记录ID
            k1,                      -- 分区日期
            sku_id,                  -- 商品SKU ID
            warehouse_id,            -- 仓库ID
            stock,                   -- 库存数量
            is_default,              -- 是否默认仓库
            create_time              -- 创建时间
        from ods.ods_ware_sku_full
        where k1 = date('${pdate}')  -- 按分区日期过滤，只处理当天数据
    ) wse
    left join
    (
        -- 仓库信息子查询
        select
            id,                      -- 仓库ID
            name,                    -- 仓库名称
            address,                 -- 仓库地址
            areacode                 -- 仓库区域编码
        from ods.ods_ware_info_full
        where k1 = date('${pdate}')  -- 按分区日期过滤，只处理当天数据
    ) wi
    on wse.warehouse_id = wi.id      -- 通过仓库ID关联仓库信息
    left join
    (
        -- 商品信息子查询
        select
            id,                      -- 商品SKU ID
            sku_name                 -- 商品SKU名称
        from ods.ods_sku_info_full
        where k1 = date('${pdate}')  -- 按分区日期过滤，只处理当天数据
    ) si
    on wse.sku_id = si.id;           -- 通过商品SKU ID关联商品信息

/*
 * 设计说明:
 * 1. 多维度关联设计:
 *    - 以库存记录为主体，关联仓库和商品维度信息
 *    - 使用left join确保即使缺少维度信息，库存记录也不会丢失
 *    - 完整展示了"什么商品"在"哪个仓库"有"多少库存"
 *    
 * 2. 字段标准化和丰富:
 *    - 引入商品名称、仓库名称等描述性字段，提高数据可读性
 *    - 增加stock_name字段，将库存数量转换为业务含义明确的状态描述
 *    - 保留仓库地址和区域信息，支持后续地理维度分析
 *
 * 3. 过滤策略:
 *    - 所有子查询都使用相同的时间过滤条件k1=date('${pdate}')
 *    - 确保库存数据、仓库信息和商品信息都是同一天的快照
 *
 * 4. 数据应用场景:
 *    - 库存管理：实时掌握各仓库各商品库存状况
 *    - 仓库优化：分析各区域仓库的库存分布，优化仓储布局
 *    - 补货决策：识别低库存商品，及时安排补货
 *    - 供应链优化：结合销售数据分析库存周转率和仓储效率
 */ 
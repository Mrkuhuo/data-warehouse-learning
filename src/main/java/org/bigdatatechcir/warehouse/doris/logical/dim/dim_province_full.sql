-- ===============================================================================
-- 地区维度表ETL逻辑(dim_province_full)
-- 功能描述：整合地区相关的维度信息，构建标准化的地区维度数据
-- 数据来源：ods_base_province_full、ods_base_region_full
-- 调度策略：低频全量刷新，通常按月或季度更新(地区数据变化不频繁)
-- 依赖关系：依赖ODS层相关地区基础表的数据准备完成
-- 备注：修正了ods_base_region_full表字段引用，使用region_name而不是name
-- ===============================================================================

-- 地区维度表数据插入
insert into dim.dim_province_full(id, province_name, area_code, iso_code, iso_3166_2, region_id, region_name)
select 
    p.id,                 -- 省份ID
    p.name as province_name, -- 省份名称
    p.area_code,          -- 地区编码
    p.iso_code,           -- 旧版ISO编码
    p.iso_3166_2,         -- 新版ISO编码
    p.region_id,          -- 地区ID，关联地区表的外键
    r.region_name         -- 地区名称，通过关联region表获取（注意：使用region_name而不是name）
from ods.ods_base_province_full p 
    -- 关联地区表获取地区名称
    left join ods.ods_base_region_full r on p.region_id = r.id
-- 可以增加where条件筛选有效数据
-- where p.k1 = date('${pdate}')
;
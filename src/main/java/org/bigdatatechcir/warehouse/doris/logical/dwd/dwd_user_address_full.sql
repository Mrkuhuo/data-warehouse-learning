/*
 * 文件名: dwd_user_address_full.sql
 * 功能描述: 用户域用户地址全量表 - 整合用户地址信息与省份维度数据
 * 数据粒度: 用户地址ID
 * 刷新策略: 全量刷新
 * 调度周期: 每日
 * 依赖表: 
 *   - ods.ods_user_address_full: 用户地址原始数据
 *   - ods.ods_base_province_full: 省份维度数据
 * 目标表: dwd.dwd_user_address_full
 * 主要功能: 
 *   1. 整合用户地址信息与省份名称
 *   2. 补充城市和区县信息(临时使用省份信息代替)
 *   3. 生成完整地址字符串
 *   4. 标准化地址数据字段
 */

-- 用户域用户地址全量表
INSERT INTO dwd.dwd_user_address_full(
    id,              -- 唯一标识
    k1,              -- 数据日期，用于分区 
    user_id,         -- 用户ID
    province_id,     -- 省份ID
    province_name,   -- 省份名称
    city_id,         -- 城市ID
    city_name,       -- 城市名称
    district_id,     -- 区县ID
    district_name,   -- 区县名称
    detail_address,  -- 详细地址
    consignee,       -- 收件人
    phone_num,       -- 联系电话
    is_default,      -- 是否默认地址(1:是 0:否)
    create_time,     -- 创建时间
    operate_time,    -- 操作时间
    postal_code,     -- 邮政编码
    full_address     -- 完整地址
)
select
    ua.id,                   -- 地址ID，主键
    date('${pdate}') as k1,  -- 使用参数日期作为k1值，便于分区管理
    ua.user_id,              -- 用户ID，关联用户维度
    ua.province_id,          -- 省份ID，关联省份维度
    bp.name as province_name, -- 省份名称，从省份表获取
    ua.city_id,              -- 城市ID，当前使用省份ID代替
    bp.area_code as city_name, -- 使用省份表的area_code字段作为城市名称(临时方案)
    ua.district_id,          -- 区县ID，当前使用省份ID代替
    bp.iso_code as district_name, -- 使用省份表的iso_code字段作为区县名称(临时方案)
    ua.user_address as detail_address, -- 将原表user_address字段映射为标准字段detail_address
    ua.consignee,            -- 收件人姓名
    ua.phone_num,            -- 联系电话
    ua.is_default,           -- 是否为默认地址
    now() as create_time,    -- 使用当前时间作为数据创建时间
    now() as operate_time,   -- 使用当前时间作为数据操作时间
    null as postal_code,     -- 邮政编码字段，当前数据源中无此信息，填充NULL
    concat(bp.name, ' ', bp.area_code, ' ', bp.iso_code, ' ', ua.user_address) as full_address -- 拼接完整地址字符串，便于展示和搜索
from
    (
        -- 用户地址子查询，提取并标准化地址基本信息
        select
            id,              -- 地址ID
            user_id,         -- 用户ID
            province_id,     -- 省份ID
            province_id as city_id, -- 暂用province_id代替city_id，因为源表缺少city_id字段
            province_id as district_id, -- 暂用province_id代替district_id，因为源表缺少district_id字段
            user_address,    -- 详细地址
            consignee,       -- 收件人
            phone_num,       -- 联系电话
            is_default       -- 是否默认地址
        from ods.ods_user_address_full
        -- 注：此处未使用k1过滤，因为我们要获取全量数据
    ) ua
    left join
    (
        -- 省份维度子查询，提取省份相关信息
        select
            id,              -- 省份ID
            name,            -- 省份名称
            area_code,       -- 区域编码(临时用作城市名称)
            iso_code         -- ISO编码(临时用作区县名称)
        from ods.ods_base_province_full
        -- 注：省份表通常为维度表，不需要k1过滤
    ) bp
    on ua.province_id = bp.id; -- 基于省份ID关联省份维度信息

/*
 * 设计说明:
 * 1. 当前实现中，由于缺少城市表和区县表，暂时使用省份表的字段代替
 *    - 使用area_code代替城市名称
 *    - 使用iso_code代替区县名称
 *    这是临时解决方案，后续应建立专门的城市和区县维度表
 *    
 * 2. 时间字段处理:
 *    - 使用${pdate}参数作为分区日期
 *    - 使用now()函数生成create_time和operate_time
 *    确保时间字段的一致性和可追溯性
 *
 * 3. 全量表特点:
 *    - 不使用增量条件(如where k1=date)过滤数据
 *    - 每次运行会处理所有历史数据
 *    - 适用于维度数据和基础信息表
 *
 * 4. 数据质量考虑:
 *    - left join确保不会因为维度表缺失数据而丢失原始记录
 *    - 对于缺失的维度信息，相关字段将为NULL
 *    - 后续可考虑添加COALESCE函数处理NULL值
 */
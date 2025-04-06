-- ===============================================================================
-- 活动维度表ETL逻辑(dim_activity_full)
-- 功能描述：整合活动相关的所有维度信息，构建活动全维度视图
-- 数据来源：ods_activity_info_full、ods_activity_rule_full、ods_activity_sku_full
-- 调度策略：每日全量刷新
-- 依赖关系：依赖ODS层相关活动表的数据准备完成
-- 备注：修正了各表结构的适配问题
-- ===============================================================================

-- 活动维度表数据插入
insert into dim.dim_activity_full(
    id, 
    k1, 
    activity_name, 
    activity_type, 
    activity_desc, 
    start_time, 
    end_time, 
    create_time, 
    rule_id, 
    rule_name, 
    benefit_amount, 
    benefit_discount, 
    benefit_level, 
    benefit_desc, 
    limit_amount, 
    limit_num, 
    status, 
    sku_id_list, 
    category_id_list
)
with 
    -- 获取活动基础信息
    activity as 
    (
        select
            id,                   -- 活动ID
            k1,                   -- 分区日期
            activity_name,        -- 活动名称  
            activity_type,        -- 活动类型
            activity_desc,        -- 活动描述
            start_time,           -- 开始时间
            end_time,             -- 结束时间
            create_time,          -- 创建时间
            1 as status           -- 默认活动状态为有效(1)，源表中没有此字段
        from ods.ods_activity_info_full
        where k1 = date('${pdate}')  -- 只处理当日分区数据
    ),
    
    -- 获取活动规则信息
    rule as 
    (
        select
            id as rule_id,         -- 规则ID
            activity_id,           -- 关联的活动ID
            -- 从activity_type派生规则名称
            case activity_type
                when '3101' then concat('满减活动规则')
                when '3102' then concat('满件打折规则')
                when '3103' then concat('折扣规则')
                else concat('活动规则-', activity_type)
            end as rule_name,      -- 规则名称(从活动类型派生)
            activity_type,         -- 活动类型
            condition_amount,      -- 满减金额
            condition_num,         -- 满减件数
            benefit_amount,        -- 优惠金额
            benefit_discount,      -- 优惠折扣
            benefit_level,         -- 优惠等级
            -- 生成优惠描述
            case activity_type
                when '3101' then concat('满', condition_amount, '元减', benefit_amount, '元')
                when '3102' then concat('满', condition_num, '件打', CAST(benefit_discount * 10 AS STRING), '折')
                when '3103' then concat('打', CAST(benefit_discount * 10 AS STRING), '折')
                else '优惠活动'
            end as benefit_desc,   -- 优惠描述(从规则计算)
            condition_amount as limit_amount,  -- 限制金额(使用满减条件金额)
            condition_num as limit_num         -- 限制数量(使用满减条件数量)
        from ods.ods_activity_rule_full
        -- 注意：该表无分区字段k1，取所有数据
    ),
    
    -- 获取活动关联的商品
    activity_sku as 
    (
        select
            activity_id,                       -- 活动ID
            collect_set(sku_id) as sku_ids     -- 收集活动关联的所有商品ID
        from ods.ods_activity_sku_full
        where k1 = date('${pdate}')            -- 只处理当日分区数据
        group by activity_id                   -- 按活动ID分组聚合
    ),
    
    -- 获取当日商品信息(简化版，仅获取必要字段)
    sku_info as
    (
        select
            id as sku_id,          -- 商品ID (注意：ods_sku_info_full表中使用id字段作为SKU ID)
            category3_id           -- 三级品类ID
        from ods.ods_sku_info_full
        where k1 = date('${pdate}')  -- 只处理当日分区数据
    ),
    
    -- 获取品类信息(三级品类表没有分区字段)
    category_info as
    (
        select
            c3.id as category3_id,        -- 三级品类ID
            c3.name as category3_name,    -- 三级品类名称
            c3.category2_id,              -- 二级品类ID
            c2.name as category2_name,    -- 二级品类名称
            c2.category1_id,              -- 一级品类ID
            c1.name as category1_name     -- 一级品类名称
        from ods.ods_base_category3_full c3
        left join ods.ods_base_category2_full c2 on c3.category2_id = c2.id
        left join ods.ods_base_category1_full c1 on c2.category1_id = c1.id
        -- 品类表都没有分区字段，取所有数据
    ),
    
    -- 获取活动关联的品类(通过活动关联的商品推导)
    activity_category as 
    (
        select
            a.activity_id,                          -- 活动ID
            collect_set(si.category3_id) as cate_ids  -- 收集活动关联的所有品类ID
        from ods.ods_activity_sku_full a
        join sku_info si on a.sku_id = si.sku_id      -- 关联商品信息获取品类
        where a.k1 = date('${pdate}')                -- 只处理当日分区数据
        group by a.activity_id                       -- 按活动ID分组聚合
    )
    
-- 主查询：整合所有活动相关维度信息
select
    a.id,                       -- 活动ID
    a.k1,                       -- 分区日期
    a.activity_name,            -- 活动名称
    a.activity_type,            -- 活动类型
    a.activity_desc,            -- 活动描述
    a.start_time,               -- 开始时间
    a.end_time,                 -- 结束时间
    a.create_time,              -- 创建时间
    r.rule_id,                  -- 规则ID
    r.rule_name,                -- 规则名称(从活动类型派生)
    r.benefit_amount,           -- 优惠金额
    r.benefit_discount,         -- 优惠折扣
    r.benefit_level,            -- 优惠等级
    r.benefit_desc,             -- 优惠描述(从规则计算)
    r.limit_amount,             -- 限制金额(源表中为满减条件金额)
    r.limit_num,                -- 限制数量(源表中为满减条件数量)
    a.status,                   -- 活动状态(默认为1-有效)
    s.sku_ids as sku_id_list,   -- 适用商品ID列表
    c.cate_ids as category_id_list  -- 适用品类ID列表
from activity a
    -- 关联活动规则信息
    left join rule r on a.id = r.activity_id
    -- 关联活动商品关系
    left join activity_sku s on a.id = s.activity_id
    -- 关联活动品类关系
    left join activity_category c on a.id = c.activity_id;
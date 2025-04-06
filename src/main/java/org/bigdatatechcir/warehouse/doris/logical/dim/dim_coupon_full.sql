-- ===============================================================================
-- 优惠券维度表ETL逻辑(dim_coupon_full)
-- 功能描述：整合优惠券相关的所有维度信息，构建优惠券全维度视图
-- 数据来源：ods_coupon_info_full、ods_coupon_range_full、ods_coupon_use_full
-- 调度策略：每日全量刷新
-- 依赖关系：依赖ODS层相关优惠券表的数据准备完成
-- 备注：修正了字段映射问题，源表中没有coupon_desc字段，使用range_desc代替；valid_days字段需动态计算
--       修正了ods_coupon_range_full表相关的查询，该表没有k1分区字段和category_id字段
-- ===============================================================================

-- 优惠券维度表数据插入
insert into dim.dim_coupon_full(
    id,
    k1,
    coupon_name,
    coupon_type,
    coupon_desc,
    condition_amount,
    condition_num,
    benefit_amount,
    benefit_discount,
    benefit_desc,
    create_time,
    range_type,
    limit_num,
    taken_count,
    used_count,
    start_time,
    end_time,
    valid_days,
    status,
    sku_id_list,
    category_id_list
)
with 
    -- 获取优惠券基础信息
    coupon as
    (
        select
            id,                     -- 优惠券ID
            k1,                     -- 分区日期
            coupon_name,            -- 优惠券名称
            coupon_type,            -- 优惠券类型
            range_desc as coupon_desc,  -- 优惠券描述 (使用range_desc字段作为coupon_desc)
            condition_amount,       -- 使用门槛金额
            condition_num,          -- 使用门槛件数
            benefit_amount,         -- 优惠金额
            benefit_discount,       -- 优惠折扣
            -- 生成优惠描述
            case coupon_type
                when '1' then concat('满', condition_amount, '元减', benefit_amount, '元')
                when '2' then concat('打', CAST(benefit_discount * 10 AS STRING), '折')
                when '3' then concat('满', condition_amount, '元减', benefit_amount, '元')
                when '4' then concat('满', condition_num, '件打', CAST(benefit_discount * 10 AS STRING), '折')
                else concat('优惠券-', coupon_type)
            end as benefit_desc,    -- 优惠描述（根据类型生成）
            create_time,            -- 创建时间
            range_type,             -- 适用范围类型
            limit_num,              -- 每人限领数量
            taken_count,            -- 已领取数量（源表中有该字段）
            start_time,             -- 可领取开始时间
            end_time,               -- 可领取结束时间
            -- 计算有效天数（expire_time - create_time的天数，没有expire_time则为30）
            case 
                when expire_time is not null then datediff(to_date(expire_time), to_date(create_time))
                else 30  -- 默认30天有效期
            end as valid_days,      -- 有效天数
            1 as status             -- 状态（默认为有效）
        from ods.ods_coupon_info_full
        where k1 = date('${pdate}')  -- 只处理当日分区数据
    ),
    
    -- 获取优惠券领取和使用统计
    coupon_stats as
    (
        select
            coupon_id,                  -- 优惠券ID
            count(*) as total_count,    -- 总领取数量
            sum(case when used_time is not null then 1 else 0 end) as use_count  -- 已使用数量
        from ods.ods_coupon_use_full
        where k1 = date('${pdate}')     -- 只处理当日分区数据
        group by coupon_id              -- 按优惠券ID分组聚合
    ),
    
    -- 获取优惠券关联的商品
    coupon_sku as
    (
        select
            coupon_id,                       -- 优惠券ID
            collect_set(range_id) as sku_ids   -- 收集优惠券关联的所有商品ID（使用range_id作为sku_id）
        from ods.ods_coupon_range_full
        where range_type = 'SKU'             -- 筛选商品范围类型（注意：该表没有k1分区字段）
        group by coupon_id                   -- 按优惠券ID分组聚合
    ),
    
    -- 获取优惠券关联的品类
    coupon_category as
    (
        select
            coupon_id,                           -- 优惠券ID
            collect_set(range_id) as cate_ids    -- 收集优惠券关联的所有品类ID（使用range_id作为category_id）
        from ods.ods_coupon_range_full
        where range_type = 'CATEGORY'            -- 筛选品类范围类型（注意：该表没有k1分区字段）
        group by coupon_id                       -- 按优惠券ID分组聚合
    )
    
-- 主查询：整合所有优惠券相关维度信息
select
    c.id,                       -- 优惠券ID
    c.k1,                       -- 分区日期
    c.coupon_name,              -- 优惠券名称
    c.coupon_type,              -- 优惠券类型
    c.coupon_desc,              -- 优惠券描述
    c.condition_amount,         -- 使用门槛金额
    c.condition_num,            -- 使用门槛件数
    c.benefit_amount,           -- 优惠金额
    c.benefit_discount,         -- 优惠折扣
    c.benefit_desc,             -- 优惠描述
    c.create_time,              -- 创建时间
    c.range_type,               -- 适用范围类型
    c.limit_num,                -- 每人限领数量
    coalesce(s.total_count, c.taken_count, 0) as taken_count,  -- 已领取数量，优先使用统计数据，其次使用源表数据，最后默认为0
    coalesce(s.use_count, 0) as used_count,     -- 已使用数量，如果为空则默认为0
    c.start_time,               -- 可领取开始时间
    c.end_time,                 -- 可领取结束时间
    c.valid_days,               -- 有效天数
    c.status,                   -- 状态
    sku.sku_ids as sku_id_list, -- 适用商品ID列表
    cat.cate_ids as category_id_list -- 适用品类ID列表
from coupon c
    -- 关联优惠券统计信息
    left join coupon_stats s on c.id = s.coupon_id
    -- 关联优惠券商品关系
    left join coupon_sku sku on c.id = sku.coupon_id
    -- 关联优惠券品类关系
    left join coupon_category cat on c.id = cat.coupon_id;
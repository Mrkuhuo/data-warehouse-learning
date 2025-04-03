/*
 * 文件名: dwd_marketing_activity_full.sql
 * 功能描述: 营销域活动全量表 - 整合活动基本信息、规则和关联商品
 * 数据粒度: 活动
 * 刷新策略: 全量刷新
 * 调度周期: 每日
 * 依赖表: 
 *   - ods.ods_activity_info_full: 活动基本信息表
 *   - ods.ods_activity_rule_full: 活动规则表
 *   - ods.ods_activity_sku_full: 活动关联商品表
 * 目标表: dwd.dwd_marketing_activity_full
 * 主要功能: 
 *   1. 整合活动基本信息、规则条件和参与商品
 *   2. 汇总活动规则和参与商品，形成便于分析的字符串格式
 *   3. 动态计算活动状态，反映当前活动进行情况
 *   4. 为营销活动分析和决策提供数据支持
 */

-- 营销域活动全量表
INSERT INTO dwd.dwd_marketing_activity_full(
    id,             -- 记录唯一标识
    k1,             -- 数据日期分区
    activity_id,    -- 活动ID
    activity_name,  -- 活动名称
    activity_type,  -- 活动类型
    start_time,     -- 开始时间
    end_time,       -- 结束时间
    create_time,    -- 创建时间
    activity_desc,  -- 活动描述
    rules,          -- 活动规则(字符串格式)
    sku_ids,        -- 关联商品ID列表
    status          -- 活动状态
)
with
    -- CTE 1: 活动基本信息
    act_info as
    (
        select
            id,                     -- 活动ID
            k1,                     -- 分区字段
            activity_name,          -- 活动名称
            activity_type,          -- 活动类型
            start_time,             -- 开始时间
            end_time,               -- 结束时间
            create_time,            -- 创建时间
            activity_desc           -- 活动描述
        from ods.ods_activity_info_full
        where k1=date('${pdate}')   -- 按分区日期过滤，只处理当天数据
    ),
    -- CTE 2: 活动规则汇总
    act_rule as
    (
        select
            activity_id,            -- 活动ID
            -- 将规则信息聚合为字符串: "满减条件:优惠金额:折扣比例;..." 格式
            GROUP_CONCAT(CONCAT(condition_amount, ':', benefit_amount, ':', benefit_discount), ';') as rules
        from ods.ods_activity_rule_full
        group by activity_id        -- 按活动ID分组聚合
    ),
    -- CTE 3: 活动关联商品汇总
    act_sku as
    (
        select
            activity_id,            -- 活动ID
            -- 将活动关联的所有商品ID合并为逗号分隔的字符串
            GROUP_CONCAT(sku_id, ',') as sku_ids
        from ods.ods_activity_sku_full
        where k1=date('${pdate}')   -- 按分区日期过滤，只处理当天数据
        group by activity_id        -- 按活动ID分组聚合
    )
-- 主查询: 整合活动信息、规则和商品
select
    ai.id,                          -- 使用活动ID作为记录ID
    ai.k1,                          -- 分区字段
    ai.id as activity_id,           -- 活动ID (与id字段相同)
    ai.activity_name,               -- 活动名称
    ai.activity_type,               -- 活动类型
    ai.start_time,                  -- 开始时间
    ai.end_time,                    -- 结束时间
    ai.create_time,                 -- 创建时间
    ai.activity_desc,               -- 活动描述
    ar.rules,                       -- 活动规则(字符串格式)
    sku.sku_ids,                    -- 关联商品ID列表
    -- 根据时间动态设置状态
    case 
        when current_timestamp() < ai.start_time then '未开始'
        when current_timestamp() > ai.end_time then '已结束'
        else '进行中'
    end as status                   -- 根据当前时间和活动时间范围动态计算活动状态
from act_info ai
    left join act_rule ar on ai.id = ar.activity_id  -- 关联活动规则
    left join act_sku sku on ai.id = sku.activity_id; -- 关联活动商品

/*
 * 设计说明:
 * 1. CTE(公共表表达式)设计:
 *    - 使用三个CTE分别处理活动基本信息、规则汇总和商品汇总
 *    - 这种模块化设计使SQL逻辑更清晰，便于维护和调整
 *    - 每个CTE单独负责一项数据处理任务，降低复杂度
 *    
 * 2. 数据聚合设计:
 *    - 使用GROUP_CONCAT函数将活动规则和商品ID合并成易于存储和解析的字符串格式
 *    - 活动规则采用"满减条件:优惠金额:折扣比例;"格式，以分号分隔多条规则
 *    - 商品ID采用逗号分隔的字符串格式，便于前端解析和展示
 *
 * 3. 活动状态计算:
 *    - 根据当前时间与活动开始、结束时间的比较，动态计算活动状态
 *    - 这种设计避免了硬编码状态，确保数据实时反映活动进行情况
 *    - 状态包括"未开始"、"进行中"和"已结束"三种
 *
 * 4. 关联策略:
 *    - 以活动基本信息表为主表，左连接规则和商品信息
 *    - 确保即使活动没有规则或商品，也能保留基本信息记录
 *    - 利用活动ID作为关联键，保持数据一致性
 *
 * 5. 数据应用场景:
 *    - 营销效果分析：评估不同类型活动的销售影响
 *    - 活动规划：根据历史活动数据规划新活动
 *    - 商品促销分析：分析哪些商品经常参与活动及其表现
 *    - 活动监控：实时监控当前进行中的活动状态
 */ 
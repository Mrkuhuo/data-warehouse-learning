/*
 * 文件名: dwd_marketing_coupon_info_full.sql
 * 功能描述: 营销域优惠券全量表 - 整合优惠券基本信息与使用范围
 * 数据粒度: 优惠券
 * 刷新策略: 全量刷新
 * 调度周期: 每日
 * 依赖表: 
 *   - ods.ods_coupon_info_full: 优惠券基本信息表
 *   - ods.ods_coupon_range_full: 优惠券使用范围表
 * 目标表: dwd.dwd_marketing_coupon_info_full
 * 主要功能: 
 *   1. 整合优惠券基本信息与适用范围数据
 *   2. 汇总优惠券适用范围，便于分析和展示
 *   3. 标准化优惠券信息的展示格式
 *   4. 为营销分析和优惠券发放决策提供数据支持
 */

-- 营销域优惠券全量表
INSERT INTO dwd.dwd_marketing_coupon_info_full(
    id,                -- 记录唯一标识
    k1,                -- 数据日期分区
    coupon_id,         -- 优惠券ID
    coupon_name,       -- 优惠券名称
    coupon_type,       -- 优惠券类型
    condition_amount,  -- 使用条件金额
    benefit_amount,    -- 优惠金额
    start_time,        -- 开始时间
    end_time,          -- 结束时间
    create_time,       -- 创建时间
    range_type,        -- 使用范围类型(1:商品 2:品类 3:品牌)
    limit_num,         -- 领取限制数量
    range_ids,         -- 适用范围ID列表
    range_names        -- 适用范围名称列表
)
with
    -- CTE 1: 优惠券基本信息
    cou_info as
    (
        select
            id,                     -- 优惠券ID
            k1,                     -- 分区字段
            coupon_name,            -- 优惠券名称
            coupon_type,            -- 优惠券类型
            condition_amount,       -- 使用条件金额
            benefit_amount,         -- 优惠金额
            create_time,            -- 创建时间
            range_type,             -- 使用范围类型
            limit_num,              -- 领取限制数量
            start_time,             -- 开始时间
            end_time,               -- 结束时间
            operate_time,           -- 操作时间
            expire_time             -- 过期时间
        from ods.ods_coupon_info_full
        where k1=date('${pdate}')   -- 按分区日期过滤，只处理当天数据
    ),
    -- CTE 2: 优惠券使用范围汇总
    cou_range as
    (
        select
            coupon_id,              -- 优惠券ID
            -- 将范围描述格式化为易读的字符串: "类型：ID;"
            GROUP_CONCAT(
                case 
                    when range_type = '1' then concat('商品：', COALESCE(range_id, ''))
                    when range_type = '2' then concat('品类：', COALESCE(range_id, ''))
                    when range_type = '3' then concat('品牌：', COALESCE(range_id, ''))
                    else ''
                end,
                ';'
            ) as range_desc,        -- 范围描述(未使用在最终结果中)
            -- 将所有范围ID合并为分号分隔的字符串
            GROUP_CONCAT(COALESCE(range_id, ''), ';') as range_ids,
            -- 暂时使用ID代替名称，后续可能需要连接其他表获取实际名称
            GROUP_CONCAT(
                case 
                    when range_type = '1' then COALESCE(range_id, '')  -- 商品ID
                    when range_type = '2' then COALESCE(range_id, '')  -- 品类ID
                    when range_type = '3' then COALESCE(range_id, '')  -- 品牌ID
                    else ''
                end,
                ';'
            ) as range_names        -- 范围名称(当前实际存储ID)
        from ods.ods_coupon_range_full
        group by coupon_id          -- 按优惠券ID分组聚合
    )
-- 主查询: 整合优惠券信息和使用范围
select
    ci.id,                          -- 使用优惠券ID作为记录ID
    ci.k1,                          -- 分区字段
    ci.id as coupon_id,             -- 优惠券ID (与id字段相同)
    ci.coupon_name,                 -- 优惠券名称
    ci.coupon_type,                 -- 优惠券类型
    ci.condition_amount,            -- 使用条件金额
    ci.benefit_amount,              -- 优惠金额
    ci.start_time,                  -- 开始时间
    ci.end_time,                    -- 结束时间
    ci.create_time,                 -- 创建时间
    ci.range_type,                  -- 使用范围类型
    ci.limit_num,                   -- 领取限制数量
    cr.range_ids,                   -- 适用范围ID列表
    cr.range_names                  -- 适用范围名称列表(当前实际存储ID)
from cou_info ci
    left join cou_range cr on ci.id = cr.coupon_id; -- 通过优惠券ID关联使用范围

/*
 * 设计说明:
 * 1. CTE(公共表表达式)设计:
 *    - 使用两个CTE分别处理优惠券基本信息和使用范围汇总
 *    - 这种模块化设计使SQL逻辑清晰，便于维护和调整
 *    - 每个CTE专注于其特定的数据处理任务
 *    
 * 2. 范围描述格式化:
 *    - 针对不同的范围类型(商品/品类/品牌)，使用CASE表达式进行格式化
 *    - 将范围ID格式化为"类型：ID"的形式，提高可读性
 *    - 使用GROUP_CONCAT函数将多个范围描述合并为分号分隔的字符串
 *
 * 3. 数据完整性处理:
 *    - 使用COALESCE函数处理可能为空的字段，确保不会因NULL值导致连接失败
 *    - 对于没有设置使用范围的优惠券，通过left join保留其基本信息
 *
 * 4. 名称字段处理:
 *    - 目前range_names字段存储的实际上是ID值，而非名称
 *    - 在实际应用中，可能需要进一步关联商品表、品类表或品牌表获取实际名称
 *    - 这是一个设计权衡，避免在此SQL中引入过多的表关联，保持查询效率
 *
 * 5. 数据应用场景:
 *    - 优惠券使用分析：评估不同类型优惠券的领取和使用情况
 *    - 营销策略优化：根据历史数据调整优惠券发放策略
 *    - 商品促销分析：分析不同商品、品类或品牌的优惠券效果
 *    - 用户画像：结合用户领券数据，分析用户对不同优惠券的偏好
 */ 
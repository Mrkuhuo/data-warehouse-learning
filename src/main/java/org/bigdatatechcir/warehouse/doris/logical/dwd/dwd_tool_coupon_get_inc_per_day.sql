/*
 * 文件名: dwd_tool_coupon_get_inc_per_day.sql
 * 功能描述: 工具域优惠券领取事务事实表(每日增量) - 记录用户每日新增领券行为
 * 数据粒度: 优惠券领取事件
 * 刷新策略: 每日增量加载
 * 调度周期: 每日
 * 依赖表: 
 *   - ods.ods_coupon_use_full: 优惠券使用记录全量表
 * 目标表: dwd.dwd_tool_coupon_get_inc
 * 主要功能: 
 *   1. 提取每日新增优惠券领取记录
 *   2. 生成日期ID字段，便于按日期分析
 *   3. 实时追踪优惠券营销效果
 */

-- 工具域优惠券领取事务事实表(每日增量)
INSERT INTO dwd.dwd_tool_coupon_get_inc(
    id,             -- 领取记录ID
    k1,             -- 数据日期分区
    coupon_id,      -- 优惠券ID
    user_id,        -- 用户ID
    date_id,        -- 日期ID(yyyy-MM-dd格式)
    get_time        -- 领取时间
)
select
    id,                                            -- 领取记录ID
    k1,                                            -- 分区字段
    coupon_id,                                     -- 优惠券ID
    user_id,                                       -- 用户ID
    date_format(get_time,'yyyy-MM-dd') date_id,    -- 将时间戳转换为日期ID
    get_time                                       -- 领取时间
from ods.ods_coupon_use_full                       -- 数据来源：优惠券使用记录表
where k1=date('${pdate}')                          -- 按分区日期过滤，只处理当天数据

/*
 * 设计说明:
 * 1. 每日增量加载策略:
 *    - 使用k1=date('${pdate}')条件过滤优惠券使用表
 *    - 只处理特定日期新增的优惠券领取记录
 *    - 与首次加载脚本(dwd_tool_coupon_get_inc_first.sql)配合使用
 *    
 * 2. 简洁设计:
 *    - 相比其他事实表，本表结构和处理逻辑较为简单
 *    - 不需要额外关联维度表，所有必要字段都在源表中
 *    - 这种简洁设计提高了数据处理效率
 *
 * 3. 时间处理:
 *    - 保留原始get_time字段，便于精确时间分析
 *    - 同时生成date_id字段(yyyy-MM-dd格式)，便于按天汇总分析
 *
 * 4. 数据应用场景:
 *    - 每日领券监控：实时跟踪各类优惠券的领取情况
 *    - 营销活动评估：分析优惠券发放后的领取效果
 *    - 用户活跃度指标：将领券行为作为用户日常活跃的一个指标
 *    - 营销策略优化：根据领取数据调整优惠券发放策略
 */
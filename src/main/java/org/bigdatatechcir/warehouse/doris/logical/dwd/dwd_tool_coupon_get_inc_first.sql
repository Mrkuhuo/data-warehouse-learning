/*
 * 文件名: dwd_tool_coupon_get_inc_first.sql
 * 功能描述: 工具域优惠券领取事务事实表(首次加载) - 记录用户领取优惠券行为
 * 数据粒度: 优惠券领取事件
 * 刷新策略: 首次增量加载
 * 调度周期: 一次性执行
 * 依赖表: 
 *   - ods.ods_coupon_use_full: 优惠券使用记录全量表
 * 目标表: dwd.dwd_tool_coupon_get_inc
 * 主要功能: 
 *   1. 提取历史优惠券领取记录
 *   2. 生成日期ID字段，便于按日期分析
 *   3. 为优惠券营销效果分析提供基础数据
 */

-- 工具域优惠券领取事务事实表(首次加载)
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
-- 注：首次加载不使用k1过滤，加载所有历史数据

/*
 * 设计说明:
 * 1. 首次加载特点:
 *    - 不使用时间过滤条件，加载所有历史优惠券领取记录
 *    - 为DWD层初始化提供完整的历史优惠券领取数据
 *    - 后续日常加载应使用dwd_tool_coupon_get_inc_per_day.sql
 *    
 * 2. 数据源选择:
 *    - 使用ods_coupon_use_full表作为数据源
 *    - 该表记录了优惠券的整个生命周期，包括领取、使用等状态
 *    - 本脚本只关注领取环节，不过滤优惠券使用状态
 *
 * 3. 时间处理:
 *    - 保留原始get_time字段，便于精确时间分析
 *    - 同时生成date_id字段(yyyy-MM-dd格式)，便于按天汇总分析
 *
 * 4. 数据应用场景:
 *    - 优惠券领取趋势：分析用户领券的时间规律和趋势
 *    - 用户偏好分析：了解不同用户对优惠券的领取偏好
 *    - 营销效果评估：结合优惠券使用数据，分析优惠券领取到使用的转化率
 *    - 用户活跃度指标：将领券行为作为用户活跃的一个指标进行追踪
 */
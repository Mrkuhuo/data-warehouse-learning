/*
 * 文件名: dwd_tool_coupon_pay_inc_first.sql
 * 功能描述: 工具域优惠券使用(支付)事务事实表(首次加载) - 记录优惠券支付成功事件
 * 数据粒度: 优惠券支付事件
 * 刷新策略: 首次增量加载
 * 调度周期: 一次性执行
 * 依赖表: 
 *   - ods.ods_coupon_use_full: 优惠券使用全量表
 * 目标表: dwd.dwd_tool_coupon_pay_inc
 * 主要功能: 
 *   1. 提取历史所有优惠券支付成功事件数据
 *   2. 筛选已完成支付的优惠券使用记录
 *   3. 生成日期ID便于时间维度分析
 *   4. 为后续每日增量加载奠定数据基础
 */

-- 工具域优惠券使用(支付)事务事实表
INSERT INTO dwd.dwd_tool_coupon_pay_inc(
    id,            -- 记录唯一标识
    k1,            -- 数据日期分区
    coupon_id,     -- 优惠券ID
    user_id,       -- 用户ID
    order_id,      -- 订单ID
    date_id,       -- 日期ID，格式yyyy-MM-dd
    payment_time   -- 支付时间
)
select
    id,                                         -- 优惠券使用记录ID
    k1,                                         -- 分区字段
    coupon_id,                                  -- 优惠券ID
    user_id,                                    -- 用户ID
    order_id,                                   -- 订单ID
    date_format(used_time,'yyyy-MM-dd') date_id, -- 将支付时间转换为日期ID格式
    used_time                                   -- 优惠券最终使用时间（支付成功时间）
from ods.ods_coupon_use_full
where used_time is not null;                    -- 筛选已支付成功的优惠券记录

/*
 * 设计说明:
 * 1. 首次加载特点:
 *    - 此脚本用于DWD层初始化时一次性执行
 *    - 不使用分区日期(k1)过滤，加载所有历史优惠券支付数据
 *    - 后续日常加载应使用dwd_tool_coupon_pay_inc_per_day.sql
 *    
 * 2. 数据筛选逻辑:
 *    - 通过used_time is not null条件筛选支付成功的优惠券记录
 *    - ods_coupon_use_full表中，used_time表示订单支付成功的时间
 *    - used_time标志着优惠券使用生命周期的最终完成状态
 *    - 优惠券使用有三个状态：领取、下单使用、支付成功
 *
 * 3. 时间字段处理:
 *    - 保留原始used_time作为payment_time，保留完整时间信息
 *    - 同时生成date_id字段(格式yyyy-MM-dd)，便于按天汇总分析
 *    - 此设计支持不同时间粒度的分析需求
 *
 * 4. 执行策略:
 *    - 此脚本应在数仓初始化阶段执行一次
 *    - 执行前应确保目标表为空，避免数据重复
 *    - 执行后应立即切换到每日增量加载模式
 *
 * 5. 与优惠券事实表体系的关系:
 *    - 优惠券相关事实表构成了完整的优惠券生命周期：领取->下单->支付
 *    - 本表关注优惠券支付成功事件，是最终环节
 *    - 可通过coupon_id与dwd_tool_coupon_get_inc表(领取)和dwd_tool_coupon_order_inc表(下单)关联
 *
 * 6. 数据应用场景:
 *    - 优惠券完整转化率分析：计算优惠券从领取到最终支付的完整转化率
 *    - 下单到支付转化分析：分析使用优惠券下单后的支付完成率
 *    - 优惠券ROI计算：计算优惠券投入产出比，评估优惠券价值
 *    - 用户价值分析：分析不同用户群体的优惠券使用完成情况
 *    - 营销活动评估：评估优惠券在营销活动中产生的实际销售转化
 *    - 优惠券策略优化：为不同类型优惠券设计提供数据支持
 */
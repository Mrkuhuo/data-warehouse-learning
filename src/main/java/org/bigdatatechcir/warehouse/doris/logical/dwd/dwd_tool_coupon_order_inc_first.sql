/*
 * 文件名: dwd_tool_coupon_order_inc_first.sql
 * 功能描述: 工具域优惠券使用(下单)事务事实表(首次加载) - 记录用户使用优惠券下单事件
 * 数据粒度: 优惠券使用事件
 * 刷新策略: 首次增量加载
 * 调度周期: 一次性执行
 * 依赖表: 
 *   - ods.ods_coupon_use_full: 优惠券使用全量表
 * 目标表: dwd.dwd_tool_coupon_order_inc
 * 主要功能: 
 *   1. 提取历史所有优惠券下单事件数据
 *   2. 筛选已下单但未支付的优惠券使用记录
 *   3. 生成日期ID便于时间维度分析
 *   4. 为后续每日增量加载奠定数据基础
 */

-- 工具域优惠券使用(下单)事务事实表
INSERT INTO dwd.dwd_tool_coupon_order_inc(
    id,            -- 记录唯一标识
    k1,            -- 数据日期分区
    coupon_id,     -- 优惠券ID
    user_id,       -- 用户ID
    order_id,      -- 订单ID
    date_id,       -- 日期ID，格式yyyy-MM-dd
    order_time     -- 下单时间
)
select
    id,                                         -- 优惠券使用记录ID
    k1,                                         -- 分区字段
    coupon_id,                                  -- 优惠券ID
    user_id,                                    -- 用户ID
    order_id,                                   -- 订单ID
    date_format(using_time,'yyyy-MM-dd') date_id, -- 将下单时间转换为日期ID格式
    using_time                                  -- 优惠券使用时间（下单时间）
from ods.ods_coupon_use_full
where using_time is not null;                   -- 筛选已使用(下单)的优惠券记录

/*
 * 设计说明:
 * 1. 首次加载特点:
 *    - 此脚本用于DWD层初始化时一次性执行
 *    - 不使用分区日期(k1)过滤，加载所有历史优惠券下单数据
 *    - 后续日常加载应使用dwd_tool_coupon_order_inc_per_day.sql
 *    
 * 2. 数据筛选逻辑:
 *    - 通过using_time is not null条件筛选已使用(下单)的优惠券记录
 *    - 优惠券使用有三个状态：领取、下单使用、支付成功
 *    - 此表专注于下单使用阶段，对应订单已创建但可能未支付的状态
 *
 * 3. 时间字段处理:
 *    - 保留原始using_time作为order_time，保留完整时间信息
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
 *    - 本表关注优惠券下单事件，是中间环节
 *    - 可通过coupon_id与dwd_tool_coupon_get_inc表(领取)和dwd_tool_coupon_pay_inc表(支付)关联
 *
 * 6. 数据应用场景:
 *    - 优惠券转化率分析：计算优惠券从领取到下单的转化率
 *    - 下单到支付转化分析：分析使用优惠券下单后的支付完成率
 *    - 优惠券策略优化：评估不同优惠券对促进下单的效果
 *    - 用户行为分析：分析用户使用优惠券下单的时间分布特征
 *    - 营销活动评估：评估优惠券在营销活动中促进下单的效果
 */
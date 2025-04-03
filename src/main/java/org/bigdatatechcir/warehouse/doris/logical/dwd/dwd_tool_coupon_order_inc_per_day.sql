/*
 * 文件名: dwd_tool_coupon_order_inc_per_day.sql
 * 功能描述: 工具域优惠券使用(下单)事务事实表(每日增量) - 记录用户使用优惠券下单事件
 * 数据粒度: 优惠券使用事件
 * 刷新策略: 每日增量加载
 * 调度周期: 每日
 * 依赖表: 
 *   - ods.ods_coupon_use_full: 优惠券使用全量表
 * 目标表: dwd.dwd_tool_coupon_order_inc
 * 主要功能: 
 *   1. 提取每日新增的优惠券下单事件数据
 *   2. 筛选当日已下单但未支付的优惠券使用记录
 *   3. 生成日期ID便于时间维度分析
 *   4. 为营销效果分析提供数据支持
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
where using_time is not null                    -- 筛选已使用(下单)的优惠券记录
and k1=date('${pdate}')                         -- 按分区日期筛选，只处理当天数据

/*
 * 设计说明:
 * 1. 每日增量加载特点:
 *    - 此脚本在完成首次历史数据加载后，每日定时执行
 *    - 使用k1=date('${pdate}')条件，仅处理当天新增或变更的数据
 *    - 配合首次加载脚本(dwd_tool_coupon_order_inc_first.sql)共同维护完整数据
 *    
 * 2. 数据筛选逻辑:
 *    - 通过using_time is not null条件筛选已使用(下单)的优惠券记录
 *    - 优惠券使用有三个状态：领取、下单使用、支付成功
 *    - 此表专注于下单使用阶段，对应订单已创建但可能未支付的状态
 *
 * 3. 时间字段处理:
 *    - 保留原始using_time作为order_time，保留完整时间信息
 *    - 同时生成date_id字段(格式yyyy-MM-dd)，便于按天汇总分析
 *    - date_id可能与k1不同，因为using_time可能不在当前处理日期
 *
 * 4. 数据一致性考量:
 *    - 优惠券在下单后可能出现取消、退款等状态变更
 *    - 此表记录下单事件本身，状态变更由其他表或指标反映
 *    - 建议与dwd_tool_coupon_pay_inc表结合分析，获取完整视图
 *
 * 5. 与优惠券事实表体系的关系:
 *    - 优惠券相关事实表构成了完整的优惠券生命周期：领取->下单->支付
 *    - 本表关注优惠券下单事件，是中间环节
 *    - 可通过coupon_id与dwd_tool_coupon_get_inc表(领取)和dwd_tool_coupon_pay_inc表(支付)关联
 *
 * 6. 数据应用场景:
 *    - 每日优惠券使用监控：监控当日优惠券的下单使用情况
 *    - 优惠券活动实时评估：评估正在进行的优惠券活动效果
 *    - 用户行为分析：分析用户使用优惠券下单的时间分布特征
 *    - 优惠券类型分析：比较不同类型优惠券的下单转化效果
 *    - 优惠券策略调整：基于最新数据调整优惠券发放和使用策略
 */
/*
 * 文件名: dwd_tool_coupon_pay_inc_per_day.sql
 * 功能描述: 工具域优惠券使用(支付)事务事实表(每日增量) - 记录优惠券支付成功事件
 * 数据粒度: 优惠券支付事件
 * 刷新策略: 每日增量加载
 * 调度周期: 每日
 * 依赖表: 
 *   - ods.ods_coupon_use_full: 优惠券使用全量表
 * 目标表: dwd.dwd_tool_coupon_pay_inc
 * 主要功能: 
 *   1. 提取每日新增的优惠券支付成功事件数据
 *   2. 筛选当日完成支付的优惠券使用记录
 *   3. 生成日期ID便于时间维度分析
 *   4. 为营销效果评估和ROI计算提供数据支持
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
where used_time is not null                     -- 筛选已支付成功的优惠券记录
and k1=date('${pdate}');                        -- 按分区日期筛选，只处理当天数据

/*
 * 设计说明:
 * 1. 每日增量加载特点:
 *    - 此脚本在完成首次历史数据加载后，每日定时执行
 *    - 使用k1=date('${pdate}')条件，仅处理当天新增或变更的数据
 *    - 配合首次加载脚本(dwd_tool_coupon_pay_inc_first.sql)共同维护完整数据
 *    
 * 2. 数据筛选逻辑:
 *    - 通过used_time is not null条件筛选支付成功的优惠券记录
 *    - ods_coupon_use_full表中，used_time表示订单支付成功的时间
 *    - used_time标志着优惠券使用生命周期的最终完成状态
 *    - 同一个优惠券记录从下单到支付可能跨越多天，由不同的增量脚本捕获
 *
 * 3. 时间字段处理:
 *    - 保留原始used_time作为payment_time，保留完整时间信息
 *    - 同时生成date_id字段(格式yyyy-MM-dd)，便于按天汇总分析
 *    - date_id可能与k1不同，因为used_time可能不在当前处理日期
 *
 * 4. 数据一致性考量:
 *    - 优惠券支付成功是最终状态，但后续可能有退款操作
 *    - 此表只关注支付成功事件，退款等后续状态需要通过其他表关联分析
 *    - 建议与订单表和退款表结合分析，获取更完整的业务视图
 *
 * 5. 与优惠券事实表体系的关系:
 *    - 优惠券相关事实表构成了完整的优惠券生命周期：领取->下单->支付
 *    - 本表关注优惠券支付成功事件，是最终环节
 *    - 可通过coupon_id与dwd_tool_coupon_get_inc表(领取)和dwd_tool_coupon_order_inc表(下单)关联
 *
 * 6. 数据应用场景:
 *    - 每日优惠券效果监控：监控当日优惠券带来的实际销售转化
 *    - 优惠券活动实时评估：评估正在进行的优惠券活动ROI
 *    - 用户响应速度分析：分析用户从领取优惠券到最终支付的时间间隔
 *    - 优惠券类型效果比较：比较不同类型优惠券的最终转化率
 *    - 优惠券策略调整：基于最新转化数据调整优惠券发放策略
 *    - 销售预测与分析：基于优惠券支付转化趋势进行销售预测
 */
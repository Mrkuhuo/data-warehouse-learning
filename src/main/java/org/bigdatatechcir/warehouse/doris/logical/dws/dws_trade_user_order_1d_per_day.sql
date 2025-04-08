/*
 * 文件名: dws_trade_user_order_1d_per_day.sql
 * 功能描述: 交易域用户粒度订单最近1日汇总表每日增量加载脚本
 * 数据粒度: 用户ID + 日期
 * 刷新策略: 每日增量刷新
 * 调度周期: 每日执行
 * 调度依赖:
 *   - dwd层订单明细增量表数据已准备完毕
 * 数据来源:
 *   - dwd.dwd_trade_order_detail_inc: 订单明细增量表
 * 目标表: dws.dws_trade_user_order_1d
 */

-- 交易域用户粒度订单最近1日汇总表
INSERT INTO dws.dws_trade_user_order_1d(
    user_id,                    -- 用户ID
    k1,                        -- 数据日期
    order_count_1d,            -- 最近1日订单次数
    order_num_1d,              -- 最近1日订单商品件数
    order_original_amount_1d,  -- 最近1日订单原始金额
    activity_reduce_amount_1d, -- 最近1日活动优惠金额
    coupon_reduce_amount_1d,   -- 最近1日优惠券优惠金额
    order_total_amount_1d      -- 最近1日订单总金额
)
SELECT
    user_id,                    -- 用户ID
    k1,                        -- 数据日期
    COUNT(DISTINCT(order_id)), -- 统计用户最近1日的订单次数
    SUM(sku_num),              -- 统计用户最近1日的订单商品总件数
    SUM(split_original_amount),-- 统计用户最近1日的订单原始总金额
    SUM(NVL(split_activity_amount,0)), -- 统计用户最近1日的活动优惠总金额，NULL值转为0
    SUM(NVL(split_coupon_amount,0)),   -- 统计用户最近1日的优惠券优惠总金额，NULL值转为0
    SUM(split_total_amount)    -- 统计用户最近1日的订单实际支付总金额
FROM 
    dwd.dwd_trade_order_detail_inc  -- 从订单明细增量表获取数据
WHERE 
    k1 = date('${pdate}')  -- 只处理指定日期的数据
GROUP BY 
    user_id, k1;  -- 按用户ID和日期分组统计

/*
 * 脚本设计说明:
 * 1. 数据处理逻辑:
 *    - 从订单明细增量表获取用户订单数据
 *    - 只处理指定日期(${pdate})的数据
 *    - 按用户ID和日期分组统计各项指标
 *    - 使用NVL函数处理NULL值，确保金额计算的准确性
 *
 * 2. 指标计算方法:
 *    - 订单次数：使用COUNT DISTINCT统计不重复的订单ID数
 *    - 订单商品件数：使用SUM统计所有商品数量
 *    - 订单金额：使用SUM统计各类金额，包括原始金额、优惠金额和实际支付金额
 *
 * 3. 数据质量保障:
 *    - 使用WHERE条件确保只处理指定日期的数据
 *    - 使用NVL函数处理NULL值，避免金额计算错误
 *    - 使用DISTINCT确保订单次数的准确性
 *
 * 4. 优化考虑:
 *    - 增量处理：只处理当天的数据，提高处理效率
 *    - 分组统计：使用GROUP BY提高查询效率
 *    - NULL值处理：使用NVL函数确保金额计算准确性
 */
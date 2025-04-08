/*
 * 文件名: dws_trade_user_order_nd.sql
 * 功能描述: 交易域用户粒度订单最近N日汇总表加载脚本
 * 数据粒度: 用户ID + 日期
 * 刷新策略: 每日增量刷新
 * 调度周期: 每日执行
 * 调度依赖:
 *   - dws层用户粒度订单最近1日汇总表数据已准备完毕
 * 数据来源:
 *   - dws.dws_trade_user_order_1d: 用户粒度订单最近1日汇总表
 * 目标表: dws.dws_trade_user_order_nd
 */

-- 交易域用户粒度订单最近N日汇总表
INSERT INTO dws.dws_trade_user_order_nd(
    user_id,                    -- 用户ID
    k1,                        -- 数据日期
    order_count_7d,            -- 最近7日订单次数
    order_num_7d,              -- 最近7日订单商品件数
    order_original_amount_7d,  -- 最近7日订单原始金额
    activity_reduce_amount_7d, -- 最近7日活动优惠金额
    coupon_reduce_amount_7d,   -- 最近7日优惠券优惠金额
    order_total_amount_7d,     -- 最近7日订单总金额
    order_count_30d,           -- 最近30日订单次数
    order_num_30d,             -- 最近30日订单商品件数
    order_original_amount_30d, -- 最近30日订单原始金额
    activity_reduce_amount_30d,-- 最近30日活动优惠金额
    coupon_reduce_amount_30d,  -- 最近30日优惠券优惠金额
    order_total_amount_30d     -- 最近30日订单总金额
)
SELECT
    user_id,                    -- 用户ID
    k1,                        -- 数据日期
    -- 最近7日指标：只统计最近7天的数据
    SUM(IF(k1 >= date_add(date('${pdate}'), -6), order_count_1d, 0)),            -- 最近7日订单次数
    SUM(IF(k1 >= date_add(date('${pdate}'), -6), order_num_1d, 0)),              -- 最近7日订单商品件数
    SUM(IF(k1 >= date_add(date('${pdate}'), -6), order_original_amount_1d, 0)),  -- 最近7日订单原始金额
    SUM(IF(k1 >= date_add(date('${pdate}'), -6), activity_reduce_amount_1d, 0)), -- 最近7日活动优惠金额
    SUM(IF(k1 >= date_add(date('${pdate}'), -6), coupon_reduce_amount_1d, 0)),   -- 最近7日优惠券优惠金额
    SUM(IF(k1 >= date_add(date('${pdate}'), -6), order_total_amount_1d, 0)),     -- 最近7日订单总金额
    -- 最近30日指标：统计所有30天内的数据
    SUM(order_count_1d),            -- 最近30日订单次数
    SUM(order_num_1d),              -- 最近30日订单商品件数
    SUM(order_original_amount_1d),  -- 最近30日订单原始金额
    SUM(activity_reduce_amount_1d), -- 最近30日活动优惠金额
    SUM(coupon_reduce_amount_1d),   -- 最近30日优惠券优惠金额
    SUM(order_total_amount_1d)      -- 最近30日订单总金额
FROM 
    dws.dws_trade_user_order_1d  -- 从用户粒度订单最近1日汇总表获取数据
WHERE 
    -- 只处理最近30天的数据
    k1 >= date_add(date('${pdate}'), -29)
    AND k1 <= date('${pdate}')
GROUP BY 
    user_id, k1;  -- 按用户ID和日期分组统计

/*
 * 脚本设计说明:
 * 1. 数据处理逻辑:
 *    - 从用户粒度订单最近1日汇总表获取数据
 *    - 只处理最近30天的数据
 *    - 使用条件判断分别计算7日和30日的指标
 *    - 7日指标：只统计最近7天的数据
 *    - 30日指标：统计所有30天内的数据
 *
 * 2. 指标计算方法:
 *    - 7日指标：使用IF条件筛选最近7天数据后SUM
 *    - 30日指标：直接SUM所有30天内的数据
 *    - 所有金额指标：包括原始金额、活动优惠金额、优惠券优惠金额和订单总金额
 *
 * 3. 数据质量保障:
 *    - 使用WHERE条件确保只处理最近30天的数据
 *    - 使用IF条件确保7日指标只统计最近7天
 *    - 通过GROUP BY确保数据不重复统计
 *
 * 4. 优化考虑:
 *    - 增量处理：只处理最近30天的数据，提高处理效率
 *    - 分组统计：使用GROUP BY提高查询效率
 *    - 条件聚合：使用IF条件实现灵活的统计窗口
 */
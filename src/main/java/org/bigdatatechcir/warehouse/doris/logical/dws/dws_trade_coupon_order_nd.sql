/*
 * 文件名: dws_trade_coupon_order_nd.sql
 * 功能描述: 交易域优惠券粒度订单最近n日汇总表加载脚本
 * 数据粒度: 优惠券 + 日期
 * 调度周期: 每日执行
 * 调度依赖:
 *   - dwd层订单明细事实表数据已准备完毕
 *   - dwd层优惠券订单明细事实表数据已准备完毕
 *   - dim层优惠券维度表数据已准备完毕
 * 数据来源:
 *   - dwd.dwd_trade_order_detail_inc: 交易域订单明细事实表
 *   - dwd.dwd_tool_coupon_order_inc: 工具域优惠券下单事实表
 *   - dim.dim_coupon_full: 优惠券维度表
 * 目标表: dws.dws_trade_coupon_order_nd
 * 聚合周期: 最近30日
 */

-- 将INSERT和WITH合并为一个语句
INSERT INTO dws.dws_trade_coupon_order_nd (
    coupon_id, k1, coupon_name, coupon_type_code, coupon_type_name, 
    coupon_rule, start_date, original_amount_30d, coupon_reduce_amount_30d
)
WITH
-- 当前日期参数化
current_date_param AS (
    SELECT DATE('${pdate}') AS cur_date
),

-- 获取优惠券维度信息
coupon_dim AS (
    SELECT
        id AS coupon_id,                              -- 优惠券ID
        coupon_name,                                  -- 优惠券名称
        coupon_type AS coupon_type_code,              -- 优惠券类型编码
        coupon_type AS coupon_type_name,              -- 优惠券类型名称（与类型编码相同）
        CONCAT(
            '满', CAST(condition_amount AS STRING), '元减', 
            CAST(benefit_amount AS STRING), '元'
        ) AS coupon_rule,                             -- 构造优惠券规则描述
        DATE(start_time) AS start_date                -- 从开始时间提取日期部分
    FROM 
        dim.dim_coupon_full
    WHERE 
        k1 = (
            SELECT MAX(k1) FROM dim.dim_coupon_full 
            WHERE k1 <= DATE('${pdate}')
        )
),

-- 计算优惠券订单明细聚合数据
coupon_order_detail AS (
    SELECT
        cpn.coupon_id,                                -- 优惠券ID
        cp.cur_date,                                  -- 当前处理日期
        -- 30日汇总指标
        SUM(CASE 
            WHEN od.k1 BETWEEN DATE_SUB(cp.cur_date, 29) AND cp.cur_date 
            THEN od.split_original_amount 
            ELSE 0 
        END) AS original_amount_30d,                  -- 最近30日使用优惠券订单原始金额
        SUM(CASE 
            WHEN od.k1 BETWEEN DATE_SUB(cp.cur_date, 29) AND cp.cur_date 
            THEN COALESCE(od.split_coupon_amount, 0.0) 
            ELSE 0 
        END) AS coupon_reduce_amount_30d              -- 最近30日优惠券优惠金额
    FROM
        dwd.dwd_tool_coupon_order_inc cpn             -- 工具域优惠券下单事实表
    JOIN
        dwd.dwd_trade_order_detail_inc od             -- 订单明细表
        ON cpn.order_id = od.order_id                 -- 通过订单ID关联
        AND cpn.coupon_id = od.coupon_id              -- 同时确保优惠券ID一致
    JOIN
        current_date_param cp
    WHERE
        -- 过滤最近30天数据，提高查询性能
        od.k1 BETWEEN DATE_SUB(cp.cur_date, 29) AND cp.cur_date
    GROUP BY
        cpn.coupon_id, cp.cur_date
)

-- PART 2: 生成最终结果，关联优惠券维度信息和优惠券订单数据
SELECT
    COALESCE(od.coupon_id, dim.coupon_id) AS coupon_id,  -- 优惠券ID
    DATE('${pdate}') AS k1,                               -- 分区日期，使用固定日期参数
    COALESCE(dim.coupon_name, '') AS coupon_name,         -- 优惠券名称
    COALESCE(dim.coupon_type_code, '') AS coupon_type_code, -- 优惠券类型编码
    COALESCE(dim.coupon_type_name, '') AS coupon_type_name, -- 优惠券类型名称（与类型编码相同）
    COALESCE(dim.coupon_rule, '') AS coupon_rule,         -- 优惠券规则
    COALESCE(dim.start_date, DATE('${pdate}')) AS start_date, -- 优惠券开始日期
    COALESCE(od.original_amount_30d, 0) AS original_amount_30d,  -- 使用下单原始金额
    COALESCE(od.coupon_reduce_amount_30d, 0) AS coupon_reduce_amount_30d  -- 使用下单优惠金额
FROM
    -- 左侧：优惠券维度信息
    coupon_dim dim
FULL JOIN
    -- 右侧：优惠券订单汇总数据
    (
        SELECT 
            coupon_id, 
            cur_date,
            original_amount_30d, 
            coupon_reduce_amount_30d
        FROM 
            coupon_order_detail
    ) od
    ON dim.coupon_id = od.coupon_id
CROSS JOIN
    current_date_param cp
WHERE
    -- 过滤条件：确保至少有维度或者有订单数据的优惠券被保留
    (dim.coupon_id IS NOT NULL OR od.coupon_id IS NOT NULL);

/*
 * 说明：
 * 1. 本脚本采用CTE（Common Table Expression）结构，分步计算各类数据：
 *    - current_date_param: 定义当前处理日期
 *    - coupon_dim: 获取优惠券维度信息
 *    - coupon_order_detail: 计算优惠券订单汇总指标
 *
 * 2. 查询结构和优化：
 *    - 使用FULL JOIN确保同时包含有订单数据的优惠券和没有订单但存在于维度表的优惠券
 *    - 使用COALESCE处理NULL值，确保数值字段默认为0
 *    - 使用日期过滤条件提高查询效率
 *
 * 3. 业务计算逻辑：
 *    - 原始金额：使用该优惠券下单的订单原始总金额
 *    - 优惠金额：优惠券带来的实际优惠总额
 *    - 使用率计算：可通过 coupon_reduce_amount_30d / original_amount_30d 计算优惠券的折扣力度
 *
 * 4. 分析维度建议：
 *    - 按优惠券类型：分析不同类型优惠券的效果
 *    - 按优惠力度：分析不同折扣力度的转化效果
 *    - 按时间趋势：分析优惠券使用的周期性规律
 *
 * 5. 使用场景举例：
 *    - 营销效果评估：评估不同优惠券策略的销售带动效果
 *    - 优惠券设计：为新优惠券活动提供历史数据参考
 *    - 用户偏好分析：了解用户对不同类型优惠券的偏好
 *    - 促销决策：指导未来优惠券发放策略和力度
 */
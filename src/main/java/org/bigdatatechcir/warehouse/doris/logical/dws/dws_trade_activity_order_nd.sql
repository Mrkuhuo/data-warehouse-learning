/*
 * 文件名: dws_trade_activity_order_nd.sql
 * 功能描述: 交易域活动粒度订单最近n日汇总表加载脚本
 * 数据粒度: 活动 + 日期
 * 调度周期: 每日执行
 * 调度依赖:
 *   - dwd层订单明细事实表数据已准备完毕
 *   - dim层活动维度表数据已准备完毕
 * 数据来源:
 *   - dwd.dwd_trade_order_detail_inc: 交易域订单明细事实表(包含活动ID)
 *   - dwd.dwd_marketing_activity_full: 营销域活动全量表
 *   - dim.dim_activity_full: 活动维度表
 * 目标表: dws.dws_trade_activity_order_nd
 * 聚合周期: 最近30日
 */

-- 将INSERT和WITH合并为一个语句
INSERT INTO dws.dws_trade_activity_order_nd (
    activity_id, k1, activity_name, activity_type_code, activity_type_name, 
    start_date, original_amount_30d, activity_reduce_amount_30d
)
WITH
-- 当前日期参数化
current_date_param AS (
    SELECT DATE('${pdate}') AS cur_date
),

-- 获取活动维度信息
activity_dim AS (
    SELECT
        id AS activity_id,                            -- 活动ID
        activity_name,                                -- 活动名称
        activity_type AS activity_type_code,          -- 活动类型编码
        activity_type AS activity_type_name,          -- 活动类型名称(与编码相同)
        DATE(start_time) AS start_date                -- 活动开始日期
    FROM 
        dim.dim_activity_full
    WHERE 
        k1 = (
            SELECT MAX(k1) FROM dim.dim_activity_full 
            WHERE k1 <= DATE('${pdate}')
        )
),

-- 计算活动订单明细聚合数据(直接从订单明细表获取活动相关数据)
activity_order_detail AS (
    SELECT
        od.activity_id,                              -- 活动ID
        cp.cur_date,                                 -- 当前处理日期
        -- 30日汇总指标
        SUM(CASE 
            WHEN od.k1 BETWEEN DATE_SUB(cp.cur_date, 29) AND cp.cur_date 
            THEN od.split_original_amount 
            ELSE 0 
        END) AS original_amount_30d,                  -- 最近30日参与活动订单原始金额
        SUM(CASE 
            WHEN od.k1 BETWEEN DATE_SUB(cp.cur_date, 29) AND cp.cur_date 
            THEN COALESCE(od.split_activity_amount, 0.0) 
            ELSE 0 
        END) AS activity_reduce_amount_30d            -- 最近30日参与活动订单优惠金额
    FROM
        dwd.dwd_trade_order_detail_inc od             -- 订单明细表(已包含活动ID)
    JOIN
        current_date_param cp
    WHERE
        -- 过滤最近30天数据，提高查询性能
        od.k1 BETWEEN DATE_SUB(cp.cur_date, 29) AND cp.cur_date
        AND od.activity_id IS NOT NULL               -- 只统计有活动ID的订单
    GROUP BY
        od.activity_id, cp.cur_date
)

-- PART 2: 生成最终结果，关联活动维度信息和活动订单数据
SELECT
    COALESCE(od.activity_id, dim.activity_id) AS activity_id,  -- 活动ID
    DATE('${pdate}') AS k1,                                     -- 分区日期，使用固定日期参数
    COALESCE(dim.activity_name, '') AS activity_name,           -- 活动名称
    COALESCE(dim.activity_type_code, '') AS activity_type_code, -- 活动类型编码
    COALESCE(dim.activity_type_name, '') AS activity_type_name, -- 活动类型名称
    COALESCE(dim.start_date, DATE('${pdate}')) AS start_date,   -- 活动开始日期
    COALESCE(od.original_amount_30d, 0) AS original_amount_30d, -- 参与活动订单原始金额
    COALESCE(od.activity_reduce_amount_30d, 0) AS activity_reduce_amount_30d -- 参与活动订单优惠金额
FROM
    -- 左侧：活动维度信息
    activity_dim dim
FULL JOIN
    -- 右侧：活动订单汇总数据
    (
        SELECT 
            activity_id, 
            cur_date,
            original_amount_30d, 
            activity_reduce_amount_30d
        FROM 
            activity_order_detail
    ) od
    ON dim.activity_id = od.activity_id
CROSS JOIN
    current_date_param cp  -- 使用CROSS JOIN确保每条记录都有日期值
WHERE
    -- 过滤条件：确保至少有维度或者有订单数据的活动被保留
    (dim.activity_id IS NOT NULL OR od.activity_id IS NOT NULL);
    -- 如果需要仅保留有订单金额的活动，可以使用下面的条件替代
    -- AND COALESCE(od.original_amount_30d, 0) > 0;

/*
 * 说明：
 * 1. 本脚本采用CTE（Common Table Expression）结构，分步计算各类数据：
 *    - current_date_param: 定义当前处理日期
 *    - activity_dim: 获取活动维度信息
 *    - activity_order_detail: 计算活动订单汇总指标
 *
 * 2. 查询结构和优化：
 *    - 使用FULL JOIN确保同时包含有订单数据的活动和没有订单但存在于维度表的活动
 *    - 使用COALESCE处理NULL值，确保数值字段默认为0
 *    - 使用日期过滤条件提高查询效率
 *
 * 3. 应用场景说明：
 *    - 活动效果评估：通过原始金额和优惠金额计算活动转化率
 *    - 活动ROI分析：评估活动投入与销售增长的比例
 *    - 活动类型比较：对比不同类型活动的效果
 *    - 活动趋势监控：监控活动效果随时间的变化
 *
 * 4. 可扩展性：
 *    - 可以增加1日、7日等其他时间窗口的汇总指标
 *    - 可以添加订单数、用户数等其他维度的指标
 *    - 可以计算活动优惠力度（优惠金额/原始金额）等衍生指标
 *
 * 5. 注意事项：
 *    - 数据质量监控：应定期检查活动ID的一致性
 *    - 活动类型维护：确保活动类型编码和名称的映射关系正确
 *    - 历史活动处理：可考虑添加过滤条件，仅保留最近一年的活动数据
 */
/*
 * 文件名: dws_trade_province_order_1d.sql
 * 功能描述: 交易域省份粒度订单最近1日汇总表加载脚本
 * 数据粒度: 省份 + 日期
 * 调度周期: 每日执行
 * 调度依赖:
 *   - dwd层订单明细事实表数据已准备完毕
 *   - dim层省份维度表数据已准备完毕
 * 数据来源:
 *   - dwd.dwd_trade_order_detail_inc: 交易域订单明细事实表
 *   - dim.dim_province_full: 省份维度表
 * 目标表: dws.dws_trade_province_order_1d
 * 聚合周期: 最近1日
 */

-- PART 1: 生成省份订单基础数据
-- PART 2: 生成最终结果，关联省份维度信息和省份订单数据
INSERT INTO dws.dws_trade_province_order_1d (
    province_id, k1, province_name, area_code, iso_code, iso_3166_2,
    order_count_1d, order_original_amount_1d, activity_reduce_amount_1d,
    coupon_reduce_amount_1d, order_total_amount_1d
)
WITH
-- 当前日期参数化
current_date_param AS (
    SELECT DATE('${pdate}') AS cur_date
),

-- 获取省份维度信息
province_dim AS (
    SELECT
        id AS province_id,                            -- 省份ID
        province_name,                                -- 省份名称
        area_code,                                    -- 地区编码
        iso_code,                                     -- 旧版ISO-3166-2编码
        iso_3166_2                                    -- 新版ISO-3166-2编码
    FROM
        dim.dim_province_full
),

-- 计算省份订单聚合数据
province_order_detail AS (
    SELECT
        province_id,                                  -- 省份ID
        k1,                                           -- 当前处理日期
        COUNT(DISTINCT order_id) AS order_count_1d,   -- 订单数
        SUM(split_original_amount) AS order_original_amount_1d,  -- 原始金额
        SUM(COALESCE(split_activity_amount, 0.0)) AS activity_reduce_amount_1d,  -- 活动优惠金额
        SUM(COALESCE(split_coupon_amount, 0.0)) AS coupon_reduce_amount_1d,      -- 优惠券优惠金额
        SUM(split_total_amount) AS order_total_amount_1d  -- 最终金额
    FROM
        dwd.dwd_trade_order_detail_inc
    WHERE
        -- 只处理当天的数据
        k1 = DATE('${pdate}')
    GROUP BY
        province_id, k1
)

-- 最终结果查询
SELECT
    COALESCE(od.province_id, dim.province_id) AS province_id,  -- 省份ID
    DATE('${pdate}') AS k1,                                   -- 分区日期
    dim.province_name,                                         -- 省份名称
    dim.area_code,                                             -- 地区编码
    dim.iso_code,                                              -- 旧版ISO-3166-2编码
    dim.iso_3166_2,                                            -- 新版ISO-3166-2编码
    COALESCE(od.order_count_1d, 0) AS order_count_1d,          -- 订单数
    COALESCE(od.order_original_amount_1d, 0) AS order_original_amount_1d,  -- 原始金额
    COALESCE(od.activity_reduce_amount_1d, 0) AS activity_reduce_amount_1d,  -- 活动优惠金额
    COALESCE(od.coupon_reduce_amount_1d, 0) AS coupon_reduce_amount_1d,      -- 优惠券优惠金额
    COALESCE(od.order_total_amount_1d, 0) AS order_total_amount_1d            -- 最终金额
FROM
    -- 左侧：省份维度信息
    province_dim dim
    FULL JOIN
    -- 右侧：省份订单汇总数据
    province_order_detail od
    ON dim.province_id = od.province_id
WHERE
    -- 过滤条件：确保至少有维度或者有订单数据的省份被保留
    (dim.province_id IS NOT NULL OR od.province_id IS NOT NULL);

/*
 * 说明：
 * 1. 本脚本采用CTE（Common Table Expression）结构，分步计算各类数据：
 *    - current_date_param: 定义当前处理日期
 *    - province_dim: 获取省份维度信息
 *    - province_order_detail: 计算省份订单汇总指标
 *
 * 2. 查询结构和优化：
 *    - 使用FULL JOIN确保同时包含有订单数据的省份和没有订单但存在于维度表的省份
 *    - 使用COALESCE处理NULL值，确保数值字段默认为0
 *    - 使用日期过滤条件，只处理当天数据，提高查询效率
 *
 * 3. 业务分析价值：
 *    - 地域差异分析：了解不同省份的消费能力和购买偏好
 *    - 区域营销效果：通过活动和优惠券金额分析各地区营销策略效果
 *    - 销售预测基础：提供按地区分析的历史数据，支持销售预测
 *    - 市场开拓指导：识别高潜力和低渗透率的地区，指导市场开拓
 *
 * 4. 数据质量考量：
 *    - 省份数据准确性：确保订单中的省份ID能够与省份维度表正确关联
 *    - 数据完整性：使用FULL JOIN保证不遗漏任何省份数据
 *    - 默认值处理：对于没有订单的省份，相关指标设置为0而非NULL
 *
 * 5. 后续应用建议：
 *    - 结合地图可视化，创建销售热力图
 *    - 与人口和GDP等外部数据结合，计算渗透率和消费能力指标
 *    - 建立省份销售环比/同比分析报表，监控区域销售趋势
 *    - 根据省份消费特点，制定差异化的营销和产品策略
 */ 
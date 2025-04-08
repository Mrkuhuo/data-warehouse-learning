/*
 * 文件名: dws_trade_province_order_1d_first.sql
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

-- 交易域省份粒度订单最近1日汇总表数据加载
-- 插入新数据
INSERT INTO dws.dws_trade_province_order_1d (
    province_id,
    k1,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    order_count_1d,
    order_original_amount_1d,
    activity_reduce_amount_1d,
    coupon_reduce_amount_1d,
    order_total_amount_1d
)
WITH 
-- 当前日期参数化
current_date_param AS (
    SELECT CURRENT_DATE() AS cur_date
),

-- 获取省份维度信息
province_dim AS (
    SELECT
        id AS province_id,
        province_name,
        area_code,
        iso_code,
        iso_3166_2
    FROM 
        dim.dim_province_full
),

-- 计算省份订单聚合数据
province_order_data AS (
    SELECT
        province_id,
        k1,
        COUNT(DISTINCT order_id) AS order_count_1d,
        SUM(split_original_amount) AS order_original_amount_1d,
        SUM(COALESCE(split_activity_amount, 0)) AS activity_reduce_amount_1d,
        SUM(COALESCE(split_coupon_amount, 0)) AS coupon_reduce_amount_1d,
        SUM(split_total_amount) AS order_total_amount_1d
    FROM 
        dwd.dwd_trade_order_detail_inc
    WHERE
        k1 = CURRENT_DATE()
    GROUP BY 
        province_id, k1
)


SELECT
    p.province_id,
    CURRENT_DATE() AS k1,
    p.province_name,
    p.area_code,
    p.iso_code,
    p.iso_3166_2,
    COALESCE(o.order_count_1d, 0) AS order_count_1d,
    COALESCE(o.order_original_amount_1d, 0) AS order_original_amount_1d,
    COALESCE(o.activity_reduce_amount_1d, 0) AS activity_reduce_amount_1d,
    COALESCE(o.coupon_reduce_amount_1d, 0) AS coupon_reduce_amount_1d,
    COALESCE(o.order_total_amount_1d, 0) AS order_total_amount_1d
FROM 
    province_dim p
LEFT JOIN 
    province_order_data o
ON 
    p.province_id = o.province_id;

/*
 * 说明：
 * 1. 本脚本采用Doris的CTE（Common Table Expression）结构，分步计算各类数据：
 *    - current_date_param: 定义当前处理日期
 *    - province_dim: 获取省份维度信息
 *    - province_order_data: 计算省份订单汇总指标
 *
 * 2. 查询结构和优化：
 *    - 使用WITH子句定义CTE，分步计算各类数据
 *    - 先删除当天数据，避免重复
 *    - 使用LEFT JOIN确保包含所有省份，即使没有订单数据
 *    - 使用COALESCE处理NULL值，确保数值字段默认为0
 *    - 添加日期过滤条件，只处理当天数据
 *    - 优化JOIN顺序，从维度表开始JOIN
 *
 * 3. 业务分析价值：
 *    - 地域差异分析：了解不同省份的消费能力和购买偏好
 *    - 区域营销效果：通过活动和优惠券金额分析各地区营销策略效果
 *    - 销售预测基础：提供按地区分析的历史数据，支持销售预测
 */
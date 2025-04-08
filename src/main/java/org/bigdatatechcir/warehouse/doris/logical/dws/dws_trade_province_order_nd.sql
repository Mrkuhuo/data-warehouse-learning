/*
 * 文件名: dws_trade_province_order_nd.sql
 * 功能描述: 交易域省份粒度订单最近n日汇总表加载脚本
 * 数据粒度: 省份 + 日期
 * 调度周期: 每日执行
 * 调度依赖:
 *   - dws_trade_province_order_1d表数据已准备完毕
 * 数据来源:
 *   - dws.dws_trade_province_order_1d: 交易域省份粒度订单最近1日汇总表
 * 目标表: dws.dws_trade_province_order_nd
 * 聚合周期: 最近7日、最近30日
 */

-- PART 1: 设置参数和获取省份维度信息
WITH
-- 当前日期参数化
current_date_param AS (
    SELECT DATE('${pdate}') AS cur_date
),

-- 获取省份维度信息（复用最新的维度数据）
province_dim AS (
    SELECT DISTINCT
        province_id,                                   -- 省份ID
        province_name,                                 -- 省份名称
        area_code,                                     -- 地区编码
        iso_code,                                      -- 旧版ISO-3166-2编码
        iso_3166_2                                     -- 新版ISO-3166-2编码
    FROM 
        dws.dws_trade_province_order_1d
    WHERE 
        k1 = DATE('${pdate}')
)

-- PART 2: 生成最终结果，聚合多天的1日数据
INSERT INTO dws.dws_trade_province_order_nd (
    province_id, k1, province_name, area_code, iso_code, iso_3166_2,
    -- 7日汇总字段
    order_count_7d, order_original_amount_7d, activity_reduce_amount_7d, 
    coupon_reduce_amount_7d, order_total_amount_7d,
    -- 30日汇总字段
    order_count_30d, order_original_amount_30d, activity_reduce_amount_30d, 
    coupon_reduce_amount_30d, order_total_amount_30d
)
SELECT
    -- 维度字段
    dim.province_id,                                  -- 省份ID
    cur_date AS k1,                                   -- 分区日期
    dim.province_name,                                -- 省份名称
    dim.area_code,                                    -- 地区编码
    dim.iso_code,                                     -- 旧版ISO-3166-2编码
    dim.iso_3166_2,                                   -- 新版ISO-3166-2编码
    
    -- 7日汇总指标
    SUM(IF(k1 BETWEEN DATE_SUB(cur_date, 6) AND cur_date, order_count_1d, 0)) AS order_count_7d,
    SUM(IF(k1 BETWEEN DATE_SUB(cur_date, 6) AND cur_date, order_original_amount_1d, 0)) AS order_original_amount_7d,
    SUM(IF(k1 BETWEEN DATE_SUB(cur_date, 6) AND cur_date, activity_reduce_amount_1d, 0)) AS activity_reduce_amount_7d,
    SUM(IF(k1 BETWEEN DATE_SUB(cur_date, 6) AND cur_date, coupon_reduce_amount_1d, 0)) AS coupon_reduce_amount_7d,
    SUM(IF(k1 BETWEEN DATE_SUB(cur_date, 6) AND cur_date, order_total_amount_1d, 0)) AS order_total_amount_7d,
    
    -- 30日汇总指标
    SUM(IF(k1 BETWEEN DATE_SUB(cur_date, 29) AND cur_date, order_count_1d, 0)) AS order_count_30d,
    SUM(IF(k1 BETWEEN DATE_SUB(cur_date, 29) AND cur_date, order_original_amount_1d, 0)) AS order_original_amount_30d,
    SUM(IF(k1 BETWEEN DATE_SUB(cur_date, 29) AND cur_date, activity_reduce_amount_1d, 0)) AS activity_reduce_amount_30d,
    SUM(IF(k1 BETWEEN DATE_SUB(cur_date, 29) AND cur_date, coupon_reduce_amount_1d, 0)) AS coupon_reduce_amount_30d,
    SUM(IF(k1 BETWEEN DATE_SUB(cur_date, 29) AND cur_date, order_total_amount_1d, 0)) AS order_total_amount_30d
FROM
    -- 省份维度信息
    province_dim dim
LEFT JOIN
    -- 一日汇总数据
    dws.dws_trade_province_order_1d od
    ON dim.province_id = od.province_id
JOIN
    current_date_param
WHERE
    -- 只处理最近30天的历史数据
    od.k1 BETWEEN DATE_SUB(cur_date, 29) AND cur_date
GROUP BY
    dim.province_id, dim.province_name, dim.area_code, dim.iso_code, dim.iso_3166_2, cur_date;

/*
 * 说明：
 * 1. 本脚本的数据处理流程：
 *    - 不同于其他脚本直接从DWD层获取数据，本脚本从DWS层的1日汇总表获取数据
 *    - 先获取最新的省份维度信息
 *    - 然后对1日汇总表数据按不同时间窗口（7日和30日）进行聚合
 *    - 最后按省份和当前日期生成汇总记录
 *
 * 2. 查询优化策略：
 *    - 使用IF条件聚合代替多次查询，减少数据扫描次数
 *    - 使用最近一天的数据获取维度信息，避免重复关联维度表
 *    - 使用LEFT JOIN确保即使某些天没有数据的省份也能被包含
 *    - 只处理最近30天的数据，提高查询效率
 *
 * 3. 多日聚合的业务价值：
 *    - 提供不同时间粒度的销售视图，满足短期和中期分析需求
 *    - 降低查询复杂度，避免分析时需要临时聚合
 *    - 支持稳定的趋势分析，减小单日波动带来的干扰
 *    - 为销售预测和季节性分析提供准备好的数据
 *
 * 4. 与其他表的配合使用：
 *    - 与1日汇总表配合：1日表提供精细粒度，n日表提供趋势视图
 *    - 与商品粒度表配合：可联合分析区域和商品的交叉维度
 *    - 与用户粒度表配合：可分析不同区域的用户行为差异
 *
 * 5. 数据质量注意事项：
 *    - 需确保1日汇总表数据的完整性，否则会影响n日汇总结果
 *    - 历史回溯计算时需调整日期参数并重新执行脚本
 *    - 首次执行时需确保有足够的历史数据（至少30天）
 *
 * 6. 扩展优化方向：
 *    - 可考虑添加同比、环比计算逻辑
 *    - 可以增加用户数等指标的去重计算
 *    - 考虑增加销售增长率和贡献率等派生指标
 */
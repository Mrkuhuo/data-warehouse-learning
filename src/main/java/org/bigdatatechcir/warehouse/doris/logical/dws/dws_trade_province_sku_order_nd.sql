/*
 * 文件名: dws_trade_province_sku_order_nd.sql
 * 功能描述: 交易域省份商品粒度订单最近n日汇总表
 * 数据粒度: 省份 + 商品 + 日期
 * 刷新策略: 每日全量刷新
 * 调度周期: 每日执行
 * 调度依赖:
 *   - dws层1日汇总表数据已准备完毕
 * 数据来源:
 *   - dws.dws_trade_province_sku_order_1d: 交易域省份商品粒度订单最近1日汇总表
 * 目标表: dws.dws_trade_province_sku_order_nd
 * 聚合周期: 最近1日、最近7日、最近30日
 * 应用场景:
 *   - 省份商品销售分析: 分析各省份各商品的销售趋势和变化
 *   - 区域商品偏好: 发现不同区域的商品偏好差异
 *   - 供应链决策支持: 为区域商品库存管理提供数据支持
 *   - 区域销售异常监控: 监控特定区域特定商品的销售异常
 */
-- 交易域省份商品粒度订单最近n日汇总表（每日执行）
INSERT INTO dws.dws_trade_province_sku_order_nd(
    province_id, sku_id, k1,                          -- 主键维度字段
    province_name, province_area_code, province_iso_code, -- 省份维度冗余字段
    sku_name, category1_id, category1_name, category2_id, category2_name, category3_id, category3_name, tm_id, tm_name, -- 商品维度冗余字段
    -- 1日汇总指标
    order_count_1d, order_num_1d, order_user_count_1d, -- 1日订单量、商品件数、用户数指标
    order_original_amount_1d, activity_reduce_amount_1d, coupon_reduce_amount_1d, order_total_amount_1d, -- 1日金额指标
    -- 7日汇总指标
    order_count_7d, order_num_7d, order_user_count_7d, -- 7日订单量、商品件数、用户数指标
    order_original_amount_7d, activity_reduce_amount_7d, coupon_reduce_amount_7d, order_total_amount_7d, -- 7日金额指标
    -- 30日汇总指标
    order_count_30d, order_num_30d, order_user_count_30d, -- 30日订单量、商品件数、用户数指标
    order_original_amount_30d, activity_reduce_amount_30d, coupon_reduce_amount_30d, order_total_amount_30d, -- 30日金额指标
    -- 新增趋势指标 - 环比和同比增长率
    order_count_1d_wow_rate, order_count_1d_yoy_rate, -- 订单数环比、同比增长率
    order_total_amount_1d_wow_rate, order_total_amount_1d_yoy_rate -- 销售额环比、同比增长率
)
-- 设置变量
WITH parameter AS (
    SELECT 
        DATE('${pdate}') AS cur_date,                                 -- 当前处理日期
        DATE_SUB(DATE('${pdate}'), INTERVAL 1 DAY) AS yesterday,      -- 昨天日期
        DATE_SUB(DATE('${pdate}'), INTERVAL 7 DAY) AS last_7day,      -- 7天前日期
        DATE_SUB(DATE('${pdate}'), INTERVAL 30 DAY) AS last_30day,    -- 30天前日期
        DATE_SUB(DATE('${pdate}'), INTERVAL 1 YEAR) AS last_year_date -- 去年同期日期
),
-- 计算当前日期数据
current_data AS (
    SELECT
        o.province_id,                                    -- 省份ID（主键之一）
        o.sku_id,                                         -- 商品ID（主键之一）
        p.cur_date AS k1,                                 -- 当前处理日期（主键之一）
        MAX(o.province_name) AS province_name,            -- 省份名称（取最新值）
        MAX(o.province_area_code) AS province_area_code,  -- 省份区号（取最新值）
        MAX(o.province_iso_code) AS province_iso_code,    -- 省份ISO编码（取最新值）
        MAX(o.sku_name) AS sku_name,                      -- 商品名称（取最新值）
        MAX(o.category1_id) AS category1_id,              -- 一级品类ID（取最新值）
        MAX(o.category1_name) AS category1_name,          -- 一级品类名称（取最新值）
        MAX(o.category2_id) AS category2_id,              -- 二级品类ID（取最新值）
        MAX(o.category2_name) AS category2_name,          -- 二级品类名称（取最新值）
        MAX(o.category3_id) AS category3_id,              -- 三级品类ID（取最新值）
        MAX(o.category3_name) AS category3_name,          -- 三级品类名称（取最新值）
        MAX(o.tm_id) AS tm_id,                            -- 品牌ID（取最新值）
        MAX(o.tm_name) AS tm_name,                        -- 品牌名称（取最新值）
        
        -- 1日汇总指标（使用当日数据）
        SUM(IF(o.k1 = p.cur_date, o.order_count_1d, 0)) AS order_count_1d,           -- 1日订单数
        SUM(IF(o.k1 = p.cur_date, o.order_num_1d, 0)) AS order_num_1d,               -- 1日商品件数
        SUM(IF(o.k1 = p.cur_date, o.order_user_count_1d, 0)) AS order_user_count_1d, -- 1日用户数
        SUM(IF(o.k1 = p.cur_date, o.order_original_amount_1d, 0)) AS order_original_amount_1d,       -- 1日原始总金额
        SUM(IF(o.k1 = p.cur_date, o.activity_reduce_amount_1d, 0)) AS activity_reduce_amount_1d,     -- 1日活动优惠金额
        SUM(IF(o.k1 = p.cur_date, o.coupon_reduce_amount_1d, 0)) AS coupon_reduce_amount_1d,         -- 1日优惠券优惠金额
        SUM(IF(o.k1 = p.cur_date, o.order_total_amount_1d, 0)) AS order_total_amount_1d,             -- 1日最终金额
        
        -- 7日汇总指标（过滤最近7天数据）
        SUM(IF(o.k1 BETWEEN p.last_7day AND p.cur_date, o.order_count_1d, 0)) AS order_count_7d,           -- 7日订单数
        SUM(IF(o.k1 BETWEEN p.last_7day AND p.cur_date, o.order_num_1d, 0)) AS order_num_7d,               -- 7日商品件数
        -- 注：用户数7日聚合为近似值
        SUM(IF(o.k1 BETWEEN p.last_7day AND p.cur_date, o.order_user_count_1d, 0)) AS order_user_count_7d, -- 7日用户数（近似值）
        SUM(IF(o.k1 BETWEEN p.last_7day AND p.cur_date, o.order_original_amount_1d, 0)) AS order_original_amount_7d,       -- 7日原始总金额
        SUM(IF(o.k1 BETWEEN p.last_7day AND p.cur_date, o.activity_reduce_amount_1d, 0)) AS activity_reduce_amount_7d,     -- 7日活动优惠金额
        SUM(IF(o.k1 BETWEEN p.last_7day AND p.cur_date, o.coupon_reduce_amount_1d, 0)) AS coupon_reduce_amount_7d,         -- 7日优惠券优惠金额
        SUM(IF(o.k1 BETWEEN p.last_7day AND p.cur_date, o.order_total_amount_1d, 0)) AS order_total_amount_7d,             -- 7日最终金额
        
        -- 30日汇总指标（过滤最近30天数据）
        SUM(IF(o.k1 BETWEEN p.last_30day AND p.cur_date, o.order_count_1d, 0)) AS order_count_30d,           -- 30日订单数
        SUM(IF(o.k1 BETWEEN p.last_30day AND p.cur_date, o.order_num_1d, 0)) AS order_num_30d,               -- 30日商品件数
        -- 注：用户数30日聚合为近似值
        SUM(IF(o.k1 BETWEEN p.last_30day AND p.cur_date, o.order_user_count_1d, 0)) AS order_user_count_30d, -- 30日用户数（近似值）
        SUM(IF(o.k1 BETWEEN p.last_30day AND p.cur_date, o.order_original_amount_1d, 0)) AS order_original_amount_30d,       -- 30日原始总金额
        SUM(IF(o.k1 BETWEEN p.last_30day AND p.cur_date, o.activity_reduce_amount_1d, 0)) AS activity_reduce_amount_30d,     -- 30日活动优惠金额
        SUM(IF(o.k1 BETWEEN p.last_30day AND p.cur_date, o.coupon_reduce_amount_1d, 0)) AS coupon_reduce_amount_30d,         -- 30日优惠券优惠金额
        SUM(IF(o.k1 BETWEEN p.last_30day AND p.cur_date, o.order_total_amount_1d, 0)) AS order_total_amount_30d              -- 30日最终金额
    FROM 
        dws.dws_trade_province_sku_order_1d o,
        parameter p
    WHERE 
        -- 只处理最近30天的数据，提高性能
        o.k1 BETWEEN p.last_30day AND p.cur_date
    GROUP BY 
        o.province_id, o.sku_id, p.cur_date
),
-- 计算上周同期数据，用于环比计算
last_week_data AS (
    SELECT
        o.province_id,
        o.sku_id,
        SUM(IF(o.k1 = DATE_SUB(p.yesterday, INTERVAL 6 DAY), o.order_count_1d, 0)) AS last_week_order_count_1d,
        SUM(IF(o.k1 = DATE_SUB(p.yesterday, INTERVAL 6 DAY), o.order_total_amount_1d, 0)) AS last_week_order_total_amount_1d
    FROM 
        dws.dws_trade_province_sku_order_1d o,
        parameter p
    WHERE 
        o.k1 = DATE_SUB(p.yesterday, INTERVAL 6 DAY)
    GROUP BY 
        o.province_id, o.sku_id
),
-- 计算去年同期数据，用于同比计算
last_year_data AS (
    SELECT
        o.province_id,
        o.sku_id,
        SUM(IF(o.k1 = p.last_year_date, o.order_count_1d, 0)) AS last_year_order_count_1d,
        SUM(IF(o.k1 = p.last_year_date, o.order_total_amount_1d, 0)) AS last_year_order_total_amount_1d
    FROM 
        dws.dws_trade_province_sku_order_1d o,
        parameter p
    WHERE 
        o.k1 = p.last_year_date
    GROUP BY 
        o.province_id, o.sku_id
)
-- 整合当前数据、环比数据和同比数据
SELECT
    cd.province_id,
    cd.sku_id,
    cd.k1,
    cd.province_name,
    cd.province_area_code,
    cd.province_iso_code,
    cd.sku_name,
    cd.category1_id,
    cd.category1_name,
    cd.category2_id,
    cd.category2_name,
    cd.category3_id,
    cd.category3_name,
    cd.tm_id,
    cd.tm_name,
    -- 1日汇总指标
    cd.order_count_1d,
    cd.order_num_1d,
    cd.order_user_count_1d,
    cd.order_original_amount_1d,
    cd.activity_reduce_amount_1d,
    cd.coupon_reduce_amount_1d,
    cd.order_total_amount_1d,
    -- 7日汇总指标
    cd.order_count_7d,
    cd.order_num_7d,
    cd.order_user_count_7d,
    cd.order_original_amount_7d,
    cd.activity_reduce_amount_7d,
    cd.coupon_reduce_amount_7d,
    cd.order_total_amount_7d,
    -- 30日汇总指标
    cd.order_count_30d,
    cd.order_num_30d,
    cd.order_user_count_30d,
    cd.order_original_amount_30d,
    cd.activity_reduce_amount_30d,
    cd.coupon_reduce_amount_30d,
    cd.order_total_amount_30d,
    -- 1日订单数环比增长率 (当前值 - 上期值) / 上期值
    CASE 
        WHEN COALESCE(lw.last_week_order_count_1d, 0) = 0 THEN NULL
        ELSE ROUND((cd.order_count_1d - COALESCE(lw.last_week_order_count_1d, 0)) / COALESCE(lw.last_week_order_count_1d, 1) * 100, 2)
    END AS order_count_1d_wow_rate,
    -- 1日订单数同比增长率
    CASE 
        WHEN COALESCE(ly.last_year_order_count_1d, 0) = 0 THEN NULL
        ELSE ROUND((cd.order_count_1d - COALESCE(ly.last_year_order_count_1d, 0)) / COALESCE(ly.last_year_order_count_1d, 1) * 100, 2)
    END AS order_count_1d_yoy_rate,
    -- 1日销售额环比增长率
    CASE 
        WHEN COALESCE(lw.last_week_order_total_amount_1d, 0) = 0 THEN NULL
        ELSE ROUND((cd.order_total_amount_1d - COALESCE(lw.last_week_order_total_amount_1d, 0)) / COALESCE(lw.last_week_order_total_amount_1d, 1) * 100, 2)
    END AS order_total_amount_1d_wow_rate,
    -- 1日销售额同比增长率
    CASE 
        WHEN COALESCE(ly.last_year_order_total_amount_1d, 0) = 0 THEN NULL
        ELSE ROUND((cd.order_total_amount_1d - COALESCE(ly.last_year_order_total_amount_1d, 0)) / COALESCE(ly.last_year_order_total_amount_1d, 1) * 100, 2)
    END AS order_total_amount_1d_yoy_rate
FROM 
    current_data cd
LEFT JOIN 
    last_week_data lw ON cd.province_id = lw.province_id AND cd.sku_id = lw.sku_id
LEFT JOIN 
    last_year_data ly ON cd.province_id = ly.province_id AND cd.sku_id = ly.sku_id;

/*
 * 脚本设计说明:
 * 1. 数据处理逻辑:
 *    - 使用CTE结构优化代码可读性，分别计算当前数据、环比数据和同比数据
 *    - 基于1日汇总表(dws_trade_province_sku_order_1d)计算各周期聚合指标
 *    - 采用参数化处理日期(${pdate})，支持历史数据重新计算
 *    - 使用条件聚合(IF语句)避免多次扫描数据，提高性能
 *    - 只处理最近30天数据减少处理数据量，提高执行效率
 *
 * 2. 指标计算方法:
 *    - 订单量/商品件数/金额: 直接对原子指标按时间区间累加
 *    - 用户数计算问题: 原子指标的简单累加会导致重复计算用户，实际用户数会偏高
 *      > 最佳解决方案: 使用APPROX_COUNT_DISTINCT或位图函数处理用户去重
 *      > 本脚本中使用SUM函数的近似计算，需注意分析时考虑此误差
 *    - 增长率计算: 通过(当前值-对比值)/对比值计算，避免除零错误
 *      > 环比增长率: 与上周同一天对比
 *      > 同比增长率: 与去年同一天对比
 *
 * 3. 表设计优化:
 *    - 维度冗余设计: 冗余存储省份和商品维度，避免查询时关联开销
 *    - 多时间窗口聚合: 一张表同时包含1日、7日、30日数据，满足不同周期分析需求
 *    - 分区设计: 以日期(k1)作为分区键，支持历史数据清理和快速查询
 *    - 复合主键: 使用[province_id, sku_id, k1]作为复合主键，支持多维查询优化
 *    - 趋势指标设计: 增加环比和同比增长率指标，直接反映销售趋势变化
 *
 * 4. 应用场景示例:
 *    - 热销商品区域分布: 识别特定商品在不同省份的销售表现，发现区域热点
 *    - 区域商品结构分析: 分析不同省份的商品类目偏好，指导区域化营销策略
 *    - 区域促销效果评估: 通过活动和优惠券金额占比，评估促销活动在不同地区的效果
 *    - 趋势异常监控: 通过环比和同比增长率，快速识别异常波动的区域或商品
 *    - 季节性分析: 借助同比指标，分析不同地区不同商品的季节性特征
 *
 * 5. 注意事项和改进方向:
 *    - 用户数精确计算: 考虑使用位图或HyperLogLog等技术优化用户去重计算
 *    - 异常值处理: 增长率计算中已加入对除零情况的处理，但仍需关注极端值
 *    - 历史数据处理: 当前每次处理只保留30天数据，根据业务需求可调整处理范围
 *    - 数据一致性: 依赖上游1日汇总表，需确保1日表数据质量和完整性
 *    - 性能优化: 通过分区裁剪和索引优化进一步提升查询性能
 */ 
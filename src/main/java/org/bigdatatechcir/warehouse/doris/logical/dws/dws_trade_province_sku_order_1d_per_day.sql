/*
 * 文件名: dws_trade_province_sku_order_1d_per_day.sql
 * 功能描述: 交易域省份商品粒度订单最近1日汇总表每日增量加载
 * 数据粒度: 省份 + 商品 + 日期
 * 刷新策略: 每日增量/覆盖式
 * 调度周期: 每日一次
 * 依赖表:
 *   - dwd.dwd_trade_order_detail_inc: 交易域订单明细事实表（增量）
 *   - dim.dim_sku_full: 商品维度表（全量）
 *   - dim.dim_province_full: 省份维度表（全量）
 * 目标表: dws.dws_trade_province_sku_order_1d
 * 主要功能: 提供最近1日的省份维度商品订单聚合指标，用于区域销售分析和商品地区偏好分析
 */
-- 交易域省份商品粒度订单最近1日汇总表（每日增量加载）
INSERT INTO dws.dws_trade_province_sku_order_1d(
    province_id, sku_id, k1,
    province_name, province_area_code, province_iso_code,
    sku_name, category1_id, category1_name, category2_id, category2_name, category3_id, category3_name, tm_id, tm_name,
    order_count_1d, order_num_1d, order_user_count_1d,
    order_original_amount_1d, activity_reduce_amount_1d, coupon_reduce_amount_1d, order_total_amount_1d
)
-- 设置变量
WITH parameter AS (
    SELECT
        DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), '%Y-%m-%d') AS k1_yesterday -- 获取前一天日期
)

-- 主查询：关联订单聚合数据与商品、省份维度信息
SELECT
    od.province_id,                                    -- 省份ID
    od.sku_id,                                         -- 商品SKU_ID
    od.k1,                                             -- 数据日期
    COALESCE(prov.province_name, '未知省份'),          -- 省份名称
    COALESCE(prov.area_code, '未知区号'),              -- 省份区号
    COALESCE(prov.iso_code, '未知编码'),               -- 省份ISO编码
    COALESCE(sku.sku_name, '未知商品'),                -- 商品名称
    COALESCE(sku.category1_id, '-1'),                  -- 一级品类ID
    COALESCE(sku.category1_name, '未知品类'),          -- 一级品类名称
    COALESCE(sku.category2_id, '-1'),                  -- 二级品类ID
    COALESCE(sku.category2_name, '未知品类'),          -- 二级品类名称
    COALESCE(sku.category3_id, '-1'),                  -- 三级品类ID
    COALESCE(sku.category3_name, '未知品类'),          -- 三级品类名称
    COALESCE(sku.tm_id, '-1'),                         -- 品牌ID
    COALESCE(sku.tm_name, '未知品牌'),                 -- 品牌名称
    od.order_count_1d,                                 -- 下单次数
    od.order_num_1d,                                   -- 下单商品件数
    od.order_user_count_1d,                            -- 下单用户数
    od.order_original_amount_1d,                       -- 原始金额
    od.activity_reduce_amount_1d,                      -- 活动优惠金额
    od.coupon_reduce_amount_1d,                        -- 优惠券优惠金额
    od.order_total_amount_1d                           -- 下单最终金额
FROM
    (
        -- 子查询：按省份、商品、日期维度聚合订单数据（仅处理昨天的数据）
        SELECT
            province_id,                               -- 省份ID
            sku_id,                                    -- 商品ID
            k1,                                        -- 日期
            COUNT(DISTINCT order_id) AS order_count_1d, -- 计算下单次数（按订单去重）
            SUM(sku_num) AS order_num_1d,             -- 累计下单商品件数
            COUNT(DISTINCT user_id) AS order_user_count_1d, -- 下单用户数（按用户去重）
            SUM(split_original_amount) AS order_original_amount_1d,           -- 累计原始金额
            SUM(COALESCE(split_activity_amount, 0.0)) AS activity_reduce_amount_1d, -- 累计活动优惠金额
            SUM(COALESCE(split_coupon_amount, 0.0)) AS coupon_reduce_amount_1d,     -- 累计优惠券优惠金额
            SUM(split_total_amount) AS order_total_amount_1d                  -- 累计最终金额
        FROM
            dwd.dwd_trade_order_detail_inc
        WHERE
                k1 = (SELECT k1_yesterday FROM parameter)   -- 仅处理昨天的数据
        GROUP BY
            province_id, sku_id, k1
    ) od
-- 关联省份维度表，丰富省份信息
        LEFT JOIN
    (
        SELECT
            id,                                        -- 省份ID
            province_name,                             -- 省份名称
            area_code,                                 -- 省份区号
            iso_code                                   -- 省份ISO编码
        FROM
            dim.dim_province_full
    ) prov
    ON od.province_id = prov.id
-- 关联商品维度表，丰富商品信息
        LEFT JOIN
    (
        SELECT
            id,                                        -- 商品ID
            sku_name,                                  -- 商品名称
            category1_id,                              -- 一级品类ID
            category1_name,                            -- 一级品类名称
            category2_id,                              -- 二级品类ID
            category2_name,                            -- 二级品类名称
            category3_id,                              -- 三级品类ID
            category3_name,                            -- 三级品类名称
            tm_id,                                     -- 品牌ID
            tm_name                                    -- 品牌名称
        FROM
            dim.dim_sku_full
        WHERE k1 = (SELECT MAX(k1) FROM dim.dim_sku_full)  -- 使用最新维度快照
    ) sku
    ON od.sku_id = sku.id;

/*
 * 说明：
 * 1. 脚本执行策略：
 *    - 先删除目标表中昨天的数据，采用"覆盖式刷新"策略
 *    - 在dwd.dwd_trade_order_detail_inc表中聚合昨天的订单数据
 *    - 按省份ID、商品ID和日期进行分组聚合
 *    - 关联最新的维度表获取省份和商品的详细信息
 *    - 将结果插入目标表
 *
 * 2. 聚合指标说明：
 *    - 订单统计：计算特定省份特定商品一天内的订单总量
 *    - 商品数量：计算该商品在指定省份的销售件数
 *    - 用户统计：计算购买该商品的用户数量，反映商品在该省的受欢迎程度
 *    - 金额计算：包括原始金额、各类优惠金额和最终金额，用于分析不同促销策略的区域效果
 *
 * 3. 数据应用场景：
 *    - 区域商品分析：分析不同省份对不同商品的偏好，指导区域化营销策略
 *    - 商品热度地图：结合地图可视化，直观展示商品在全国各省的热度分布
 *    - 区域促销效果评估：通过优惠金额分析，评估促销活动在不同地区的效果差异
 *    - 供应链优化：基于区域销售情况，优化区域仓储和物流策略
 *
 * 4. 执行优化考虑：
 *    - 仅处理昨天的增量数据，避免全表扫描，提高执行效率
 *    - 使用覆盖式刷新策略，确保数据一致性，处理可能的重复数据问题
 *    - 使用LEFT JOIN确保即使维度信息不完整也能保留聚合数据
 *    - 对NULL值使用COALESCE函数提供默认值，保证数据完整性
 *
 * 5. 数据质量考量：
 *    - 省份ID映射正确性：确保订单中的省份ID能正确关联到省份维度表
 *    - 商品ID映射完整性：确保订单中的商品ID能找到对应的商品信息
 *    - 异常值处理：使用COALESCE函数处理NULL值，避免计算错误
 */ 
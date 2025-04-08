/*
 * 文件名: dws_trade_province_sku_order_1d_first.sql
 * 功能描述: 交易域省份商品粒度订单最近1日汇总表首次历史数据加载
 * 数据粒度: 省份 + 商品 + 日期
 * 调度周期: 一次性执行（历史数据初始化）
 * 数据来源:
 *   - dwd.dwd_trade_order_detail_inc: 交易域订单明细事实表
 *   - dim.dim_sku_full: 商品维度表
 *   - dim.dim_province_full: 省份维度表
 * 目标表: dws.dws_trade_province_sku_order_1d
 */

-- 交易域省份商品粒度订单最近1日汇总表（首次历史数据加载）
INSERT INTO dws.dws_trade_province_sku_order_1d(
    province_id, sku_id, k1, 
    province_name, province_area_code, province_iso_code,
    sku_name, category1_id, category1_name, category2_id, category2_name, category3_id, category3_name, tm_id, tm_name,
    order_count_1d, order_num_1d, order_user_count_1d,
    order_original_amount_1d, activity_reduce_amount_1d, coupon_reduce_amount_1d, order_total_amount_1d
)
-- 主查询：关联订单聚合数据与商品、省份维度信息
SELECT
    od.province_id,                                    -- 省份ID
    od.sku_id,                                         -- 商品SKU_ID
    od.k1,                                             -- 数据日期
    COALESCE(prov.province_name, '未知省份'),                   -- 省份名称
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
        -- 子查询：按省份、商品、日期维度聚合订单数据
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
        -- 首次加载，使用最新维度快照
        WHERE k1 = (SELECT MAX(k1) FROM dim.dim_sku_full)
    ) sku
ON od.sku_id = sku.id;

/*
 * 说明：
 * 1. 脚本执行流程：
 *    - 在dwd.dwd_trade_order_detail_inc表中聚合历史数据
 *    - 按省份ID、商品ID和日期进行分组聚合
 *    - 关联维度表获取省份和商品的详细信息
 *    - 将结果插入目标表
 *
 * 2. 聚合指标说明：
 *    - 订单统计：通过COUNT(DISTINCT order_id)计算每个省份每个商品每天的订单数
 *    - 商品数量：通过SUM(sku_num)计算商品总件数
 *    - 用户数统计：通过COUNT(DISTINCT user_id)计算去重后的下单用户数
 *    - 金额计算：包括原始金额、优惠金额和最终金额的汇总
 *
 * 3. 数据处理考量：
 *    - 使用LEFT JOIN处理维度表关联，确保即使维度信息缺失也能保留订单数据
 *    - 使用COALESCE函数为缺失的维度信息提供默认值，提高数据完整性
 *    - 对可能为NULL的优惠金额字段使用COALESCE确保计算准确性
 *
 * 4. 首次加载特点：
 *    - 不限制日期范围，加载历史所有数据
 *    - 使用维度表的最新快照，确保维度信息一致性
 *    - 单次处理所有历史数据，执行时间可能较长
 *
 * 5. 执行建议：
 *    - 考虑分批次加载，如按月加载历史数据
 *    - 对于大量历史数据，可增加并行度参数
 *    - 监控执行进度和资源使用情况，必要时调整执行计划
 */ 
/*
 * 脚本名称: dws_trade_user_sku_order_refund_nd.sql
 * 目标表: dws.dws_trade_user_sku_order_refund_nd
 * 数据粒度: 用户 + SKU + 日期
 * 刷新策略: 增量刷新，每日新增数据
 * 调度周期: 每日调度一次
 * 运行参数:
 *   - pdate: 数据日期，默认为当天
 * 依赖表:
 *   - dws.dws_trade_user_sku_order_refund_1d: 交易域用户商品粒度退单最近1日汇总表
 */

-- 交易域用户商品粒度退单最近n日汇总表
-- 计算逻辑: 
-- 1. 从1日汇总表获取最近30天的数据
-- 2. 按用户、商品和日期分组聚合退单数据
-- 3. 计算7日和30日的累计指标
INSERT INTO dws.dws_trade_user_sku_order_refund_nd(
    /* 维度字段 */
    user_id, sku_id, k1,               /* 主键维度 */
    
    /* 冗余维度 */
    sku_name,                          /* 商品名称 */
    category1_id, category1_name,      /* 一级品类信息 */
    category2_id, category2_name,      /* 二级品类信息 */
    category3_id, category3_name,      /* 三级品类信息 */
    tm_id, tm_name,                    /* 品牌信息 */
    
    /* 度量值字段 - 7日累计 */
    order_refund_count_7d,             /* 7日退单次数 */
    order_refund_num_7d,               /* 7日退单件数 */
    order_refund_amount_7d,            /* 7日退单金额 */
    
    /* 度量值字段 - 30日累计 */
    order_refund_count_30d,            /* 30日退单次数 */
    order_refund_num_30d,              /* 30日退单件数 */
    order_refund_amount_30d            /* 30日退单金额 */
)
SELECT
    /* 维度字段: 保持与1日汇总表一致 */
    user_id,                           /* 用户ID: 退单用户标识 */
    sku_id,                            /* 商品SKU_ID: 被退货商品标识 */
    k1,                                /* 数据日期: 退单日期 */
    
    /* 冗余维度: 保持与1日汇总表一致 */
    sku_name,                          /* 商品名称: 便于识别具体商品 */
    category1_id,                      /* 一级品类ID: 商品所属大类 */
    category1_name,                    /* 一级品类名称: 便于分析大类退货情况 */
    category2_id,                      /* 二级品类ID: 商品所属中类 */
    category2_name,                    /* 二级品类名称: 便于分析中类退货情况 */
    category3_id,                      /* 三级品类ID: 商品所属小类 */
    category3_name,                    /* 三级品类名称: 便于分析小类退货情况 */
    tm_id,                             /* 品牌ID: 商品所属品牌 */
    tm_name,                           /* 品牌名称: 便于品牌退货分析 */
    
    /* 7日累计指标: 只统计最近7天的数据 */
    SUM(IF(k1 >= DATE_ADD(DATE('${pdate}'), -6), order_refund_count_1d, 0)) AS order_refund_count_7d,    /* 7日退单次数: 最近7天的退单次数总和 */
    SUM(IF(k1 >= DATE_ADD(DATE('${pdate}'), -6), order_refund_num_1d, 0)) AS order_refund_num_7d,        /* 7日退单件数: 最近7天的退货数量总和 */
    SUM(IF(k1 >= DATE_ADD(DATE('${pdate}'), -6), order_refund_amount_1d, 0)) AS order_refund_amount_7d,  /* 7日退单金额: 最近7天的退款金额总和 */
    
    /* 30日累计指标: 统计最近30天的数据 */
    SUM(order_refund_count_1d) AS order_refund_count_30d,                /* 30日退单次数: 最近30天的退单次数总和 */
    SUM(order_refund_num_1d) AS order_refund_num_30d,                    /* 30日退单件数: 最近30天的退货数量总和 */
    SUM(order_refund_amount_1d) AS order_refund_amount_30d               /* 30日退单金额: 最近30天的退款金额总和 */
FROM 
    dws.dws_trade_user_sku_order_refund_1d   /* 从1日汇总表获取数据 */
WHERE 
    k1 >= DATE_ADD(DATE('${pdate}'), -29)    /* 时间筛选: 只处理最近30天的数据 */
    AND k1 <= DATE('${pdate}')               /* 时间筛选: 确保不超过处理日期 */
GROUP BY 
    user_id, sku_id, k1,               /* 按用户、商品和日期分组 */
    sku_name,                          /* 保持冗余维度一致 */
    category1_id, category1_name,      /* 一级品类信息 */
    category2_id, category2_name,      /* 二级品类信息 */
    category3_id, category3_name,      /* 三级品类信息 */
    tm_id, tm_name;                    /* 品牌信息 */

/*
 * 数据处理说明:
 *
 * 1. 累计指标计算:
 *    - 7日累计: 使用IF条件筛选最近7天的数据并求和
 *    - 30日累计: 直接对30天内的所有数据求和
 *    - 时间窗口: 使用DATE_ADD函数计算日期范围
 *
 * 2. 数据来源与处理:
 *    - 数据来源: 从1日汇总表获取最近30天的数据
 *    - 时间筛选: 通过WHERE条件限定只处理最近30天的数据
 *    - 数据聚合: 按用户、商品和日期分组聚合退单指标
 *    - 维度保持: 保持与1日汇总表相同的维度字段
 *
 * 3. 性能优化:
 *    - 时间筛选: 通过WHERE条件限定只处理最近30天的数据，减少数据处理量
 *    - 分组优化: 按用户、商品和日期分组，保证统计粒度一致
 *    - 条件计算: 使用IF条件在聚合时筛选7日数据，避免多次扫描
 *
 * 4. 应用建议:
 *    - 每日调度: 应设置为每日运行，确保数据及时更新
 *    - 依赖管理: 确保上游1日汇总表数据已准备就绪
 *    - 参数设置: 正确设置pdate参数，支持历史数据补录
 *    - 结果验证: 建议通过对比总数和抽样检查验证数据准确性
 *
 * 5. 业务价值:
 *    - 退货趋势分析: 支持分析商品在不同时间窗口的退货趋势
 *    - 品类退货分析: 支持分析品类在不同时间窗口的退货情况
 *    - 品牌退货分析: 支持分析品牌在不同时间窗口的退货情况
 *    - 用户行为分析: 支持分析用户在不同时间窗口的退货行为
 *    - 商品质量评估: 通过退货数据评估商品质量和用户满意度
 */
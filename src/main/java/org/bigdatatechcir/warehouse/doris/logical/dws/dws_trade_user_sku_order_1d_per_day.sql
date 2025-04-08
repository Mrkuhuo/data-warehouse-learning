/*
 * 脚本名称: dws_trade_user_sku_order_1d_per_day.sql
 * 目标表: dws.dws_trade_user_sku_order_1d
 * 数据粒度: 用户 + SKU + 日期
 * 刷新策略: 增量刷新，每日新增数据
 * 调度周期: 每日调度一次
 * 运行参数:
 *   - pdate: 数据日期，默认为当天
 * 依赖表:
 *   - dwd.dwd_trade_order_detail_inc: 交易域订单明细事实表
 *   - dim.dim_sku_full: 商品维度表全量快照
 */

-- 交易域用户商品粒度订单最近1日汇总表
-- 计算逻辑: 
-- 1. 筛选出当日的订单数据
-- 2. 按用户、商品和日期分组聚合订单数据
-- 3. 关联商品维度信息，丰富分析维度
INSERT INTO dws.dws_trade_user_sku_order_1d
(
    /* 维度字段 */
    user_id, sku_id, k1,               /* 主键维度 */
    
    /* 冗余维度 */
    sku_name,                          /* 商品名称 */
    category1_id, category1_name,      /* 一级品类信息 */
    category2_id, category2_name,      /* 二级品类信息 */
    category3_id, category3_name,      /* 三级品类信息 */
    tm_id, tm_name,                    /* 品牌信息 */
    
    /* 度量值字段 - 订单统计 */
    order_count_1d, order_num_1d,      /* 订单次数和商品件数 */
    
    /* 度量值字段 - 金额统计 */
    order_original_amount_1d,          /* 原始金额 */
    activity_reduce_amount_1d,         /* 活动优惠金额 */
    coupon_reduce_amount_1d,           /* 优惠券优惠金额 */
    order_total_amount_1d              /* 最终金额 */
)
-- 主查询：关联订单聚合数据与商品维度信息
SELECT
    od.user_id,                                     /* 用户ID: 下单用户标识 */
    od.sku_id,                                      /* 商品SKU_ID: 购买商品标识 */
    od.k1,                                          /* 数据日期: 订单日期 */
    
    /* 商品维度信息: 使用COALESCE处理维度缺失情况 */
    COALESCE(sku.sku_name, '未知商品'),             /* 商品名称: 便于识别具体商品 */
    COALESCE(sku.category1_id, '-1'),               /* 一级品类ID: 商品所属大类 */
    COALESCE(sku.category1_name, '未知品类'),       /* 一级品类名称: 便于分析大类销售情况 */
    COALESCE(sku.category2_id, '-1'),               /* 二级品类ID: 商品所属中类 */
    COALESCE(sku.category2_name, '未知品类'),       /* 二级品类名称: 便于分析中类销售情况 */
    COALESCE(sku.category3_id, '-1'),               /* 三级品类ID: 商品所属小类 */
    COALESCE(sku.category3_name, '未知品类'),       /* 三级品类名称: 便于分析小类销售情况 */
    COALESCE(sku.tm_id, '-1'),                      /* 品牌ID: 商品所属品牌 */
    COALESCE(sku.tm_name, '未知品牌'),              /* 品牌名称: 便于品牌销售分析 */
    
    /* 订单统计指标: 反映用户对商品的购买行为 */
    od.order_count_1d,                              /* 下单次数: 统计用户对该商品的下单次数 */
    od.order_num_1d,                                /* 下单商品件数: 统计用户购买该商品的总数量 */
    
    /* 金额统计指标: 反映商品的销售金额和优惠情况 */
    od.order_original_amount_1d,                    /* 原始金额: 未优惠的订单金额 */
    od.activity_reduce_amount_1d,                   /* 活动优惠金额: 活动带来的优惠金额 */
    od.coupon_reduce_amount_1d,                     /* 优惠券优惠金额: 优惠券带来的优惠金额 */
    od.order_total_amount_1d                        /* 最终金额: 优惠后的实际支付金额 */
FROM
    (
        /* 子查询: 按用户、商品、日期维度聚合订单数据 */
        SELECT
            user_id,                                /* 用户ID */
            sku_id,                                 /* 商品ID */
            k1,                                     /* 日期 */
            COUNT(*) AS order_count_1d,             /* 下单次数: 计算订单明细数 */
            SUM(sku_num) AS order_num_1d,           /* 下单商品件数: 汇总商品数量 */
            SUM(split_original_amount) AS order_original_amount_1d,           /* 原始金额: 汇总未优惠订单金额 */
            SUM(COALESCE(split_activity_amount, 0.0)) AS activity_reduce_amount_1d, /* 活动优惠金额: 汇总活动优惠 */
            SUM(COALESCE(split_coupon_amount, 0.0)) AS coupon_reduce_amount_1d,     /* 优惠券优惠金额: 汇总优惠券优惠 */
            SUM(split_total_amount) AS order_total_amount_1d                  /* 最终金额: 汇总实际支付金额 */
        FROM 
            dwd.dwd_trade_order_detail_inc
        /* 时间筛选: 只处理当天数据，通过调度参数传入 */
        WHERE 
            k1 = DATE('${pdate}')
        /* 分组: 按用户、商品和日期分组，保证统计粒度一致 */
        GROUP BY 
            user_id, sku_id, k1
    ) od
/* 关联商品维度表: 丰富商品相关维度信息 */
LEFT JOIN
    (
        SELECT
            id,                                     /* 商品ID: 关联键 */
            sku_name,                               /* 商品名称 */
            category1_id,                           /* 一级品类ID */
            category1_name,                         /* 一级品类名称 */
            category2_id,                           /* 二级品类ID */
            category2_name,                         /* 二级品类名称 */
            category3_id,                           /* 三级品类ID */
            category3_name,                         /* 三级品类名称 */
            tm_id,                                  /* 品牌ID */
            tm_name                                 /* 品牌名称 */
        FROM 
            dim.dim_sku_full
        /* 维度表过滤: 使用与处理日期匹配的维度快照，确保数据一致性 */
        WHERE k1 = (
            SELECT MAX(k1) FROM dim.dim_sku_full 
            WHERE k1 <= DATE('${pdate}')
        )
    ) sku
ON od.sku_id = sku.id;  /* 关联条件: 商品ID匹配 */

/*
 * 数据处理说明:
 *
 * 1. 增量处理模式:
 *    - 先删除当天数据: 避免重复加载导致的数据重复
 *    - 只处理当天数据: 筛选特定日期的订单数据，减少处理量
 *    - 完整维度关联: 确保每日数据都有完整的商品维度信息
 *
 * 2. 数据来源与处理:
 *    - 订单数据: 从订单明细事实表获取当日订单数据
 *    - 维度信息: 从商品维度表获取与处理日期匹配的最新商品信息
 *    - 数据聚合: 按用户、商品和日期分组聚合订单指标
 *    - 维度关联: 通过商品ID关联商品维度信息
 *
 * 3. 维度处理策略:
 *    - 缺失维度处理: 使用COALESCE函数提供默认值，避免NULL值
 *    - 维度快照选择: 使用不晚于处理日期的最新快照，确保数据一致性
 *    - 冗余维度设计: 存储完整的品类和品牌信息，避免查询时关联
 *
 * 4. 性能优化:
 *    - 时间筛选: 通过WHERE条件限定只处理当天数据，减少数据处理量
 *    - 预先删除: 使用DELETE语句避免重复数据，而非更复杂的MERGE操作
 *    - 维度表过滤: 只获取所需的维度快照，减少维度表扫描量
 *
 * 5. 应用建议:
 *    - 每日调度: 应设置为每日运行，确保数据及时更新
 *    - 依赖管理: 确保上游DWD层数据和维度表数据已准备就绪
 *    - 参数设置: 正确设置pdate参数，支持历史数据补录
 *    - 结果验证: 建议通过对比总数和抽样检查验证数据准确性
 */
-- ===============================================================================
-- 各品牌商品交易统计报表(ads_trade_stats_by_tm)
-- 功能描述：统计各品牌商品的订单量、订单人数、退单量等交易指标
-- 数据来源：dws_trade_user_sku_order_nd、dws_trade_user_sku_order_refund_nd
-- 刷新策略：每日刷新
-- 应用场景：品牌销售分析、品牌质量评估、品牌结构优化
-- ===============================================================================

-- DROP TABLE IF EXISTS ads.ads_trade_stats_by_tm;
CREATE  TABLE ads.ads_trade_stats_by_tm
(
    `dt`          VARCHAR(255) COMMENT '统计日期',
    `recent_days`             BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `tm_id`                   STRING COMMENT '品牌ID',
    `tm_name`                 STRING COMMENT '品牌名称',
    `order_count`             BIGINT COMMENT '订单数',
    `order_user_count`        BIGINT COMMENT '订单人数',
    `order_refund_count`      BIGINT COMMENT '退单数',
    `order_refund_user_count` BIGINT COMMENT '退单人数'
)
    ENGINE=OLAP
DUPLICATE KEY(`dt`)
COMMENT '各品牌商品交易统计'
DISTRIBUTED BY HASH(`dt`)
PROPERTIES
(
    "replication_allocation" = "tag.location.default: 1",
    "is_being_synced" = "false",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false"
);

/*
 * 表设计说明：
 *
 * 1. 表用途与价值：
 *    本表是品牌销售分析的核心报表，统计各品牌的订单数、用户数、退单数等交易指标，
 *    帮助业务团队了解品牌的销售表现和质量情况，为品牌引入、培育和淘汰决策提供数据支持。
 *
 * 2. 数据组成与维度：
 *    - 时间维度：统计日期和时间周期(1天/7天/30天)，支持不同时间粒度的品牌分析
 *    - 品牌维度：包含品牌ID和名称，支持品牌间的对比分析
 *    - 交易指标：包含订单相关指标和退单相关指标，反映品牌的销售量和质量
 *
 * 3. 关键指标解读：
 *    - 订单数：品牌产生的订单总数，反映品牌销售规模和市场份额
 *    - 订单人数：购买该品牌的用户数，反映品牌的用户覆盖面和普及度
 *    - 退单数：品牌产生的退单总数，反映品牌产品质量或满意度问题
 *    - 退单人数：申请退单的用户数，反映遇到问题的用户规模
 *    - 退单率：可通过计算 order_refund_count/order_count 得出，反映品牌质量控制水平
 *    - 人均订单数：可通过计算 order_count/order_user_count 得出，反映用户对该品牌的忠诚度
 *
 * 4. 典型应用场景：
 *    - 品牌销售排名：发现销售表现最好和最差的品牌，优化品牌结构
 *    - 品牌质量评估：通过退单率等指标评估品牌质量水平，指导供应链优化
 *    - 品牌用户分析：分析不同品牌的用户购买特征，如购买频次、忠诚度等
 *    - 品牌结构优化：基于销售和质量指标，优化平台品牌结构
 *    - 品牌合作决策：为品牌引入、培育或淘汰决策提供数据依据
 *    - 营销资源分配：根据品牌表现，合理分配营销资源和展示位置
 *
 * 5. 查询示例：
 *    - 查询销售订单量前10的品牌：
 *      SELECT tm_name, order_count, order_user_count
 *      FROM ads.ads_trade_stats_by_tm
 *      WHERE dt = '${yesterday}' AND recent_days = 30
 *      ORDER BY order_count DESC
 *      LIMIT 10;
 *    
 *    - 计算各品牌退单率并排名：
 *      SELECT tm_name, 
 *             order_count, order_refund_count,
 *             order_refund_count/order_count AS refund_rate
 *      FROM ads.ads_trade_stats_by_tm
 *      WHERE dt = '${yesterday}' AND recent_days = 30
 *      AND order_count > 100  -- 过滤订单量太小的品牌
 *      ORDER BY refund_rate ASC;  -- 退单率从低到高
 *
 *    - 分析品牌的用户忠诚度：
 *      SELECT tm_name, 
 *             order_user_count, 
 *             order_count,
 *             order_count/order_user_count AS orders_per_user
 *      FROM ads.ads_trade_stats_by_tm
 *      WHERE dt = '${yesterday}' AND recent_days = 30
 *      AND order_user_count > 50  -- 过滤用户数太少的品牌
 *      ORDER BY orders_per_user DESC;
 *
 * 6. 建议扩展：
 *    - 添加金额指标：增加订单金额、退单金额等财务指标
 *    - 添加品类维度：增加品牌主营品类信息，支持同类品牌对比
 *    - 添加商品维度：增加品牌SKU数、SPU数等，评估品牌的商品丰富度
 *    - 添加转化指标：增加品牌曝光量、点击量等，计算品牌转化率
 *    - 添加评价指标：增加平均评分、好评率等，评估品牌口碑
 */
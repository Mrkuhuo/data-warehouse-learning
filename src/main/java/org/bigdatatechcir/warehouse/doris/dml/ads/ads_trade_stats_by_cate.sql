-- ===============================================================================
-- 各分类商品交易统计报表(ads_trade_stats_by_cate)
-- 功能描述：统计各品类商品的订单量、订单人数、退单量等交易指标
-- 数据来源：dws_trade_user_sku_order_nd、dws_trade_user_sku_order_refund_nd
-- 刷新策略：每日刷新
-- 应用场景：品类销售分析、品类结构优化、退款原因分析
-- ===============================================================================

-- DROP TABLE IF EXISTS ads.ads_trade_stats_by_cate;
CREATE  TABLE ads.ads_trade_stats_by_cate
(
    `dt`          VARCHAR(255) COMMENT '统计日期',
    `recent_days`             BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `category1_id`            STRING COMMENT '一级分类id',
    `category1_name`          STRING COMMENT '一级分类名称',
    `category2_id`            STRING COMMENT '二级分类id',
    `category2_name`          STRING COMMENT '二级分类名称',
    `category3_id`            STRING COMMENT '三级分类id',
    `category3_name`          STRING COMMENT '三级分类名称',
    `order_count`             BIGINT COMMENT '订单数',
    `order_user_count`        BIGINT COMMENT '订单人数',
    `order_refund_count`      BIGINT COMMENT '退单数',
    `order_refund_user_count` BIGINT COMMENT '退单人数'
)
    ENGINE=OLAP
DUPLICATE KEY(`dt`)
COMMENT '各分类商品交易统计'
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
 *    本表是品类销售分析的核心报表，统计各级品类的订单数、用户数、退单数等交易指标，
 *    帮助业务团队了解品类的销售表现和质量情况，为品类结构优化和商品管理提供数据支持。
 *
 * 2. 数据组成与维度：
 *    - 时间维度：统计日期和时间周期(1天/7天/30天)，支持不同时间粒度的品类分析
 *    - 品类维度：包含三级品类结构(一级/二级/三级分类)，支持多级品类分析
 *    - 交易指标：包含订单相关指标和退单相关指标，反映品类的销售量和质量
 *
 * 3. 关键指标解读：
 *    - 订单数：品类产生的订单总数，反映品类销售规模
 *    - 订单人数：购买该品类的用户数，反映品类的用户覆盖面
 *    - 退单数：品类产生的退单总数，反映品类产品质量或满意度问题
 *    - 退单人数：申请退单的用户数，反映遇到问题的用户规模
 *    - 退单率：可通过计算 order_refund_count/order_count 得出，反映品类商品的质量问题比例
 *    - 人均订单数：可通过计算 order_count/order_user_count 得出，反映用户对该品类的购买频次
 *
 * 4. 典型应用场景：
 *    - 品类销售排名：发现销售表现最好和最差的品类，优化产品结构
 *    - 品类质量分析：通过退单率评估品类质量问题，改进品控和供应链
 *    - 品类用户特征：分析不同品类的用户购买行为特征，如购买频次、客单价等
 *    - 销售结构优化：基于各品类销售情况，优化商品结构和营销资源分配
 *    - 退单原因分析：结合退单数据，分析不同品类的退单原因，有针对性地改进
 *
 * 5. 查询示例：
 *    - 查询销售订单量前10的一级品类：
 *      SELECT category1_name, order_count, order_user_count
 *      FROM ads.ads_trade_stats_by_cate
 *      WHERE dt = '${yesterday}' AND recent_days = 30
 *      AND category2_id IS NULL  -- 仅查看一级品类汇总
 *      ORDER BY order_count DESC
 *      LIMIT 10;
 *    
 *    - 计算各品类退单率并排名：
 *      SELECT category1_name, category2_name, 
 *             order_count, order_refund_count,
 *             order_refund_count/order_count AS refund_rate
 *      FROM ads.ads_trade_stats_by_cate
 *      WHERE dt = '${yesterday}' AND recent_days = 30
 *      AND order_count > 100  -- 过滤订单量太小的品类
 *      ORDER BY refund_rate DESC;
 *
 *    - 分析一级品类下各二级品类的销售占比：
 *      WITH category_sales AS (
 *        SELECT category1_id, category1_name, category2_id, category2_name, order_count
 *        FROM ads.ads_trade_stats_by_cate
 *        WHERE dt = '${yesterday}' AND recent_days = 30
 *        AND category2_id IS NOT NULL
 *        AND category3_id IS NULL
 *      )
 *      SELECT category1_name, category2_name, 
 *             order_count,
 *             order_count/SUM(order_count) OVER(PARTITION BY category1_id) AS category_share
 *      FROM category_sales
 *      ORDER BY category1_id, category_share DESC;
 *
 * 6. 建议扩展：
 *    - 添加金额指标：增加订单金额、退单金额等财务指标
 *    - 添加商品维度：增加SKU数、SPU数等，评估品类的商品丰富度
 *    - 添加转化指标：增加曝光量、点击量等，计算品类转化率
 *    - 添加时间指标：增加平均发货时间、退单时间等时效指标
 *    - 添加趋势指标：增加环比、同比增长率，监控品类发展趋势
 */
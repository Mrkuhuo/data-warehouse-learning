-- ===============================================================================
-- 品牌品类交叉分析报表(ads_trade_stats_by_tm_category)
-- 功能描述：基于品牌和品类维度的交叉分析，提供品牌在各品类的表现及占比分析
-- 数据来源：dws_trade_tm_category1_cat_nd
-- 刷新策略：每日刷新
-- 应用场景：品牌竞争分析、品类机会分析、品牌品类交叉分析
-- ===============================================================================

-- DROP TABLE IF EXISTS ads.ads_trade_stats_by_tm_category;
CREATE TABLE ads.ads_trade_stats_by_tm_category
(
    `dt`                    VARCHAR(255) COMMENT '统计日期',
    `recent_days`           BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `tm_id`                 STRING COMMENT '品牌ID',
    `tm_name`               STRING COMMENT '品牌名称',
    `category1_id`          STRING COMMENT '一级品类ID',
    `category1_name`        STRING COMMENT '一级品类名称',
    `order_count`           BIGINT COMMENT '订单数',
    `order_user_count`      BIGINT COMMENT '下单用户数',
    `order_sku_count`       BIGINT COMMENT '下单商品种类数',
    `order_amount`          DECIMAL(16, 2) COMMENT '下单金额',
    `category_amount_ratio` DECIMAL(10, 2) COMMENT '品类金额占比 - 该品牌在此品类的销售额占品类总销售额的百分比(%)',
    `tm_amount_ratio`       DECIMAL(10, 2) COMMENT '品牌金额占比 - 此品类的销售额占该品牌总销售额的百分比(%)',
    `sku_coverage_rate`     DECIMAL(10, 2) COMMENT '品牌SKU覆盖率 - 该品牌在此品类销售的SKU数占品类SKU总数的百分比(%)',
    `wow_rate`              DECIMAL(10, 2) COMMENT '周环比变化率 - 与上周同期相比的增长率(%)',
    `yoy_rate`              DECIMAL(10, 2) COMMENT '同比变化率 - 与去年同期相比的增长率(%)',
    `growth_type`           STRING COMMENT '增长类型 - 高速增长/平稳增长/下降/急剧下降'
)
ENGINE=OLAP
DUPLICATE KEY(`dt`)
COMMENT '品牌品类交叉分析报表'
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
 *    本表是品牌与品类维度交叉分析的核心报表，通过多维度指标展示品牌在各品类中的竞争力和表现。
 *    能够帮助业务方快速识别品牌的优势品类、发展机会和潜在风险，为品牌战略决策提供数据支持。
 *
 * 2. 主要分析维度：
 *    - 时间维度：支持按日、周、月不同时间粒度分析，可进行趋势和同环比分析
 *    - 品牌维度：展示品牌整体表现，可比较不同品牌在同一品类的竞争表现
 *    - 品类维度：展示品类整体表现，可分析同一品牌在不同品类的布局情况
 *
 * 3. 核心指标解读：
 *    - 销售量值指标：订单数、用户数、SKU种类数、销售金额，反映基础业绩表现
 *    - 占比指标：品类金额占比反映市场份额，品牌金额占比反映品牌结构
 *    - SKU覆盖率：反映品牌在品类中的产品丰富度和布局完整性
 *    - 增长指标：通过环比和同比分析增长趋势，识别增长机会和风险
 *    - 分类标记：直观展示品牌品类组合的业绩表现类型，便于快速筛选
 *
 * 4. 典型应用场景：
 *    - 品类机会识别：发现品类增长潜力，为品牌拓展新品类提供方向
 *    - 品牌竞争分析：对比不同品牌在相同品类的表现，发现竞争优劣势
 *    - 资源优化配置：基于品牌在各品类表现，优化营销资源和供应链资源配置
 *    - 品牌结构优化：分析品牌销售结构，优化品类聚焦策略
 *    - 异常监控预警：通过同环比变化发现异常波动，及时干预
 *
 * 5. 查询示例：
 *    - 查找增长最快的品牌品类组合：
 *      SELECT tm_name, category1_name, wow_rate, order_amount
 *      FROM ads.ads_trade_stats_by_tm_category
 *      WHERE dt = '${yesterday}' AND recent_days = 7
 *      ORDER BY wow_rate DESC
 *      LIMIT 10;
 *    
 *    - 分析品牌在各品类的销售结构：
 *      SELECT tm_name, category1_name, order_amount, tm_amount_ratio
 *      FROM ads.ads_trade_stats_by_tm_category
 *      WHERE dt = '${yesterday}' AND recent_days = 30 AND tm_id = '${tm_id}'
 *      ORDER BY order_amount DESC;
 *
 *    - 分析品类市场份额格局：
 *      SELECT tm_name, order_amount, category_amount_ratio
 *      FROM ads.ads_trade_stats_by_tm_category
 *      WHERE dt = '${yesterday}' AND recent_days = 30 AND category1_id = '${category1_id}'
 *      ORDER BY category_amount_ratio DESC;
 */ 
/*
 * 表名: dws_trade_tm_category1_cat_nd
 * 说明: 交易域品牌一级品类粒度最近n日汇总表
 * 数据粒度: 品牌 + 一级品类 + 日期
 * 存储策略: 分区存储，按日期分区
 * 刷新周期: 每日刷新
 * 应用场景:
 *   - 品牌销售分析：监控品牌整体销售趋势和季节性表现
 *   - 品类销售分析：分析不同品类销售热度和市场竞争格局
 *   - 品牌与品类交叉分析：发现品牌在不同品类的竞争优势与劣势
 *   - 销售趋势与异常监控：通过增长率变化发现业务异常或机会
 *   - 品牌品类占比分析：评估品牌在各品类中的市场份额
 */
-- DROP TABLE IF EXISTS dws.dws_trade_tm_category1_cat_nd;
CREATE TABLE dws.dws_trade_tm_category1_cat_nd
(
    /* 维度字段 */
    `tm_id`                     VARCHAR(16) COMMENT '品牌ID - 标识品牌',
    `category1_id`              VARCHAR(16) COMMENT '一级品类ID - 标识商品大类',
    `k1`                        DATE NOT NULL COMMENT '数据日期 - 分区字段，记录数据所属日期',

    /* 冗余维度 - 用于提高分析效率，避免关联查询 */
    `tm_name`                   STRING COMMENT '品牌名称 - 展示品牌信息',
    `category1_name`            STRING COMMENT '一级品类名称 - 展示品类信息',

    /* 度量值字段 - 1日汇总 */
    `order_count_1d`            BIGINT COMMENT '最近1日下单次数 - 订单总数',
    `order_num_1d`              BIGINT COMMENT '最近1日下单件数 - 商品总数量',
    `order_user_count_1d`       BIGINT COMMENT '最近1日下单用户数 - 下单用户去重数',
    `order_sku_count_1d`        BIGINT COMMENT '最近1日下单商品种类数 - 购买的SKU种类数',
    `order_original_amount_1d`  DECIMAL(16, 2) COMMENT '最近1日下单原始金额 - 未优惠的原始总金额',
    `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT '最近1日活动优惠金额 - 活动带来的优惠总金额',
    `coupon_reduce_amount_1d`   DECIMAL(16, 2) COMMENT '最近1日优惠券优惠金额 - 优惠券带来的优惠总金额',
    `order_total_amount_1d`     DECIMAL(16, 2) COMMENT '最近1日下单最终金额 - 优惠后的实际支付总金额',

    /* 度量值字段 - 7日汇总 */
    `order_count_7d`            BIGINT COMMENT '最近7日下单次数 - 7天内订单总数',
    `order_num_7d`              BIGINT COMMENT '最近7日下单件数 - 7天内商品总数量',
    `order_user_count_7d`       BIGINT COMMENT '最近7日下单用户数 - 7天内下单用户去重数',
    `order_sku_count_7d`        BIGINT COMMENT '最近7日下单商品种类数 - 7天内购买的SKU种类数',
    `order_original_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日下单原始金额 - 7天内未优惠的原始总金额',
    `activity_reduce_amount_7d` DECIMAL(16, 2) COMMENT '最近7日活动优惠金额 - 7天内活动带来的优惠总金额',
    `coupon_reduce_amount_7d`   DECIMAL(16, 2) COMMENT '最近7日优惠券优惠金额 - 7天内优惠券带来的优惠总金额',
    `order_total_amount_7d`     DECIMAL(16, 2) COMMENT '最近7日下单最终金额 - 7天内优惠后的实际支付总金额',

    /* 度量值字段 - 30日汇总 */
    `order_count_30d`            BIGINT COMMENT '最近30日下单次数 - 30天内订单总数',
    `order_num_30d`              BIGINT COMMENT '最近30日下单件数 - 30天内商品总数量',
    `order_user_count_30d`       BIGINT COMMENT '最近30日下单用户数 - 30天内下单用户去重数',
    `order_sku_count_30d`        BIGINT COMMENT '最近30日下单商品种类数 - 30天内购买的SKU种类数',
    `order_original_amount_30d`  DECIMAL(16, 2) COMMENT '最近30日下单原始金额 - 30天内未优惠的原始总金额',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '最近30日活动优惠金额 - 30天内活动带来的优惠总金额',
    `coupon_reduce_amount_30d`   DECIMAL(16, 2) COMMENT '最近30日优惠券优惠金额 - 30天内优惠券带来的优惠总金额',
    `order_total_amount_30d`     DECIMAL(16, 2) COMMENT '最近30日下单最终金额 - 30天内优惠后的实际支付总金额',

    /* 环比增长字段 - 与前一天/前一周相比 */
    `order_count_1d_wow_rate`       DECIMAL(10, 2) COMMENT '最近1日下单次数周环比 - 与上周同期相比的增长率(%)',
    `order_total_amount_1d_wow_rate` DECIMAL(10, 2) COMMENT '最近1日下单金额周环比 - 与上周同期相比的增长率(%)',
    `order_count_7d_wow_rate`       DECIMAL(10, 2) COMMENT '最近7日下单次数周环比 - 与前7天相比的增长率(%)',
    `order_total_amount_7d_wow_rate` DECIMAL(10, 2) COMMENT '最近7日下单金额周环比 - 与前7天相比的增长率(%)',

    /* 同比增长字段 - 与去年同期相比 */
    `order_count_1d_yoy_rate`       DECIMAL(10, 2) COMMENT '最近1日下单次数同比 - 与去年同期相比的增长率(%)',
    `order_total_amount_1d_yoy_rate` DECIMAL(10, 2) COMMENT '最近1日下单金额同比 - 与去年同期相比的增长率(%)',

    /* 份额和覆盖率指标 */
    `category_amount_ratio_1d`     DECIMAL(10, 2) COMMENT '品类金额占比 - 该品牌在此品类的销售额占品类总销售额的百分比(%)',
    `tm_amount_ratio_1d`           DECIMAL(10, 2) COMMENT '品牌金额占比 - 此品类的销售额占该品牌总销售额的百分比(%)',
    `sku_coverage_rate_1d`         DECIMAL(10, 2) COMMENT '品牌SKU覆盖率 - 该品牌在此品类销售的SKU数占品类SKU总数的百分比(%)',

    /* 类型标记 - 用于标记品牌&品类业绩表现 */
    `cat_1d_type`                STRING COMMENT '最近1日增长类型 - 高速增长/平稳增长/下降/急剧下降',
    `cat_7d_type`                STRING COMMENT '最近7日增长类型 - 高速增长/平稳增长/下降/急剧下降'
)
    ENGINE=OLAP
    UNIQUE KEY(`tm_id`, `category1_id`, `k1`)
COMMENT '交易域品牌一级品类粒度最近n日汇总表'
PARTITION BY RANGE(`k1`) ()
DISTRIBUTED BY HASH(`tm_id`, `category1_id`) BUCKETS 16
PROPERTIES
(
    "replication_allocation" = "tag.location.default: 1",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-90",   /* 保留90天历史数据 */
    "dynamic_partition.end" = "3",       /* 预创建未来3天分区 */
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "16",
    "dynamic_partition.create_history_partition" = "true"
);

/*
 * 表设计说明：
 *
 * 1. 主键与分布设计：
 *    - 主键：使用tm_id + category1_id + k1复合主键，支持多维度查询
 *    - 分桶：按品牌和一级品类共同分桶，优化品牌品类交叉分析的查询性能
 *    - 分区：使用日期（k1）分区，支持历史数据生命周期管理，提升查询效率
 *
 * 2. 数据组织：
 *    - 多维度分析：聚焦品牌和品类两个核心业务维度的交叉分析
 *    - 多周期聚合：同时存储1日、7日、30日三个时间窗口的指标，满足不同周期的分析需求
 *    - 多角度指标：既有基础的销售量值指标，也有增长率、占比等衍生指标，还有业务分类标记
 * 
 * 3. 特色指标设计：
 *    - SKU种类数统计：反映品牌在品类中的产品丰富度
 *    - 增长率指标：同时支持环比和同比两种时间比较维度
 *    - 占比分析：同时从品牌和品类两个视角提供占比指标
 *    - 业务分类：直接将增长状态归类，便于业务快速筛选
 *
 * 4. 典型应用场景：
 *    - 品牌竞争力分析：通过品牌在各品类的表现，评估品牌的整体竞争力
 *    - 品类机会识别：发现增长迅速的品类，为品牌拓展提供方向
 *    - 品牌聚焦度分析：通过品牌在各品类的分布，分析品牌的市场聚焦策略
 *    - 异常监控：通过增长类型标记，快速识别异常波动的品牌品类组合
 *    - 季节性分析：利用同比数据，识别品牌品类的季节性特征
 *
 * 5. 优化建议：
 *    - 关联分析：可与用户标签表关联，分析不同品牌品类的用户群体特征
 *    - 价格区间分析：考虑增加价格区间分布统计，分析品牌的价格策略
 *    - 预测应用：基于历史数据训练预测模型，预测品牌品类未来表现
 */ 
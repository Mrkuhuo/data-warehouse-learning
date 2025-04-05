/*
 * 表名: dws_trade_province_sku_order_nd
 * 说明: 交易域省份商品粒度订单最近n日汇总事实表
 * 数据粒度: 省份 + 商品 + 日期
 * 存储策略: 分区存储，按日期分区
 * 刷新周期: 每日刷新
 * 应用场景:
 *   - 区域销售业绩分析
 *   - 区域商品偏好分析
 *   - 商品区域销售热度图
 *   - 商品区域销售对比分析
 *   - 趋势异常监控与预警
 *   - 季节性销售分析
 */
-- DROP TABLE IF EXISTS dws.dws_trade_province_sku_order_nd;
CREATE TABLE dws.dws_trade_province_sku_order_nd
(
    /* 维度字段 */
    `province_id`               VARCHAR(16) COMMENT '省份ID - 标识销售区域',
    `sku_id`                    VARCHAR(255) COMMENT '商品SKU_ID - 标识具体售卖的商品',
    `k1`                        DATE NOT NULL COMMENT '数据日期 - 分区字段，记录数据所属日期',
    
    /* 冗余维度 - 用于提高分析效率，避免关联查询 */
    `province_name`             STRING COMMENT '省份名称 - 用于展示销售区域',
    `province_area_code`        STRING COMMENT '省份区号 - 电话区号，可用于地区分组',
    `province_iso_code`         STRING COMMENT '省份ISO编码 - 国际标准编码',
    `sku_name`                  STRING COMMENT '商品名称 - 展示购买的商品名称',
    `category1_id`              STRING COMMENT '一级品类ID - 商品所属一级品类',
    `category1_name`            STRING COMMENT '一级品类名称 - 商品所属一级品类名称',
    `category2_id`              STRING COMMENT '二级品类ID - 商品所属二级品类',
    `category2_name`            STRING COMMENT '二级品类名称 - 商品所属二级品类名称',
    `category3_id`              STRING COMMENT '三级品类ID - 商品所属三级品类',
    `category3_name`            STRING COMMENT '三级品类名称 - 商品所属三级品类名称',
    `tm_id`                     STRING COMMENT '品牌ID - 商品所属品牌',
    `tm_name`                   STRING COMMENT '品牌名称 - 商品所属品牌名称',
    
    /* 度量值字段 - 1日汇总 */
    `order_count_1d`            BIGINT COMMENT '最近1日下单次数 - 订单总数',
    `order_num_1d`              BIGINT COMMENT '最近1日下单件数 - 商品总数量',
    `order_user_count_1d`       BIGINT COMMENT '最近1日下单用户数 - 下单用户去重数',
    `order_original_amount_1d`  DECIMAL(16, 2) COMMENT '最近1日下单原始金额 - 未优惠的原始总金额',
    `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT '最近1日活动优惠金额 - 活动带来的优惠总金额',
    `coupon_reduce_amount_1d`   DECIMAL(16, 2) COMMENT '最近1日优惠券优惠金额 - 优惠券带来的优惠总金额',
    `order_total_amount_1d`     DECIMAL(16, 2) COMMENT '最近1日下单最终金额 - 优惠后的实际支付总金额',
    
    /* 度量值字段 - 7日汇总 */
    `order_count_7d`            BIGINT COMMENT '最近7日下单次数 - 7天内订单总数',
    `order_num_7d`              BIGINT COMMENT '最近7日下单件数 - 7天内商品总数量',
    `order_user_count_7d`       BIGINT COMMENT '最近7日下单用户数 - 7天内下单用户去重数',
    `order_original_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日下单原始金额 - 7天内未优惠的原始总金额',
    `activity_reduce_amount_7d` DECIMAL(16, 2) COMMENT '最近7日活动优惠金额 - 7天内活动带来的优惠总金额',
    `coupon_reduce_amount_7d`   DECIMAL(16, 2) COMMENT '最近7日优惠券优惠金额 - 7天内优惠券带来的优惠总金额',
    `order_total_amount_7d`     DECIMAL(16, 2) COMMENT '最近7日下单最终金额 - 7天内优惠后的实际支付总金额',
    
    /* 度量值字段 - 30日汇总 */
    `order_count_30d`            BIGINT COMMENT '最近30日下单次数 - 30天内订单总数',
    `order_num_30d`              BIGINT COMMENT '最近30日下单件数 - 30天内商品总数量',
    `order_user_count_30d`       BIGINT COMMENT '最近30日下单用户数 - 30天内下单用户去重数',
    `order_original_amount_30d`  DECIMAL(16, 2) COMMENT '最近30日下单原始金额 - 30天内未优惠的原始总金额',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '最近30日活动优惠金额 - 30天内活动带来的优惠总金额',
    `coupon_reduce_amount_30d`   DECIMAL(16, 2) COMMENT '最近30日优惠券优惠金额 - 30天内优惠券带来的优惠总金额',
    `order_total_amount_30d`     DECIMAL(16, 2) COMMENT '最近30日下单最终金额 - 30天内优惠后的实际支付总金额',
    
    /* 趋势分析指标 - 环比和同比 */
    `order_count_1d_wow_rate`             DECIMAL(10, 2) COMMENT '订单数量周环比变化率 - 与上周同期相比的变化百分比',
    `order_count_1d_yoy_rate`             DECIMAL(10, 2) COMMENT '订单数量同比变化率 - 与去年同期相比的变化百分比',
    `order_total_amount_1d_wow_rate`      DECIMAL(10, 2) COMMENT '订单金额周环比变化率 - 与上周同期相比的变化百分比',
    `order_total_amount_1d_yoy_rate`      DECIMAL(10, 2) COMMENT '订单金额同比变化率 - 与去年同期相比的变化百分比'
)
ENGINE=OLAP
UNIQUE KEY(`province_id`, `sku_id`, `k1`)
COMMENT '交易域省份商品粒度订单最近n日汇总事实表'
PARTITION BY RANGE(`k1`) ()
DISTRIBUTED BY HASH(`province_id`) BUCKETS 16
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
 *    - 主键：使用province_id + sku_id + k1复合主键，支持多维度查询
 *    - 分桶：按province_id分桶，由于省份数量有限且查询时经常按地区筛选，能更好地支持分布式计算
 *    - 分区：使用日期（k1）分区，支持历史数据生命周期管理，提升查询效率
 *
 * 2. 数据组织：
 *    - 维度分组：清晰区分维度字段、冗余维度和度量字段
 *    - 多周期聚合：同时存储1日、7日、30日三个时间窗口的指标，满足不同周期的分析需求
 *    - 趋势指标：新增环比和同比指标，提供直观的趋势判断数据，无需额外计算
 * 
 * 3. 性能优化：
 *    - 冗余存储：冗余存储维度属性（如省份名称、商品名称等），避免查询时的多表关联
 *    - 压缩存储：使用zstd压缩算法，在保证查询性能的同时降低存储成本
 *    - 动态分区：自动管理历史数据，保留90天数据用于季度级分析
 *
 * 4. 数据应用：
 *    - 空间分析：支持地域级别的商品销售分析，可与地图结合提供可视化分析
 *    - 时间分析：通过多周期汇总数据，可分析短期、中期趋势变化
 *    - 趋势预警：通过环比和同比指标，快速发现异常数据波动
 *    - 季节性分析：利用同比数据，识别商品销售的季节性特征
 *
 * 5. 扩展建议：
 *    - 用户行为分析：可考虑增加用户复购率、平均客单价等衍生指标
 *    - 商品组合分析：可与关联商品分析表结合，发现区域商品搭配特征
 *    - 预测模型支持：结构化的时间序列数据为销售预测模型提供基础数据
 */ 
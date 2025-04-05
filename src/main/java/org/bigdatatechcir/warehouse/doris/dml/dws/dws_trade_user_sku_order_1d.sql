/*
 * 表名: dws_trade_user_sku_order_1d
 * 说明: 交易域用户商品粒度订单最近1日汇总事实表
 * 数据粒度: 用户 + SKU + 日期
 * 存储策略: 分区存储，按日期分区
 * 刷新周期: 每日刷新
 * 应用场景:
 *   - 用户对特定商品的消费行为分析：识别用户对不同商品的购买偏好和消费模式
 *   - 用户偏好分析：结合品类和品牌维度，分析用户的购物兴趣和偏好变化
 *   - 商品复购率分析：追踪用户对同一商品的重复购买行为，评估产品粘性
 *   - 商品销售趋势分析：监控商品日度销售情况，及时把握销售变化
 *   - 促销活动效果评估：分析优惠活动和优惠券对特定商品销售的促进效果
 */
-- DROP TABLE IF EXISTS dws.dws_trade_user_sku_order_1d;
CREATE TABLE dws.dws_trade_user_sku_order_1d
(
    /* 维度字段 */
    `user_id`                   VARCHAR(255) COMMENT '用户ID - 用于识别下单用户',
    `sku_id`                    VARCHAR(255) COMMENT '商品SKU_ID - 标识具体售卖的商品',
    `k1`                        DATE NOT NULL COMMENT '数据日期 - 分区字段，记录数据所属日期',
    
    /* 冗余维度 - 用于提高分析效率，避免关联查询 */
    `sku_name`                  STRING COMMENT '商品名称 - 展示购买的商品名称',
    `category1_id`              STRING COMMENT '一级品类ID - 商品所属一级品类',
    `category1_name`            STRING COMMENT '一级品类名称 - 商品所属一级品类名称',
    `category2_id`              STRING COMMENT '二级品类ID - 商品所属二级品类',
    `category2_name`            STRING COMMENT '二级品类名称 - 商品所属二级品类名称',
    `category3_id`              STRING COMMENT '三级品类ID - 商品所属三级品类',
    `category3_name`            STRING COMMENT '三级品类名称 - 商品所属三级品类名称',
    `tm_id`                     STRING COMMENT '品牌ID - 商品所属品牌',
    `tm_name`                   STRING COMMENT '品牌名称 - 商品所属品牌名称',
    
    /* 度量值字段 - 订单统计 */
    `order_count_1d`            BIGINT COMMENT '最近1日下单次数 - 用户对该商品的下单次数',
    `order_num_1d`              BIGINT COMMENT '最近1日下单件数 - 用户购买该商品的总数量',
    
    /* 度量值字段 - 金额统计 */
    `order_original_amount_1d`  DECIMAL(16, 2) COMMENT '最近1日下单原始金额 - 未优惠的原始总金额',
    `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT '最近1日活动优惠金额 - 活动带来的优惠总金额',
    `coupon_reduce_amount_1d`   DECIMAL(16, 2) COMMENT '最近1日优惠券优惠金额 - 优惠券带来的优惠总金额',
    `order_total_amount_1d`     DECIMAL(16, 2) COMMENT '最近1日下单最终金额 - 优惠后的实际支付总金额'
)
ENGINE=OLAP
UNIQUE KEY(`user_id`, `sku_id`, `k1`)
COMMENT '交易域用户商品粒度订单最近1日汇总事实表'
PARTITION BY RANGE(`k1`) ()
DISTRIBUTED BY HASH(`user_id`) BUCKETS 32
PROPERTIES
(
    "replication_allocation" = "tag.location.default: 1",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false",
    "dynamic_partition.enable" = "true",           /* 启用动态分区功能 */
    "dynamic_partition.time_unit" = "DAY",         /* 按天进行分区 */
    "dynamic_partition.start" = "-90",             /* 保留90天历史数据 */
    "dynamic_partition.end" = "3",                 /* 预创建未来3天分区 */
    "dynamic_partition.prefix" = "p",              /* 分区名称前缀 */
    "dynamic_partition.buckets" = "32",            /* 每个分区的分桶数 */
    "dynamic_partition.create_history_partition" = "true" /* 创建历史分区 */
);

/*
 * 表设计说明：
 *
 * 1. 主键与分布设计：
 *    - 主键：使用user_id + sku_id + k1复合主键，支持用户对特定商品在不同日期的购买行为分析
 *    - 分桶：按用户ID分桶(32桶)，优化用户级查询性能，适合用户维度的分析场景
 *    - 分区：使用日期（k1）分区，支持历史数据生命周期管理，提升时间范围查询效率
 *
 * 2. 数据组织：
 *    - 多级品类维度：完整保留三级品类信息，支持不同粒度的品类分析
 *    - 品牌维度：包含品牌信息，便于品牌销售分析和品牌偏好研究
 *    - 订单指标：记录订单次数和商品件数，反映购买规模
 *    - 金额指标：包含原始金额、优惠金额和最终金额，支持促销效果分析
 * 
 * 3. 存储管理：
 *    - 动态分区：自动管理历史数据，保留最近90天数据，预创建3天分区
 *    - 存储优化：使用zstd压缩算法和V2存储格式，平衡查询性能和存储效率
 *    - 冗余设计：通过冗余存储维度属性，避免频繁关联维度表，提升查询效率
 *
 * 4. 典型应用场景：
 *    - 用户商品偏好分析：识别用户最常购买的商品类型和品牌
 *    - 商品销售热度监控：追踪特定商品的日常销售情况和波动
 *    - 促销活动效果分析：评估不同促销方式（活动、优惠券）对商品销售的影响
 *    - 用户消费习惯研究：分析用户在不同品类和品牌间的消费分布
 *    - 库存管理决策支持：根据商品销售情况，为库存管理提供数据支持
 *
 * 5. 优化建议：
 *    - 扩展为N日汇总表：增加7日、30日汇总指标，支持中长期分析
 *    - 添加同比环比指标：计算与去年同期和前一天的销售对比，反映趋势变化
 *    - 增加支付转化指标：结合支付表数据，计算从下单到支付的转化率
 *    - 添加价格计算指标：如平均单价、优惠力度占比等，便于价格策略分析
 *    - 考虑季节性分析：针对有季节性特征的商品，增加季节性比较维度
 */
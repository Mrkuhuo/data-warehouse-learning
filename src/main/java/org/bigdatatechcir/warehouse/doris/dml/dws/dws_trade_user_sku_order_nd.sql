/*
 * 表名: dws_trade_user_sku_order_nd
 * 说明: 交易域用户商品粒度订单最近n日汇总事实表
 * 数据粒度: 用户 + SKU + 日期
 * 存储策略: 分区存储，按日期分区
 * 刷新周期: 每日刷新
 * 应用场景:
 *   - 用户中长期购买行为分析：识别用户在更长时间范围内的商品偏好和消费模式
 *   - 商品销售稳定性分析：评估商品在7日和30日维度的销售趋势和波动
 *   - 品牌和品类渗透率研究：分析品牌和品类在不同时间跨度的市场表现
 *   - 促销活动长效性评估：衡量促销活动对商品销售的中长期影响
 *   - 用户购买频率研究：结合1日表分析用户对特定商品的重复购买行为
 */
DROP TABLE IF EXISTS dws.dws_trade_user_sku_order_nd;
CREATE TABLE dws.dws_trade_user_sku_order_nd
(
    /* 维度字段 */
    `user_id`                    VARCHAR(255) COMMENT '用户ID - 用于识别下单用户',
    `sku_id`                     VARCHAR(255) COMMENT '商品SKU_ID - 标识具体售卖的商品',
    `k1`                         DATE NOT NULL COMMENT '数据日期 - 分区字段，记录数据所属日期',
    
    /* 冗余维度 - 用于提高分析效率，避免关联查询 */
    `sku_name`                   STRING COMMENT '商品名称 - 展示购买的商品名称',
    `category1_id`               STRING COMMENT '一级品类ID - 商品所属一级品类',
    `category1_name`             STRING COMMENT '一级品类名称 - 商品所属一级品类名称',
    `category2_id`               STRING COMMENT '二级品类ID - 商品所属二级品类',
    `category2_name`             STRING COMMENT '二级品类名称 - 商品所属二级品类名称',
    `category3_id`               STRING COMMENT '三级品类ID - 商品所属三级品类',
    `category3_name`             STRING COMMENT '三级品类名称 - 商品所属三级品类名称',
    `tm_id`                      STRING COMMENT '品牌ID - 商品所属品牌',
    `tm_name`                    STRING COMMENT '品牌名称 - 商品所属品牌名称',
    
    /* 度量值字段 - 7日订单统计 */
    `order_count_7d`             BIGINT COMMENT '最近7日下单次数 - 用户对该商品的近7天下单次数',
    `order_num_7d`               BIGINT COMMENT '最近7日下单件数 - 用户近7天购买该商品的总数量',
    `order_original_amount_7d`   DECIMAL(16, 2) COMMENT '最近7日下单原始金额 - 近7天未优惠的原始总金额',
    `activity_reduce_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日活动优惠金额 - 近7天活动带来的优惠总金额',
    `coupon_reduce_amount_7d`    DECIMAL(16, 2) COMMENT '最近7日优惠券优惠金额 - 近7天优惠券带来的优惠总金额',
    `order_total_amount_7d`      DECIMAL(16, 2) COMMENT '最近7日下单最终金额 - 近7天优惠后的实际支付总金额',
    
    /* 度量值字段 - 30日订单统计 */
    `order_count_30d`            BIGINT COMMENT '最近30日下单次数 - 用户对该商品的近30天下单次数',
    `order_num_30d`              BIGINT COMMENT '最近30日下单件数 - 用户近30天购买该商品的总数量',
    `order_original_amount_30d`  DECIMAL(16, 2) COMMENT '最近30日下单原始金额 - 近30天未优惠的原始总金额',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '最近30日活动优惠金额 - 近30天活动带来的优惠总金额',
    `coupon_reduce_amount_30d`   DECIMAL(16, 2) COMMENT '最近30日优惠券优惠金额 - 近30天优惠券带来的优惠总金额',
    `order_total_amount_30d`     DECIMAL(16, 2) COMMENT '最近30日下单最终金额 - 近30天优惠后的实际支付总金额'
)
ENGINE=OLAP
UNIQUE KEY(`user_id`,`sku_id`,`k1`)
COMMENT '交易域用户商品粒度订单最近n日汇总事实表'
PARTITION BY RANGE(`k1`) ()
DISTRIBUTED BY HASH(`user_id`) BUCKETS 32
PROPERTIES
(
    "replication_allocation" = "tag.location.default: 1",
    "is_being_synced" = "false",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false",
    "dynamic_partition.enable" = "true",           /* 启用动态分区功能 */
    "dynamic_partition.time_unit" = "DAY",         /* 按天进行分区 */
    "dynamic_partition.start" = "-60",             /* 保留60天历史数据 */
    "dynamic_partition.end" = "3",                 /* 预创建未来3天分区 */
    "dynamic_partition.prefix" = "p",              /* 分区名称前缀 */
    "dynamic_partition.buckets" = "32",            /* 每个分区的分桶数 */
    "dynamic_partition.create_history_partition" = "true", /* 创建历史分区 */
    "data_property.compression_type" = "zstd"      /* 使用zstd压缩算法提高存储效率 */
);

/*
 * 表设计说明：
 *
 * 1. 主键与分布设计：
 *    - 主键：使用user_id + sku_id + k1复合主键，支持用户对特定商品在不同日期的中长期购买行为分析
 *    - 分桶：按用户ID分桶(32桶)，优化用户级查询性能，适合用户维度的分析场景
 *    - 分区：使用日期（k1）分区，支持历史数据生命周期管理，提升时间范围查询效率
 *
 * 2. 数据组织：
 *    - 多级品类维度：完整保留三级品类信息，支持不同粒度的品类分析
 *    - 品牌维度：包含品牌信息，便于品牌销售分析和品牌偏好研究
 *    - 时间维度：分别提供7日和30日两个时间窗口的汇总指标，覆盖中短期和中长期分析
 *    - 订单与金额指标：包含订单次数、件数和不同类型金额，全面反映购买规模和优惠效果
 * 
 * 3. 存储管理：
 *    - 动态分区：自动管理历史数据，保留最近60天数据，预创建3天分区
 *    - 存储优化：使用zstd压缩算法和V2存储格式，平衡查询性能和存储效率
 *    - 冗余设计：通过冗余存储维度属性，避免频繁关联维度表，提升查询效率
 *
 * 4. 典型应用场景：
 *    - 用户中长期偏好分析：分析用户对商品、品类和品牌的持续偏好
 *    - 商品销售周期研究：通过比较7日和30日指标，识别商品销售的周期性特征
 *    - 促销效果跟踪：评估促销活动对商品销售的短期和中期影响差异
 *    - 品类渗透率研究：分析不同品类在用户购物篮中的持续占比
 *    - 复购率分析：与1日表联合分析，评估商品的留存率和用户粘性
 *
 * 5. 优化建议：
 *    - 添加同比环比指标：计算7日和30日的同比环比增长率，反映趋势变化
 *    - 增加支付转化指标：结合支付表数据，计算从下单到支付的转化率
 *    - 添加客单价计算：增加平均单价指标，便于商品价格策略分析
 *    - 扩展为更多时间维度：考虑增加90日、180日等更长周期的汇总指标
 *    - 添加用户首单标记：标识用户对该商品的首次购买时间，分析用户获取情况
 */
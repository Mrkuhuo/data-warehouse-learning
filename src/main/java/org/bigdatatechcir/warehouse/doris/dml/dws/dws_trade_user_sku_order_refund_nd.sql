/*
 * 表名: dws_trade_user_sku_order_refund_nd
 * 说明: 交易域用户商品粒度退单最近n日汇总事实表
 * 数据粒度: 用户 + SKU + 日期
 * 存储策略: 分区存储，按日期分区
 * 刷新周期: 每日刷新
 * 应用场景:
 *   - 用户中长期退货行为分析：识别用户在更长时间范围内的退货模式和偏好
 *   - 商品退货风险评估：通过7日和30日维度评估商品的持续退货情况和质量稳定性
 *   - 品牌和品类退货率对比：分析不同品牌和品类在中长期内的退货表现差异
 *   - 退货趋势监控：识别特定商品或品类退货量的异常增长趋势
 *   - 用户满意度评估：通过中长期退货行为间接评估用户对产品的持续满意度
 */
-- DROP TABLE IF EXISTS dws.dws_trade_user_sku_order_refund_nd;
CREATE TABLE dws.dws_trade_user_sku_order_refund_nd
(
    /* 维度字段 */
    `user_id`                    VARCHAR(255) COMMENT '用户ID - 用于识别退单用户',
    `sku_id`                     VARCHAR(255) COMMENT '商品SKU_ID - 标识被退货的具体商品',
    `k1`                         DATE NOT NULL COMMENT '数据日期 - 分区字段，记录数据所属日期',
    
    /* 冗余维度 - 用于提高分析效率，避免关联查询 */
    `sku_name`                   STRING COMMENT '商品名称 - 展示退货的商品名称',
    `category1_id`               STRING COMMENT '一级品类ID - 商品所属一级品类',
    `category1_name`             STRING COMMENT '一级品类名称 - 商品所属一级品类名称',
    `category2_id`               STRING COMMENT '二级品类ID - 商品所属二级品类',
    `category2_name`             STRING COMMENT '二级品类名称 - 商品所属二级品类名称',
    `category3_id`               STRING COMMENT '三级品类ID - 商品所属三级品类',
    `category3_name`             STRING COMMENT '三级品类名称 - 商品所属三级品类名称',
    `tm_id`                      STRING COMMENT '品牌ID - 商品所属品牌',
    `tm_name`                    STRING COMMENT '品牌名称 - 商品所属品牌名称',
    
    /* 度量值字段 - 7日退单统计 */
    `order_refund_count_7d`      BIGINT COMMENT '最近7日退单次数 - 用户对该商品的近7天退单次数',
    `order_refund_num_7d`        BIGINT COMMENT '最近7日退单件数 - 用户近7天退回该商品的总数量',
    `order_refund_amount_7d`     DECIMAL(16, 2) COMMENT '最近7日退单金额 - 近7天退回该商品的总金额',
    
    /* 度量值字段 - 30日退单统计 */
    `order_refund_count_30d`     BIGINT COMMENT '最近30日退单次数 - 用户对该商品的近30天退单次数',
    `order_refund_num_30d`       BIGINT COMMENT '最近30日退单件数 - 用户近30天退回该商品的总数量',
    `order_refund_amount_30d`    DECIMAL(16, 2) COMMENT '最近30日退单金额 - 近30天退回该商品的总金额'
)
ENGINE=OLAP
UNIQUE KEY(`user_id`,`sku_id`,`k1`)
COMMENT '交易域用户商品粒度退单最近n日汇总事实表'
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
    "dynamic_partition.create_history_partition" = "true" /* 创建历史分区 */
);

/*
 * 表设计说明：
 *
 * 1. 主键与分布设计：
 *    - 主键：使用user_id + sku_id + k1复合主键，支持用户对特定商品在不同日期的中长期退货行为分析
 *    - 分桶：按用户ID分桶(32桶)，优化用户级查询性能，适合用户维度的退货分析场景
 *    - 分区：使用日期（k1）分区，支持历史数据生命周期管理，提升时间范围查询效率
 *
 * 2. 数据组织：
 *    - 多级品类维度：完整保留三级品类信息，支持不同粒度的品类退货分析
 *    - 品牌维度：包含品牌信息，便于品牌退货分析和品牌质量评估
 *    - 时间维度：分别提供7日和30日两个时间窗口的汇总指标，覆盖中短期和中长期退货分析
 *    - 退单指标：包含退单次数、件数和金额，全面反映退货规模和经济影响
 * 
 * 3. 存储管理：
 *    - 动态分区：自动管理历史数据，保留最近60天数据，预创建3天分区
 *    - 存储优化：使用zstd压缩算法和V2存储格式，平衡查询性能和存储效率
 *    - 冗余设计：通过冗余存储维度属性，避免频繁关联维度表，提升查询效率
 *
 * 4. 典型应用场景：
 *    - 商品质量问题监控：通过分析7日和30日退货数据，及时发现商品质量问题
 *    - 品类退货规律研究：分析不同品类的中长期退货情况，评估品类结构合理性
 *    - 用户退货行为画像：构建用户的中长期退货行为特征，识别高风险用户
 *    - 季节性退货趋势：比较不同时间窗口的退货情况，发现退货的季节性模式
 *    - 与订单表联合分析：结合订单表数据，计算商品的净留存率和长期转化情况
 *
 * 5. 优化建议：
 *    - 添加退货原因维度：增加退货原因分析，深入了解不同时间窗口的退货驱动因素
 *    - 增加退货率指标：结合订单表数据，计算7日和30日的订单-退货转化率
 *    - 添加时间间隔指标：计算订单到退货的平均时间间隔，分析用户体验和产品质量
 *    - 扩展为更多时间维度：考虑增加90日等更长周期的汇总指标，支持产品生命周期分析
 *    - 添加对比指标：计算7日到30日的退货增长率，反映退货趋势的加速或减缓情况
 */
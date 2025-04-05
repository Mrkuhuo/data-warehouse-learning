/*
 * 表名: dws_trade_user_sku_order_refund_1d
 * 说明: 交易域用户商品粒度退单最近1日汇总事实表
 * 数据粒度: 用户 + SKU + 日期
 * 存储策略: 分区存储，按日期分区
 * 刷新周期: 每日刷新
 * 应用场景:
 *   - 用户退货行为分析：识别用户对不同商品的退货模式和偏好
 *   - 商品质量问题监控：通过退单数据发现可能存在质量问题的商品
 *   - 品类退货率评估：分析不同品类和品牌的退货情况，评估产品表现
 *   - 退货原因深度研究：结合退货原因数据，分析商品退货的主要驱动因素
 *   - 用户满意度评估：通过退货行为间接评估用户对商品的满意程度
 */
-- DROP TABLE IF EXISTS dws.dws_trade_user_sku_order_refund_1d;
CREATE TABLE dws.dws_trade_user_sku_order_refund_1d
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
    
    /* 度量值字段 - 退单统计 */
    `order_refund_count_1d`      BIGINT COMMENT '最近1日退单次数 - 用户对该商品的退单次数',
    `order_refund_num_1d`        BIGINT COMMENT '最近1日退单件数 - 用户退回该商品的总数量',
    `order_refund_amount_1d`     DECIMAL(16, 2) COMMENT '最近1日退单金额 - 用户退回该商品的总金额'
)
ENGINE=OLAP
UNIQUE KEY(`user_id`,`sku_id`,`k1`)
COMMENT '交易域用户商品粒度退单最近1日汇总事实表'
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
 *    - 主键：使用user_id + sku_id + k1复合主键，支持用户对特定商品在不同日期的退货行为分析
 *    - 分桶：按用户ID分桶(32桶)，优化用户级查询性能，适合用户维度的分析场景
 *    - 分区：使用日期（k1）分区，支持历史数据生命周期管理，提升时间范围查询效率
 *
 * 2. 数据组织：
 *    - 多级品类维度：完整保留三级品类信息，支持不同粒度的品类退货分析
 *    - 品牌维度：包含品牌信息，便于品牌退货分析和问题商品识别
 *    - 退单指标：记录退单次数和退单商品件数，反映退货规模
 *    - 金额指标：包含退款金额，评估退货对收入的影响
 * 
 * 3. 存储管理：
 *    - 动态分区：自动管理历史数据，保留最近60天数据，预创建3天分区
 *    - 存储优化：使用zstd压缩算法和V2存储格式，平衡查询性能和存储效率
 *    - 冗余设计：通过冗余存储维度属性，避免频繁关联维度表，提升查询效率
 *
 * 4. 典型应用场景：
 *    - 商品退货率监控：识别退货率异常的商品，及时发现质量问题
 *    - 品类退货分析：评估不同品类的退货情况，优化品类结构
 *    - 用户退货行为研究：分析用户退货习惯，识别高退货风险用户
 *    - 季节性退货趋势：观察退货数据的季节性波动，为库存管理提供参考
 *    - 与订单表联合分析：结合订单表数据，计算商品的净销售量和真实留存率
 *
 * 5. 优化建议：
 *    - 扩展为N日汇总表：增加7日、30日汇总指标，支持中长期退货分析
 *    - 添加退货原因维度：增加退货原因分析，深入了解退货驱动因素
 *    - 增加退货转化率指标：计算商品的订单-退货转化率，评估商品实际表现
 *    - 添加时间间隔指标：计算订单到退货的平均时间间隔，分析用户体验
 *    - 与订单表关联设计：考虑与订单表协同设计，便于联合分析订单与退单关系
 */
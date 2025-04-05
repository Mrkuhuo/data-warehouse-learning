/*
 * 表名: dws_trade_province_sku_order_1d
 * 说明: 交易域省份商品粒度订单最近1日汇总事实表
 * 数据粒度: 省份 + 商品 + 日期
 * 存储策略: 分区存储，按日期分区
 * 刷新周期: 每日刷新
 * 应用场景:
 *   - 区域销售业绩分析：按省份和商品维度分析销售表现
 *   - 区域商品偏好分析：了解不同地区消费者的商品偏好差异
 *   - 商品区域销售热度图：直观展示商品在各地区的销售热度
 *   - 商品区域销售对比分析：比较同一商品在不同地区的销售差异
 */
-- DROP TABLE IF EXISTS dws.dws_trade_province_sku_order_1d;
CREATE TABLE dws.dws_trade_province_sku_order_1d
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
    `order_total_amount_1d`     DECIMAL(16, 2) COMMENT '最近1日下单最终金额 - 优惠后的实际支付总金额'
)
ENGINE=OLAP
UNIQUE KEY(`province_id`, `sku_id`, `k1`)
COMMENT '交易域省份商品粒度订单最近1日汇总事实表'
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
 * 1. 主键设计：
 *    - 使用省份ID + 商品ID + 日期作为联合主键，形成多维分析视角
 *    - 这种设计支持按省份、商品或两者组合的方式进行数据分析
 *    - 同时确保每个省份-商品-日期组合只有一条汇总记录
 *
 * 2. 分区和分桶策略：
 *    - 按日期分区，支持高效的历史数据管理和按时间范围的查询
 *    - 使用省份ID作为分桶键，而非SKU_ID，因为省份数量有限且相对固定
 *    - 16个桶适合省份数量级，提供良好的数据均衡和查询并行性
 *
 * 3. 维度设计考量：
 *    - 包含丰富的冗余维度（省份信息、商品信息、品类和品牌信息）
 *    - 通过冗余维度减少查询时的多表关联需求，提高分析效率
 *    - 品类采用三级层次结构，支持不同粒度的品类分析
 *
 * 4. 数据管理策略：
 *    - 保留90天历史数据，比普通表的60天更长，适合季度级分析
 *    - 使用zstd压缩算法平衡查询性能和存储效率
 *    - 启用动态分区自动管理历史数据，减少维护工作量
 *
 * 5. 典型查询模式：
 *    - 按省份查询商品销售排行：分析区域热销商品
 *    - 按商品查询区域销售分布：了解商品在各地区的接受度
 *    - 品类与区域交叉分析：挖掘区域消费特点
 *    - 区域商品销售趋势：结合日期维度分析变化趋势
 *
 * 6. 与其他表的关系：
 *    - 与省份粒度表关系：本表是省份粒度表的商品维度细分
 *    - 与商品粒度表关系：本表是商品粒度表的地域维度细分
 *    - 与nd汇总表关系：本表提供日粒度数据，nd表提供多日汇总视图
 *
 * 7. 优化建议：
 *    - 对高频查询场景可考虑建立物化视图，如省份-品类维度汇总
 *    - 用户数计算可考虑使用近似去重算法提高性能
 *    - 对于大省份的热销商品，可考虑优化数据分布策略
 */ 
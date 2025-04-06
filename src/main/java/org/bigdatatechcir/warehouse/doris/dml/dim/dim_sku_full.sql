-- ===============================================================================
-- 商品维度表(dim_sku_full)
-- 功能描述：整合商品相关的所有维度信息，构建商品全维度视图
-- 更新策略：全量快照，每日更新
-- 主键策略：商品ID + 日期分区
-- 分区策略：按日期范围分区
-- 应用场景：支持商品分析、销售报表、库存管理等业务场景的维度关联
-- ===============================================================================

-- DROP TABLE IF EXISTS dim.dim_sku_full;
CREATE TABLE dim.dim_sku_full
(
    `id`                   VARCHAR(255) COMMENT 'SKU ID，商品唯一标识',
    `k1`                   DATE NOT NULL COMMENT '分区字段，数据日期',
    `price`                DECIMAL(16, 2) COMMENT '商品价格，单位元',
    `sku_name`             STRING COMMENT '商品名称，展示用',
    `sku_desc`             STRING COMMENT '商品描述，包含商品详细说明',
    `weight`               DECIMAL(16, 2) COMMENT '商品重量，单位克',
    `is_sale`              BOOLEAN COMMENT '是否在售，true表示在售，false表示下架',
    `spu_id`               STRING COMMENT 'SPU编号，商品标准化单元ID',
    `spu_name`             STRING COMMENT 'SPU名称，标准化商品名称',
    `category3_id`         STRING COMMENT '三级分类ID，最细粒度的商品分类',
    `category3_name`       STRING COMMENT '三级分类名称，如"休闲男鞋"',
    `category2_id`         STRING COMMENT '二级分类ID，中间层级商品分类',
    `category2_name`       STRING COMMENT '二级分类名称，如"男鞋"',
    `category1_id`         STRING COMMENT '一级分类ID，顶层商品分类',
    `category1_name`       STRING COMMENT '一级分类名称，如"鞋靴"',
    `tm_id`                STRING COMMENT '品牌ID，品牌唯一标识',
    `tm_name`              STRING COMMENT '品牌名称，如"Nike"、"Adidas"',
    `attr_ids`             ARRAY<int(11)> COMMENT '平台属性ID集合，商品关联的平台公共属性',
    `sale_attr_ids`        ARRAY<int(11)> COMMENT '销售属性ID集合，商品特有的销售属性',
    `create_time`          STRING COMMENT '创建时间，商品首次录入时间'
)
    ENGINE=OLAP
UNIQUE KEY(`id`,`k1`) -- 使用商品ID和日期作为联合主键，确保单一日期内一个商品只有一条记录
COMMENT '商品维度表 - 整合商品全维度特征信息'
PARTITION BY RANGE(`k1`) () -- 按日期范围分区，便于管理历史数据
DISTRIBUTED BY HASH(`id`) -- 按商品ID哈希分布，确保相关查询高效
PROPERTIES
(
    "replication_allocation" = "tag.location.default: 1", -- 默认副本分配策略
    "is_being_synced" = "false", -- 是否正在同步
    "storage_format" = "V2", -- 使用V2存储格式，提升性能
    "light_schema_change" = "true", -- 允许轻量级的schema变更
    "disable_auto_compaction" = "false", -- 不禁用自动压缩
    "enable_single_replica_compaction" = "false", -- 不启用单副本压缩
    "dynamic_partition.enable" = "true", -- 启用动态分区
    "dynamic_partition.time_unit" = "DAY", -- 动态分区时间单位为天
    "dynamic_partition.start" = "-60", -- 保留过去60天的分区
    "dynamic_partition.end" = "3", -- 预创建未来3天的分区
    "dynamic_partition.prefix" = "p", -- 分区前缀
    "dynamic_partition.buckets" = "32", -- 每个分区的分桶数
    "dynamic_partition.create_history_partition" = "true" -- 创建历史分区
);
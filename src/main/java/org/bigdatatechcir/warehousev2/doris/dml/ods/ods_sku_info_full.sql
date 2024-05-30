-- DROP TABLE IF EXISTS ods.ods_sku_info_full;
CREATE  TABLE ods.ods_sku_info_full
(
    `id`              VARCHAR(255) COMMENT 'skuId',
    `k1`              DATE NOT NULL   COMMENT '分区字段',
    `spu_id`          STRING COMMENT 'spuid',
    `price`           DECIMAL(16, 2) COMMENT '价格',
    `sku_name`        STRING COMMENT '商品名称',
    `sku_desc`        STRING COMMENT '商品描述',
    `weight`          DECIMAL(16, 2) COMMENT '重量',
    `tm_id`           STRING COMMENT '品牌id',
    `category3_id`    STRING COMMENT '品类id',
    `sku_default_igm` STRING COMMENT '商品图片地址',
    `is_sale`         STRING COMMENT '是否在售',
    `create_time`     STRING COMMENT '创建时间'
)
    ENGINE=OLAP
UNIQUE KEY(`id`,`k1`)
COMMENT 'SKU商品表'
PARTITION BY RANGE(`k1`) ()
DISTRIBUTED BY HASH(`id`)
PROPERTIES
(
    "replication_allocation" = "tag.location.default: 1",
    "is_being_synced" = "false",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-60",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "32",
    "dynamic_partition.create_history_partition" = "true"
);

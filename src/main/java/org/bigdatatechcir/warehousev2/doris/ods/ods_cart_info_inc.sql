-- DROP TABLE IF EXISTS ods.ods_cart_info_inc;
CREATE  TABLE ods.ods_cart_info_inc
(
    `id`  VARCHAR(255),
    `k1`  DATE NOT NULL   COMMENT '分区字段',
    `user_id`  STRING,
    `sku_id` STRING,
    `cart_price`  DECIMAL(16, 2),
    `sku_num`  BIGINT,
    `img_url`  STRING,
    `sku_name`  STRING,
    `is_checked`  STRING,
    `create_time`  STRING,
    `operate_time`  STRING,
    `is_ordered`  STRING,
    `order_time`  STRING,
    `source_type`  STRING,
    `source_id`  STRING
)
    ENGINE=OLAP
UNIQUE KEY(`id`,`k1`)
COMMENT '购物车增量表'
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

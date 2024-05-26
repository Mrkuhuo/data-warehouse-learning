-- DROP TABLE IF EXISTS ods.ods_order_detail_inc;
CREATE  TABLE ods.ods_order_detail_inc
(
    `id`  VARCHAR(255),
    `k1` DATE NOT NULL   COMMENT '分区字段',
    `order_id`  STRING,
    `sku_id`  STRING,
    `sku_name`  STRING,
    `img_url`  STRING,
    `order_price`  DECIMAL(16, 2),
    `sku_num`  BIGINT,
    `create_time`  STRING,
    `source_type`  STRING,
    `source_id` STRING,
    `split_total_amount`  DECIMAL(16, 2),
    `split_activity_amount`  DECIMAL(16, 2),
    `split_coupon_amount`  DECIMAL(16, 2)

)
ENGINE=OLAP
UNIQUE KEY(`id`,`k1`)
COMMENT '订单明细表'
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

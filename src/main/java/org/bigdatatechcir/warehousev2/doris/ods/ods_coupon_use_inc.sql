-- DROP TABLE IF EXISTS ods.ods_coupon_use_inc;
CREATE  TABLE ods.ods_coupon_use_inc
(
    `id`  VARCHAR(255),
    `k1`  DATE NOT NULL   COMMENT '分区字段',
    `coupon_id`  STRING,
    `user_id`  STRING,
    `order_id`  STRING,
    `coupon_status`  STRING,
    `get_time`  STRING,
    `using_time`  STRING,
    `used_time`  STRING,
    `expire_time`  STRING
)
    ENGINE=OLAP
UNIQUE KEY(`id`,`k1`)
COMMENT '优惠券领用表'
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
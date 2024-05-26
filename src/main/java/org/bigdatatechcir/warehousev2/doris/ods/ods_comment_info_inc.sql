-- DROP TABLE IF EXISTS ods.ods_comment_info_inc;
CREATE  TABLE ods.ods_comment_info_inc
(
    `id`  VARCHAR(255),
    `k1`  DATE NOT NULL   COMMENT '分区字段',
    `user_id`  STRING,
    `nick_name`  STRING,
    `head_img`  STRING,
    `sku_id`  STRING,
    `spu_id`  STRING,
    `order_id`  STRING,
    `appraise`  STRING,
    `comment_txt`  STRING,
    `create_time`  STRING,
    `operate_time`  STRING
)
    ENGINE=OLAP
UNIQUE KEY(`id`,`k1`)
COMMENT '评价表'
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

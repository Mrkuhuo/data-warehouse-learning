-- DROP TABLE IF EXISTS ods.ods_user_info_inc;
CREATE  TABLE ods.ods_user_info_inc
(
    `id`  VARCHAR(255),
    `k1` DATE NOT NULL   COMMENT '分区字段',
    `login_name`  STRING,
    `nick_name`  STRING,
    `passwd`  STRING,
    `name`  STRING,
    `phone_num`  STRING,
    `email`  STRING,
    `head_img`  STRING,
    `user_level`  STRING,
    `birthday`  STRING,
    `gender`  STRING,
    `create_time`  STRING,
    `operate_time`  STRING,
    `status`  STRING
)
    ENGINE=OLAP
UNIQUE KEY(`id`,`k1`)
COMMENT '用户表'
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
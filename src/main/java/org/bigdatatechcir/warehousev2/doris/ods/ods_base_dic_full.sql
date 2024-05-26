-- DROP TABLE IF EXISTS ods.ods_base_dic_full;
CREATE  TABLE ods.ods_base_dic_full
(
    `dic_code`     VARCHAR(255) COMMENT '编号',
    `k1`           DATE NOT NULL   COMMENT '分区字段',
    `dic_name`     STRING COMMENT '编码名称',
    `parent_code`  STRING COMMENT '父编号',
    `create_time`  STRING COMMENT '创建日期',
    `operate_time` STRING COMMENT '修改日期'
)
    ENGINE=OLAP
UNIQUE KEY(`dic_code`,`k1`)
COMMENT '编码字典表'
PARTITION BY RANGE(`k1`) ()
DISTRIBUTED BY HASH(`dic_code`)
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

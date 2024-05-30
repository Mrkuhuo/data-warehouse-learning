-- DROP TABLE IF EXISTS ods.ods_base_region_full;
CREATE  TABLE ods.ods_base_region_full
(
    `id`          VARCHAR(255) COMMENT '编号',
    `region_name` STRING COMMENT '地区名称'
)
    ENGINE=OLAP
UNIQUE KEY(`id`)
COMMENT '地区表'
DISTRIBUTED BY HASH(`id`)
PROPERTIES
(
    "replication_allocation" = "tag.location.default: 1",
    "is_being_synced" = "false",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false"
);
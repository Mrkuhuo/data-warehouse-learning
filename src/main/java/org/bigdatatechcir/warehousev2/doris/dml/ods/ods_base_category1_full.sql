-- DROP TABLE IF EXISTS ods.ods_base_category1_full;
CREATE  TABLE ods.ods_base_category1_full
(
    `id`   varchar(255) COMMENT '编号',
    `name` STRING COMMENT '分类名称'
)
    ENGINE=OLAP
UNIQUE KEY(`id`)
COMMENT '一级品类表'
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

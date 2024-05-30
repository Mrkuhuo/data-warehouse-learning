-- DROP TABLE IF EXISTS ods.ods_base_category2_full;
CREATE  TABLE ods.ods_base_category2_full
(
    `id`           varchar(255) COMMENT '编号',
    `name`         STRING COMMENT '二级分类名称',
    `category1_id` STRING COMMENT '一级分类编号'

)
    ENGINE=OLAP
UNIQUE KEY(`id`)
COMMENT '二级品类表'
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
-- DROP TABLE IF EXISTS ods.ods_base_category3_full;
CREATE  TABLE ods.ods_base_category3_full
(
    `id`           VARCHAR(255) COMMENT '编号',
    `name`         STRING COMMENT '三级分类名称',
    `category2_id` STRING COMMENT '二级分类编号'
)
    ENGINE=OLAP
UNIQUE KEY(`id`)
COMMENT '三级品类表'
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
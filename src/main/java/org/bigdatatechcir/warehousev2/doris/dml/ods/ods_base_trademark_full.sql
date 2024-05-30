-- DROP TABLE IF EXISTS ods.ods_base_trademark_full;
CREATE  TABLE ods.ods_base_trademark_full
(
    `id`       VARCHAR(255) COMMENT '编号',
    `tm_name`  STRING COMMENT '品牌名称',
    `logo_url` STRING COMMENT '品牌logo的图片路径'
)
    ENGINE=OLAP
UNIQUE KEY(`id`)
COMMENT '品牌表'
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
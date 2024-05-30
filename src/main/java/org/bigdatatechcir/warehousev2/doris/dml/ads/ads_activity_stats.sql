-- DROP TABLE IF EXISTS ads.ads_activity_stats;
CREATE  TABLE ads.ads_activity_stats
(
    `dt`          VARCHAR(255) COMMENT '统计日期',
    `activity_id`   STRING COMMENT '活动ID',
    `activity_name` STRING COMMENT '活动名称',
    `start_date`    STRING COMMENT '活动开始日期',
    `reduce_rate`   DECIMAL(16, 2) COMMENT '补贴率'
)
    ENGINE=OLAP
DUPLICATE KEY(`dt`)
COMMENT '活动统计'
DISTRIBUTED BY HASH(`dt`)
PROPERTIES
(
    "replication_allocation" = "tag.location.default: 1",
    "is_being_synced" = "false",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false"
);
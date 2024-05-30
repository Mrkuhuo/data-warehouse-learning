-- DROP TABLE IF EXISTS ads.ads_user_change;
CREATE  TABLE ads.ads_user_change
(
    `dt`               VARCHAR(255) COMMENT '统计日期',
    `user_churn_count` BIGINT COMMENT '流失用户数',
    `user_back_count`  BIGINT COMMENT '回流用户数'
)
    ENGINE=OLAP
DUPLICATE KEY(`dt`)
COMMENT '用户变动统计'
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
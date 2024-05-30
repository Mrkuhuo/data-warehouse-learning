-- DROP TABLE IF EXISTS ods.ods_log_inc;
CREATE  TABLE ods.ods_log_inc
(
    `id`                            VARCHAR(255),
    `k1`                            DATE NOT NULL   COMMENT '分区字段',
    `common_ar`                     STRING,
    `common_ba`                     STRING,
    `common_ch`                     STRING,
    `common_is_new`                 STRING,
    `common_md`                     STRING,
    `common_mid`                    STRING,
    `common_os`                     STRING,
    `common_uid`                    STRING,
    `common_vc`                     STRING,
    `start_entry`                   STRING,
    `start_loading_time`            STRING,
    `start_open_ad_id`              STRING,
    `start_open_ad_ms`              STRING,
    `start_open_ad_skip_ms`         STRING,
    `page_during_time`              BIGINT,
    `page_item`                     STRING,
    `page_item_type`                STRING,
    `page_last_page_id`             STRING,
    `page_page_id`                  STRING,
    `page_source_type`              STRING,
    `actions`                       JSON COMMENT '动作信息',
    `displays`                      JSON COMMENT '曝光信息',
    `err_error_code`                BIGINT,
    `err_msg`                       STRING,
    `ts`                            BIGINT  COMMENT '时间戳'
)
    ENGINE=OLAP
UNIQUE KEY(`id`,`k1`)
COMMENT '页面日志表'
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
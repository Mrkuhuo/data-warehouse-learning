-- DROP TABLE IF EXISTS dwd.dwd_traffic_page_view_inc;
CREATE  TABLE dwd.dwd_traffic_page_view_inc
(
    `id`             VARCHAR(255),
    `k1`             DATE NOT NULL   COMMENT '分区字段',
    `province_id`    STRING COMMENT '省份id',
    `brand`          STRING COMMENT '手机品牌',
    `channel`        STRING COMMENT '渠道',
    `is_new`         STRING COMMENT '是否首次启动',
    `model`          STRING COMMENT '手机型号',
    `mid_id`         STRING COMMENT '设备id',
    `operate_system` STRING COMMENT '操作系统',
    `user_id`        STRING COMMENT '会员id',
    `version_code`   STRING COMMENT 'app版本号',
    `page_item`      STRING COMMENT '目标id ',
    `page_item_type` STRING COMMENT '目标类型',
    `last_page_id`   STRING COMMENT '上页类型',
    `page_id`        STRING COMMENT '页面ID ',
    `source_type`    STRING COMMENT '来源类型',
    `date_id`        STRING COMMENT '日期id',
    `view_time`      STRING COMMENT '跳入时间',
    `session_id`     STRING COMMENT '所属会话id',
    `during_time`    BIGINT COMMENT '持续时间毫秒'
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
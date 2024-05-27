-- DROP TABLE IF EXISTS dws.dws_traffic_session_page_view_1d;
CREATE  TABLE dws.dws_traffic_session_page_view_1d
(
    `session_id`     VARCHAR(255) COMMENT '会话id',
    `mid_id`         VARCHAR(255) comment '设备id',
    `k1`             DATE NOT NULL   COMMENT '分区字段',
    `brand`          string comment '手机品牌',
    `model`          string comment '手机型号',
    `operate_system` string comment '操作系统',
    `version_code`   string comment 'app版本号',
    `channel`        string comment '渠道',
    `during_time_1d` BIGINT COMMENT '最近1日访问时长',
    `page_count_1d`  BIGINT COMMENT '最近1日访问页面数'
)
    ENGINE=OLAP
UNIQUE KEY(`session_id`,`mid_id`,`k1`)
COMMENT '流量域会话粒度页面浏览最近1日汇总表'
PARTITION BY RANGE(`k1`) ()
DISTRIBUTED BY HASH(`session_id`)
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
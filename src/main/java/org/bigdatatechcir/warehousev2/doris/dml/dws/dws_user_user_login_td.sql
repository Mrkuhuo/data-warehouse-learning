-- DROP TABLE IF EXISTS dws.dws_user_user_login_td;
CREATE  TABLE dws.dws_user_user_login_td
(
    `user_id`         VARCHAR(255) COMMENT '用户id',
    `k1`             DATE NOT NULL   COMMENT '分区字段',
    `login_date_last` STRING COMMENT '末次登录日期',
    `login_count_td`  BIGINT COMMENT '累计登录次数'
)
    ENGINE=OLAP
UNIQUE KEY(`user_id`,`k1`)
COMMENT '用户域用户粒度登录历史至今汇总事实表'
PARTITION BY RANGE(`k1`) ()
DISTRIBUTED BY HASH(`user_id`)
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
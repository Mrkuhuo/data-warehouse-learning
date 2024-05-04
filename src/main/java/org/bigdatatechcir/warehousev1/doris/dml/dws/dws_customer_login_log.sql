CREATE TABLE dws.`dws_customer_login_log` (
  `customer_id` bigint(20) NULL COMMENT '登陆用户ID',
  `login_day` varchar(200) NULL COMMENT '用户登陆时间',
  `login_count` bigint(20) NULL COMMENT '登陆次数'
) ENGINE=OLAP
UNIQUE KEY(`customer_id`)
COMMENT '用户登陆日志表'
DISTRIBUTED BY HASH(`customer_id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"is_being_synced" = "false",
"storage_format" = "V2",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false"
);
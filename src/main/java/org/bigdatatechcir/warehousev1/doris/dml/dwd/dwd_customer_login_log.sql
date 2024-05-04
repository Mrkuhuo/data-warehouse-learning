-- dwd.dwd_customer_login_log definition

CREATE TABLE `dwd_customer_login_log` (
  `login_id` bigint(20) NOT NULL COMMENT '登陆日志ID',
  `customer_id` bigint(20) NULL COMMENT '登陆用户ID',
  `login_time` varchar(200) NULL COMMENT '用户登陆时间',
  `login_ip` varchar(200) NULL COMMENT '登陆IP',
  `login_type` tinyint(4) NULL COMMENT '登陆类型：0未成功，1成功'
) ENGINE=OLAP
UNIQUE KEY(`login_id`)
COMMENT '用户登陆日志表'
DISTRIBUTED BY HASH(`login_id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"is_being_synced" = "false",
"storage_format" = "V2",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false"
);
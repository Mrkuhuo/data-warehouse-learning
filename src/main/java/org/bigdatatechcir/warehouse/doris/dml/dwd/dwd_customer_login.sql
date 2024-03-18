-- dwd.dwd_customer_login definition

CREATE TABLE `dwd_customer_login` (
  `customer_id` bigint(20) NOT NULL COMMENT '用户ID',
  `login_name` varchar(128) NULL COMMENT '用户登录名',
  `password` varchar(128) NULL COMMENT '密码',
  `user_stats` tinyint(4) NULL COMMENT '用户状态',
  `modified_time` varchar(128) NULL COMMENT '最后修改时间'
) ENGINE=OLAP
UNIQUE KEY(`customer_id`)
COMMENT '用户登录表'
DISTRIBUTED BY HASH(`customer_id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"is_being_synced" = "false",
"storage_format" = "V2",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false"
);
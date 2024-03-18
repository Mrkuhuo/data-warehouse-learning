-- dws.dws_customer_addr definition

CREATE TABLE `dws_customer_addr` (
  `customer_id` bigint(20) NULL COMMENT 'customer_login表的自增ID',
  `modified_day` varchar(200) NULL COMMENT '数据产生日期',
  `address_count` bigint(20) NOT NULL COMMENT '地址总数'
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
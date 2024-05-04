CREATE TABLE dwd.`dwd_customer_addr` (
  `customer_addr_id` bigint(20) NULL COMMENT '自增主键ID',
  `customer_id` bigint(20) NULL COMMENT 'customer_login表的自增ID',
  `zip` bigint(20) NOT NULL COMMENT '邮编',
  `province` varchar(200) NOT NULL COMMENT '地区表中省份的ID',
  `city` varchar(200) NOT NULL COMMENT '地区表中城市的ID',
  `district` varchar(200) NOT NULL COMMENT '地区表中的区ID',
  `address` varchar(200) NOT NULL COMMENT '具体的地址门牌号',
  `is_default` tinyint(4) NOT NULL COMMENT '是否默认',
  `modified_time` varchar(128) NULL COMMENT '最后修改时间'
) ENGINE=OLAP
UNIQUE KEY(`customer_addr_id`)
COMMENT '用户登录表'
DISTRIBUTED BY HASH(`customer_addr_id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"is_being_synced" = "false",
"storage_format" = "V2",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false"
);
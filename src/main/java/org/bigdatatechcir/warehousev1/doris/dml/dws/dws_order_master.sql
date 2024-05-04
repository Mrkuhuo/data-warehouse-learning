-- dws.dws_order_master definition

CREATE TABLE `dws_order_master` (
  `customer_id` bigint(20) NULL COMMENT '用户ID',
  `modified_day` varchar(200) NULL COMMENT '最后修改时间',
  `order_sum` bigint(20) NULL COMMENT '订单金额'
) ENGINE=OLAP
UNIQUE KEY(`customer_id`)
COMMENT '订单主表'
DISTRIBUTED BY HASH(`customer_id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"is_being_synced" = "false",
"storage_format" = "V2",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false"
);
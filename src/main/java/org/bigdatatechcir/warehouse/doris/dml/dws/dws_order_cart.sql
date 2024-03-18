-- dws.dws_order_cart definition

CREATE TABLE `dws_order_cart` (
  `customer_id` bigint(20) NULL COMMENT '用户ID',
  `modified_day` varchar(200) NULL COMMENT '最后修改时间',
  `cart_count` bigint(20) NULL COMMENT '加入购物车商品数量'
) ENGINE=OLAP
UNIQUE KEY(`customer_id`)
COMMENT '购物车表'
DISTRIBUTED BY HASH(`customer_id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"is_being_synced" = "false",
"storage_format" = "V2",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false"
);
-- dwd.dwd_warehouse_shipping_info definition

CREATE TABLE `dwd_warehouse_shipping_info` (
  `ship_id` tinyint(4) NULL COMMENT '主键ID',
  `ship_name` varchar(200) NULL COMMENT '物流公司名称',
  `ship_contact` varchar(200) NULL COMMENT '物流公司联系人',
  `telephone` varchar(200) NULL COMMENT '物流公司联系电话',
  `price` DECIMAL(27, 2) NULL COMMENT '配送价格',
  `modified_time` varchar(200) NULL COMMENT '最后修改时间'
) ENGINE=OLAP
UNIQUE KEY(`ship_id`)
COMMENT '物流公司信息表'
DISTRIBUTED BY HASH(`ship_id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"is_being_synced" = "false",
"storage_format" = "V2",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false"
);
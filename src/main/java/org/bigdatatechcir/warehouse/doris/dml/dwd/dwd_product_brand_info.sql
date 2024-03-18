-- dwd.dwd_product_brand_info definition

CREATE TABLE `dwd_product_brand_info` (
  `brand_id` bigint(20) NULL COMMENT '品牌ID',
  `brand_name` varchar(200) NULL COMMENT '品牌名称',
  `telephone` varchar(200) NULL COMMENT '联系电话',
  `brand_web` varchar(200) NULL COMMENT '品牌网络',
  `brand_logo` varchar(200) NULL COMMENT '品牌logo URL',
  `brand_desc` text NULL COMMENT '品牌描述',
  `brand_status` tinyint(4) NULL COMMENT '品牌状态,0禁用,1启用',
  `brand_order` tinyint(4) NULL COMMENT '排序',
  `modified_time` varchar(200) NULL COMMENT '最后修改时间'
) ENGINE=OLAP
UNIQUE KEY(`brand_id`)
COMMENT '品牌信息表'
DISTRIBUTED BY HASH(`brand_id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"is_being_synced" = "false",
"storage_format" = "V2",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false"
);
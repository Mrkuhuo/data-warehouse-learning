CREATE TABLE `ods_product_info` (
  `product_id` bigint(20)  COMMENT '商品ID',
  `product_core` varchar(200)  COMMENT '商品编码',
  `product_name` varchar(200)  COMMENT '商品名称',
  `bar_code` varchar(200) COMMENT '国条码',
  `brand_id` bigint(20)  COMMENT '品牌表的ID',
  `one_category_id` smallint(4)  COMMENT '一级分类ID',
  `two_category_id` smallint(4)  COMMENT '二级分类ID',
  `three_category_id` smallint(4)  COMMENT '三级分类ID',
  `supplier_id` bigint(20)  COMMENT '商品的供应商ID',
  `price` decimal(27,2)  COMMENT '商品销售价格',
  `average_cost` decimal(27,2)  COMMENT '商品加权平均成本',
  `publish_status` smallint(4)  COMMENT '上下架状态：0下架1上架',
  `audit_status` smallint(4)  COMMENT '审核状态：0未审核，1已审核',
  `weight` decimal(27,2)  COMMENT '商品重量',
  `length` decimal(27,2)  COMMENT '商品长度',
  `height` decimal(27,2)  COMMENT '商品高度',
  `width` decimal(27,2)  COMMENT '商品宽度',
  `color_type` varchar(200) ,
  `production_date` varchar(200)  COMMENT '生产日期',
  `shelf_life` bigint(20)  COMMENT '商品有效期',
  `descript` text  COMMENT '商品描述',
  `indate` varchar(200)  COMMENT '商品录入时间',
  `modified_time` varchar(200)  COMMENT '最后修改时间',
) ENGINE=OLAP
UNIQUE KEY(`product_id`)
COMMENT '商品信息表'
DISTRIBUTED BY HASH(`product_id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"is_being_synced" = "false",
"storage_format" = "V2",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false"
);
-- dwd.dwd_product_info definition

CREATE TABLE `dwd_product_info` (
  `product_id` bigint(20) NULL COMMENT '商品ID',
  `product_core` varchar(200) NULL COMMENT '商品编码',
  `product_name` varchar(200) NULL COMMENT '商品名称',
  `bar_code` varchar(200) NULL COMMENT '国条码',
  `brand_id` bigint(20) NULL COMMENT '品牌表的ID',
  `one_category_id` smallint(6) NULL COMMENT '一级分类ID',
  `two_category_id` smallint(6) NULL COMMENT '二级分类ID',
  `three_category_id` smallint(6) NULL COMMENT '三级分类ID',
  `supplier_id` bigint(20) NULL COMMENT '商品的供应商ID',
  `price` DECIMAL(27, 2) NULL COMMENT '商品销售价格',
  `average_cost` DECIMAL(27, 2) NULL COMMENT '商品加权平均成本',
  `publish_status` smallint(6) NULL COMMENT '上下架状态：0下架1上架',
  `audit_status` smallint(6) NULL COMMENT '审核状态：0未审核，1已审核',
  `weight` DECIMAL(27, 2) NULL COMMENT '商品重量',
  `length` DECIMAL(27, 2) NULL COMMENT '商品长度',
  `height` DECIMAL(27, 2) NULL COMMENT '商品高度',
  `width` DECIMAL(27, 2) NULL COMMENT '商品宽度',
  `color_type` varchar(200) NULL,
  `production_date` varchar(200) NULL COMMENT '生产日期',
  `shelf_life` bigint(20) NULL COMMENT '商品有效期',
  `descript` text NULL COMMENT '商品描述',
  `indate` varchar(200) NULL COMMENT '商品录入时间',
  `modified_time` varchar(200) NULL COMMENT '最后修改时间'
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
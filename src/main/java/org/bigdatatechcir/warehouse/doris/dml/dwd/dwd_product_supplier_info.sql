-- dwd.dwd_product_supplier_info definition

CREATE TABLE `dwd_product_supplier_info` (
  `supplier_id` bigint(20) NULL COMMENT '供应商ID',
  `supplier_code` varchar(200) NULL COMMENT '供应商编码',
  `supplier_name` varchar(200) NULL COMMENT '供应商名称',
  `supplier_type` tinyint(4) NULL COMMENT '供应商类型：1.自营，2.平台',
  `link_man` varchar(200) NULL COMMENT '供应商联系人',
  `phone_number` varchar(200) NULL COMMENT '联系电话',
  `bank_name` varchar(200) NULL COMMENT '供应商开户银行名称',
  `bank_account` varchar(200) NULL COMMENT '银行账号',
  `address` varchar(200) NULL COMMENT '供应商地址',
  `supplier_status` tinyint(4) NULL COMMENT '状态：0禁止，1启用',
  `modified_time` varchar(200) NULL COMMENT '最后修改时间'
) ENGINE=OLAP
UNIQUE KEY(`supplier_id`)
COMMENT '供应商信息表'
DISTRIBUTED BY HASH(`supplier_id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"is_being_synced" = "false",
"storage_format" = "V2",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false"
);
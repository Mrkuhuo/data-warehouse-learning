CREATE TABLE `ods_order_master` (
  `order_id` bigint(20)  COMMENT '订单ID',
  `order_sn` bigint(20)  COMMENT '订单编号 yyyymmddnnnnnnnn',
  `customer_id` bigint(20)  COMMENT '下单人ID',
  `shipping_user` varchar(200)  COMMENT '收货人姓名',
  `province` smallint(4)  COMMENT '省',
  `city` smallint(4)  COMMENT '市',
  `district` smallint(4)  COMMENT '区',
  `address` varchar(200)  COMMENT '地址',
  `payment_method` tinyint(4)  COMMENT '支付方式：1现金，2余额，3网银，4支付宝，5微信',
  `order_money` decimal(27,2) COMMENT '订单金额',
  `district_money` decimal(27,2)  COMMENT '优惠金额',
  `shipping_money` decimal(27,2)  COMMENT '运费金额',
  `payment_money` decimal(27,2)  COMMENT '支付金额',
  `shipping_comp_name` varchar(200)  COMMENT '快递公司名称',
  `shipping_sn` varchar(200)  COMMENT '快递单号',
  `create_time` varchar(200)  COMMENT '下单时间',
  `shipping_time` varchar(200)  COMMENT '发货时间',
  `pay_time` varchar(200)  COMMENT '支付时间',
  `receive_time` varchar(200)  COMMENT '收货时间',
  `order_status` tinyint(4)  COMMENT '订单状态',
  `order_point` bigint(20)  COMMENT '订单积分',
  `invoice_time` varchar(200)  COMMENT '发票抬头',
  `modified_time` varchar(200)  COMMENT '最后修改时间',
) ENGINE=OLAP
UNIQUE KEY(`order_id`)
COMMENT '订单主表'
DISTRIBUTED BY HASH(`order_id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"is_being_synced" = "false",
"storage_format" = "V2",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false"
);

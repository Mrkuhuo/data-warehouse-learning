-- dwd.dwd_customer_inf definition

CREATE TABLE `dwd_customer_inf` (
  `customer_inf_id` bigint(20) NULL COMMENT '自增主键ID',
  `customer_id` bigint(20) NULL COMMENT 'customer_login表的自增ID',
  `customer_name` varchar(200) NOT NULL COMMENT '用户真实姓名',
  `identity_card_type` tinyint(4) NOT NULL DEFAULT "1" COMMENT '证件类型：1 身份证，2 军官证，3 护照',
  `identity_card_no` varchar(200) NULL COMMENT '证件号码',
  `mobile_phone` bigint(20) NULL COMMENT '手机号',
  `customer_email` varchar(200) NULL COMMENT '邮箱',
  `gender` varchar(20) NULL COMMENT '性别',
  `user_point` int(11) NOT NULL DEFAULT "0" COMMENT '用户积分',
  `register_time` varchar(200) NOT NULL COMMENT '注册时间',
  `birthday` varchar(200) NULL COMMENT '会员生日',
  `customer_level` tinyint(4) NOT NULL DEFAULT "1" COMMENT '会员级别：1 普通会员，2 青铜，3白银，4黄金，5钻石',
  `user_money` DECIMAL(8, 2) NOT NULL DEFAULT "0.00" COMMENT '用户余额',
  `modified_time` varchar(200) NOT NULL COMMENT '最后修改时间'
) ENGINE=OLAP
UNIQUE KEY(`customer_inf_id`)
COMMENT '用户登录表'
DISTRIBUTED BY HASH(`customer_inf_id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"is_being_synced" = "false",
"storage_format" = "V2",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false"
);
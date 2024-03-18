--  ads.ads_customer definition

CREATE TABLE ads_customer (
  customer_id bigint(20) NULL COMMENT '自增主键ID',
  customer_name varchar(200) NULL COMMENT '用户姓名',
  address_count bigint(20)  NULL COMMENT '地址数量',
  login_count bigint(20)  NULL COMMENT '登录次数',
  cart_count bigint(20)  NULL COMMENT '购物车产品数量',
  order_sum bigint(20)  NULL COMMENT '订单金额',
  modified_day varchar(200)  NULL COMMENT '时间'
) ENGINE=OLAP
UNIQUE KEY(customer_id)
COMMENT '用户维度统计表'
DISTRIBUTED BY HASH(customer_id) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"is_being_synced" = "false",
"storage_format" = "V2",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false"
);
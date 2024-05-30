-- DROP TABLE IF EXISTS ods.ods_order_info_inc;
CREATE  TABLE ods.ods_order_info_inc
(
    `id`  VARCHAR(255),
    `k1` DATE NOT NULL   COMMENT '分区字段',
    `consignee`  STRING,
    `consignee_tel`  STRING,
    `total_amount`  DECIMAL(16, 2),
    `order_status`  STRING,
    `user_id`  STRING,
    `payment_way`  STRING,
    `delivery_address`  STRING,
    `order_comment`  STRING,
    `out_trade_no`  STRING,
    `trade_body`  STRING,
    `create_time`  STRING,
    `operate_time`  STRING,
    `expire_time`  STRING,
    `process_status`  STRING,
    `tracking_no`  STRING,
    `parent_order_id`  STRING,
    `img_url`  STRING,
    `province_id`  STRING,
    `activity_reduce_amount`  DECIMAL(16, 2),
    `coupon_reduce_amount`  DECIMAL(16, 2),
    `original_total_amount`  DECIMAL(16, 2),
    `freight_fee`  DECIMAL(16, 2),
    `freight_fee_reduce`  DECIMAL(16, 2),
    `refundable_time`  DECIMAL(16, 2)
)
    ENGINE=OLAP
UNIQUE KEY(`id`,`k1`)
COMMENT '订单表'
PARTITION BY RANGE(`k1`) ()
DISTRIBUTED BY HASH(`id`)
PROPERTIES
(
    "replication_allocation" = "tag.location.default: 1",
    "is_being_synced" = "false",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-60",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "32",
    "dynamic_partition.create_history_partition" = "true"
);

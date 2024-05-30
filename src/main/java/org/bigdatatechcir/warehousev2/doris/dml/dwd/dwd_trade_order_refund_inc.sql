-- DROP TABLE IF EXISTS dwd.dwd_trade_order_refund_inc;
CREATE  TABLE dwd.dwd_trade_order_refund_inc
(
    `id`                      VARCHAR(255) COMMENT '编号',
    `k1`                      DATE NOT NULL   COMMENT '分区字段',
    `user_id`                 STRING COMMENT '用户ID',
    `order_id`                STRING COMMENT '订单ID',
    `sku_id`                  STRING COMMENT '商品ID',
    `province_id`             STRING COMMENT '地区ID',
    `date_id`                 STRING COMMENT '日期ID',
    `create_time`             STRING COMMENT '退单时间',
    `refund_type_code`        STRING COMMENT '退单类型编码',
    `refund_type_name`        STRING COMMENT '退单类型名称',
    `refund_reason_type_code` STRING COMMENT '退单原因类型编码',
    `refund_reason_type_name` STRING COMMENT '退单原因类型名称',
    `refund_reason_txt`       STRING COMMENT '退单原因描述',
    `refund_num`              BIGINT COMMENT '退单件数',
    `refund_amount`           DECIMAL(16, 2) COMMENT '退单金额'
)
    ENGINE=OLAP
UNIQUE KEY(`id`,`k1`)
COMMENT '交易域退单事务事实表'
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
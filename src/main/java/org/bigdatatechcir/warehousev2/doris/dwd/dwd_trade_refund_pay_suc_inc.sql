-- DROP TABLE IF EXISTS dwd.dwd_trade_refund_pay_suc_inc;
CREATE  TABLE dwd.dwd_trade_refund_pay_suc_inc
(
    `id`                VARCHAR(255) COMMENT '编号',
    `k1`                DATE NOT NULL   COMMENT '分区字段',
    `user_id`           STRING COMMENT '用户ID',
    `order_id`          STRING COMMENT '订单编号',
    `sku_id`            STRING COMMENT 'SKU编号',
    `province_id`       STRING COMMENT '地区ID',
    `payment_type_code` STRING COMMENT '支付类型编码',
    `payment_type_name` STRING COMMENT '支付类型名称',
    `date_id`           STRING COMMENT '日期ID',
    `callback_time`     STRING COMMENT '支付成功时间',
    `refund_num`        DECIMAL(16, 2) COMMENT '退款件数',
    `refund_amount`     DECIMAL(16, 2) COMMENT '退款金额'
)
    ENGINE=OLAP
UNIQUE KEY(`id`,`k1`)
COMMENT '交易域提交退款成功事务事实表'
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
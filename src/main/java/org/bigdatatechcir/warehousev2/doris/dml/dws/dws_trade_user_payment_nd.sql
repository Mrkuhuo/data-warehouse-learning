-- DROP TABLE IF EXISTS dws.dws_trade_user_payment_nd;
CREATE  TABLE dws.dws_trade_user_payment_nd
(
    `user_id`                   VARCHAR(255) COMMENT '用户id',
    `k1`                        DATE NOT NULL   COMMENT '分区字段',
    `payment_count_7d`   BIGINT COMMENT '最近7日支付次数',
    `payment_num_7d`     BIGINT COMMENT '最近7日支付商品件数',
    `payment_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日支付金额',
    `payment_count_30d`  BIGINT COMMENT '最近30日支付次数',
    `payment_num_30d`    BIGINT COMMENT '最近30日支付商品件数',
    `payment_amount_30d` DECIMAL(16, 2) COMMENT '最近30日支付金额'
)
    ENGINE=OLAP
UNIQUE KEY(`user_id`,`k1`)
COMMENT '交易域用户粒度支付最近n日汇总事实表'
PARTITION BY RANGE(`k1`) ()
DISTRIBUTED BY HASH(`user_id`)
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
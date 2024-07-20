-- 工具域优惠券使用(支付)事务事实表
SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

CREATE CATALOG paimon_hive WITH (
    'type' = 'paimon',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);

use CATALOG paimon_hive;

create  DATABASE IF NOT EXISTS dwd;

CREATE TABLE IF NOT EXISTS dwd.dwd_tool_coupon_pay_full(
    `id`           BIGINT COMMENT '编号',
    `k1`           STRING COMMENT '分区字段',
    `coupon_id`    BIGINT COMMENT '优惠券ID',
    `user_id`      BIGINT COMMENT 'user_id',
    `order_id`     BIGINT COMMENT 'order_id',
    `date_id`      STRING COMMENT '日期ID',
    `payment_time` TIMESTAMP(3) COMMENT '使用下单时间',
    PRIMARY KEY (`id`,`k1` ) NOT ENFORCED
    )   PARTITIONED BY (`k1` ) WITH (
    'connector' = 'paimon',
    'metastore.partitioned-table' = 'true',
    'file.format' = 'parquet',
    'write-buffer-size' = '512mb',
    'write-buffer-spillable' = 'true' ,
    'partition.expiration-time' = '1 d',
    'partition.expiration-check-interval' = '1 h',
    'partition.timestamp-formatter' = 'yyyy-MM-dd',
    'partition.timestamp-pattern' = '$k1'
    );


insert into dwd.dwd_tool_coupon_pay_full(
    id,
    k1,
    coupon_id,
    user_id,
    order_id,
    date_id,
    payment_time
)
select
    id,
    k1,
    coupon_id,
    user_id,
    order_id,
    date_format(used_time,'yyyy-MM-dd') date_id,
    used_time
from ods.ods_coupon_use_full
where used_time is not null;
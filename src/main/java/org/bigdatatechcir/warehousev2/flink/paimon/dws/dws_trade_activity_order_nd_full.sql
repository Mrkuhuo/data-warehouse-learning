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

create  DATABASE IF NOT EXISTS dws;

CREATE TABLE IF NOT EXISTS dws.dws_trade_activity_order_nd_full(
    `activity_id`                BIGINT COMMENT '活动id',
    `k1`                         STRING  COMMENT '分区字段',
    `activity_name`              STRING COMMENT '活动名称',
    `activity_type_code`         STRING COMMENT '活动类型编码',
    `activity_type_name`         STRING COMMENT '活动类型名称',
    `start_date`                 STRING COMMENT '发布日期',
    `original_amount_30d`        DECIMAL(16, 2) COMMENT '参与活动订单原始金额',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '参与活动订单优惠金额',
    PRIMARY KEY (`activity_id`,`k1` ) NOT ENFORCED
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


INSERT INTO dws.dws_trade_activity_order_nd_full(activity_id, k1, activity_name, activity_type_code, activity_type_name, start_date, original_amount_30d, activity_reduce_amount_30d)
select
    act.activity_id,
    od.k1,
    activity_name,
    activity_type_code,
    activity_type_name,
    date_format(start_time,'yyyy-MM-dd'),
    sum(split_original_amount),
    sum(split_activity_amount)
from
    (
        select
            activity_id,
            activity_name,
            activity_type_code,
            activity_type_name,
            start_time
        from dim.dim_activity_full
        group by activity_id, activity_name, activity_type_code, activity_type_name,start_time
    )act
        left join
    (
        select
            activity_id,
            k1,
            order_id,
            split_original_amount,
            split_activity_amount
        from dwd.dwd_trade_order_detail_full
        where activity_id is not null
    )od
    on act.activity_id=od.activity_id
group by act.activity_id,od.k1,activity_name,activity_type_code,activity_type_name,start_time;
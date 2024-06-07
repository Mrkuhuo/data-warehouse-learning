SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'execution.runtime-mode' = 'streaming';


CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);

use CATALOG iceberg_catalog;

create  DATABASE IF NOT EXISTS iceberg_dws;

CREATE TABLE IF NOT EXISTS iceberg_dws.dws_trade_activity_order_nd_full(
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
    'catalog-name'='hive_prod',
    'uri'='thrift://192.168.244.129:9083',
    'warehouse'='hdfs://192.168.244.129:9000/user/hive/warehouse/'
    );


INSERT INTO iceberg_dws.dws_trade_activity_order_nd_full /*+ OPTIONS('upsert-enabled'='true') */ (
    activity_id,
    k1,
    activity_name,
    activity_type_code,
    activity_type_name,
    start_date,
    original_amount_30d,
    activity_reduce_amount_30d
    )
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
        from iceberg_dim.dim_activity_full /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/
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
        from iceberg_dwd.dwd_trade_order_detail_full /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/
        where activity_id is not null
    )od
    on act.activity_id=od.activity_id
group by act.activity_id,od.k1,activity_name,activity_type_code,activity_type_name,start_time;
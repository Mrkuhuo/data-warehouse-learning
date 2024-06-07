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

create  DATABASE IF NOT EXISTS iceberg_ads;

CREATE TABLE IF NOT EXISTS iceberg_ads.ads_activity_stats_full(
    `dt`            STRING COMMENT '统计日期',
    `activity_id`   BIGINT COMMENT '活动ID',
    `activity_name` STRING COMMENT '活动名称',
    `start_date`    STRING COMMENT '活动开始日期',
    `reduce_rate`   DECIMAL(16, 2) COMMENT '补贴率'
    );

INSERT INTO iceberg_ads.ads_activity_stats_full(
    dt,
    activity_id,
    activity_name,
    start_date,
    reduce_rate
    )
select
    k1,
    activity_id,
    activity_name,
    start_date,
    cast(activity_reduce_amount_30d/original_amount_30d as decimal(16,2))
from iceberg_dws.dws_trade_activity_order_nd_full /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/;
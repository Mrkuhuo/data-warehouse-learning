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

CREATE TABLE IF NOT EXISTS dws.dws_traffic_page_visitor_page_view_nd_full(
    `mid_id`          string COMMENT '访客id',
    `k1`              string  COMMENT '分区字段',
    `brand`           string comment '手机品牌',
    `model`           string comment '手机型号',
    `operate_system`  string comment '操作系统',
    `page_id`         string COMMENT '页面id',
    `during_time_30d` BIGINT COMMENT '最近30日浏览时长',
    `view_count_30d`  BIGINT COMMENT '最近30日访问次数',
    PRIMARY KEY (`mid_id`,`k1` ) NOT ENFORCED
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

INSERT INTO dws.dws_traffic_page_visitor_page_view_nd_full(
    mid_id,
    k1,
    brand,
    model,
    operate_system,
    page_id,
    during_time_30d,
    view_count_30d
    )
select
    mid_id,
    k1,
    brand,
    model,
    operate_system,
    page_id,
    sum(during_time_1d),
    sum(view_count_1d)
from dws.dws_traffic_page_visitor_page_view_1d_full
group by mid_id,k1,brand,model,operate_system,page_id;
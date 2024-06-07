SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';

CREATE TABLE order_status_log_full_mq (
    `id` bigint NOT NULL,
    `order_id` bigint NULL,
    `order_status` STRING  NULL,
    `operate_time` timestamp(3) NOT NULL  ,
     PRIMARY KEY(`id`) NOT ENFORCED
) WITH (
      'connector' = 'mysql-cdc',
      'scan.startup.mode' = 'earliest-offset',
      'hostname' = '192.168.244.129',
      'port' = '3306',
      'username' = 'root',
      'password' = '',
      'database-name' = 'gmall',
      'table-name' = 'order_status_log',
      'server-time-zone' = 'Asia/Shanghai'
      );

CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);

use CATALOG iceberg_catalog;

create  DATABASE IF NOT EXISTS iceberg_ods;


CREATE TABLE IF NOT EXISTS iceberg_ods.ods_order_status_log_full(
    `id` bigint NOT NULL  COMMENT '购物券编号',
    `k1` STRING COMMENT '分区字段',
    `order_id` bigint  NULL,
    `order_status` STRING  NULL,
    `operate_time` timestamp(3) NOT NULL  ,
    PRIMARY KEY (`id`,`k1` ) NOT ENFORCED
    )   PARTITIONED BY (`k1` ) WITH (
    'catalog-name'='hive_prod',
    'uri'='thrift://192.168.244.129:9083',
    'warehouse'='hdfs://192.168.244.129:9000/user/hive/warehouse/'
   );

INSERT INTO iceberg_ods.ods_order_status_log_full  /*+ OPTIONS('upsert-enabled'='true') */(
    `id`,
    `k1`,
    `order_id`,
    `order_status`,
    `operate_time`
)
select
    id,
    DATE_FORMAT(operate_time, 'yyyy-MM-dd') AS k1,
    `order_id`,
    `order_status`,
    `operate_time`
from default_catalog.default_database.order_status_log_full_mq
where operate_time is not null;
-- 品牌表（全量表）
SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';

CREATE TABLE base_trademark_full_mq (
    `id` bigint NOT NULL  COMMENT '编号',
    `tm_name` STRING NOT NULL COMMENT '属性值',
    `logo_url` STRING NULL COMMENT '品牌logo的图片路径',
    PRIMARY KEY(`id`) NOT ENFORCED
) WITH (
      'connector' = 'mysql-cdc',
      'scan.startup.mode' = 'earliest-offset',
      'hostname' = '192.168.244.129',
      'port' = '3306',
      'username' = 'root',
      'password' = '',
      'database-name' = 'gmall',
      'table-name' = 'base_trademark',
      'server-time-zone' = 'Asia/Shanghai'
      );

CREATE CATALOG paimon_hive WITH (
    'type' = 'paimon',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);

use CATALOG paimon_hive;

create  DATABASE IF NOT EXISTS ods;

CREATE TABLE IF NOT EXISTS ods.ods_base_trademark_full(
    `id` bigint NOT NULL  COMMENT '编号',
    `tm_name` STRING NOT NULL COMMENT '属性值',
    `logo_url` STRING NULL COMMENT '品牌logo的图片路径',
    PRIMARY KEY (`id`) NOT ENFORCED
);

INSERT INTO ods.ods_base_trademark_full(
    `id` ,
    `tm_name` ,
    `logo_url`
)
select
    `id` ,
    `tm_name` ,
    `logo_url`
from default_catalog.default_database.base_trademark_full_mq;

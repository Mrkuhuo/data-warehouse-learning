-- 省份表（全量表）
SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';

CREATE TABLE base_province_full_mq (
    `id` bigint NOT NULL COMMENT 'id',
    `name` STRING  NULL COMMENT '省名称',
    `region_id` STRING NULL COMMENT '大区id',
    `area_code` STRING NULL COMMENT '行政区位码',
    `iso_code` STRING NULL COMMENT '国际编码',
    `iso_3166_2` STRING NULL COMMENT 'ISO3166编码',
    PRIMARY KEY(`id`) NOT ENFORCED
) WITH (
      'connector' = 'mysql-cdc',
      'scan.startup.mode' = 'earliest-offset',
      'hostname' = '192.168.244.129',
      'port' = '3306',
      'username' = 'root',
      'password' = '',
      'database-name' = 'gmall',
      'table-name' = 'base_province',
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

CREATE TABLE IF NOT EXISTS ods.ods_base_province_full(
    `id` bigint NOT NULL COMMENT 'id',
    `name` STRING  NULL COMMENT '省名称',
    `region_id` STRING NULL COMMENT '大区id',
    `area_code` STRING NULL COMMENT '行政区位码',
    `iso_code` STRING NULL COMMENT '国际编码',
    `iso_3166_2` STRING NULL COMMENT 'ISO3166编码',
    PRIMARY KEY (`id`) NOT ENFORCED
);

INSERT INTO ods.ods_base_province_full(
    `id` ,
    `name` ,
    `region_id` ,
    `area_code` ,
    `iso_code` ,
    `iso_3166_2`
)
select
    `id` ,
    `name` ,
    `region_id` ,
    `area_code` ,
    `iso_code` ,
    `iso_3166_2`
from default_catalog.default_database.base_province_full_mq;

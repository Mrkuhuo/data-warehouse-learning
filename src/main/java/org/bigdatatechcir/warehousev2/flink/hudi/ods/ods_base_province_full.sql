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

create catalog hudi_catalog with(
	'type' = 'hudi',
	'mode' = 'hms',
	'hive.conf.dir'='/opt/software/apache-hive-3.1.3-bin/conf'
);

use CATALOG hudi_catalog;

create  DATABASE IF NOT EXISTS hudi_ods;

CREATE TABLE IF NOT EXISTS hudi_ods.ods_base_province_full(
    `id` bigint NOT NULL COMMENT 'id',
    `name` STRING  NULL COMMENT '省名称',
    `region_id` STRING NULL COMMENT '大区id',
    `area_code` STRING NULL COMMENT '行政区位码',
    `iso_code` STRING NULL COMMENT '国际编码',
    `iso_3166_2` STRING NULL COMMENT 'ISO3166编码',
    PRIMARY KEY (`id`) NOT ENFORCED
    ) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
    );

INSERT INTO hudi_ods.ods_base_province_full(
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

SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';

CREATE TABLE activity_info_full_mq (
    `id` bigint NOT NULL COMMENT '活动id',
    `activity_name` STRING NULL COMMENT '活动名称',
    `activity_type` STRING NULL COMMENT '活动类型',
    `activity_desc` STRING NULL COMMENT '活动描述',
    `start_time` TIMESTAMP(3) NULL COMMENT '开始时间',
    `end_time` TIMESTAMP(3) NULL COMMENT '结束时间',
    `create_time` TIMESTAMP(3) NULL  COMMENT '创建时间',
    PRIMARY KEY(`id`) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'scan.startup.mode' = 'earliest-offset',
    'hostname' = '192.168.244.129',
    'port' = '3306',
    'username' = 'root',
    'password' = '',
    'database-name' = 'gmall',
    'table-name' = 'activity_info',
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

CREATE TABLE IF NOT EXISTS iceberg_ods.ods_activity_info_full(
    `id`            BIGINT COMMENT '活动id',
    `k1`            STRING COMMENT '分区字段',
    `activity_name` STRING COMMENT '活动名称',
    `activity_type` STRING COMMENT '活动类型',
    `activity_desc` STRING COMMENT '活动描述',
    `start_time`    STRING COMMENT '开始时间',
    `end_time`      STRING COMMENT '结束时间',
    `create_time`   STRING COMMENT '创建时间',
    PRIMARY KEY (`id`,`k1` ) NOT ENFORCED
)   PARTITIONED BY (`k1` ) WITH (
    'catalog-name'='hive_prod',
    'uri'='thrift://192.168.244.129:9083',
    'warehouse'='hdfs://192.168.244.129:9000/user/hive/warehouse/'
);

INSERT into  iceberg_ods.ods_activity_info_full /*+ OPTIONS('upsert-enabled'='true') */ (`id`, `k1` , `activity_name`, `activity_type`, `activity_desc`, `start_time`, `end_time`, `create_time`)
select
    id,
    DATE_FORMAT(create_time, 'yyyy-MM-dd') AS k1,
    activity_name,
    activity_type,
    activity_desc,
    DATE_FORMAT(start_time, 'yyyy-MM-dd HH:mm:ss') AS start_time,
    DATE_FORMAT(end_time, 'yyyy-MM-dd HH:mm:ss') AS end_time,
    DATE_FORMAT(create_time, 'yyyy-MM-dd HH:mm:ss') AS create_time
from default_catalog.default_database.activity_info_full_mq
where create_time is not null;

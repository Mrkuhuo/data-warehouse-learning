SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';

CREATE TABLE spu_info_full_mq (
    `id` bigint NOT NULL  COMMENT '商品id',
    `spu_name` string  NULL COMMENT '商品名称',
    `description` string  NULL COMMENT '商品描述(后台简述）',
    `category3_id` bigint  NULL COMMENT '三级分类id',
    `tm_id` bigint  NULL COMMENT '品牌id',
    PRIMARY KEY(`id`) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'scan.startup.mode' = 'earliest-offset',
    'hostname' = '192.168.244.129',
    'port' = '3306',
    'username' = 'root',
    'password' = '',
    'database-name' = 'gmall',
    'table-name' = 'spu_info',
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

CREATE TABLE IF NOT EXISTS iceberg_ods.ods_spu_info_full(
    `id` bigint NOT NULL  COMMENT '商品id',
    `spu_name` string  NULL COMMENT '商品名称',
    `description` string  NULL COMMENT '商品描述(后台简述）',
    `category3_id` bigint  NULL COMMENT '三级分类id',
    `tm_id` bigint  NULL COMMENT '品牌id',
    PRIMARY KEY (`id`) NOT ENFORCED
    );

INSERT INTO iceberg_ods.ods_spu_info_full  /*+ OPTIONS('upsert-enabled'='true') */(
    `id`,
    `spu_name`,
    `description`,
    `category3_id`,
    `tm_id`
)
select
    `id`,
    `spu_name`,
    `description`,
    `category3_id`,
    `tm_id`
from default_catalog.default_database.spu_info_full_mq;

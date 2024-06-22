-- 商品销售属性值表（全量表）
SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';

CREATE TABLE sku_sale_attr_value_full_mq (
    `id` bigint NOT NULL  COMMENT 'id',
    `sku_id` bigint  NULL COMMENT '库存单元id',
    `spu_id` int  NULL COMMENT 'spu_id(冗余)',
    `sale_attr_value_id` bigint  NULL COMMENT '销售属性值id',
    `sale_attr_id` bigint  NULL,
    `sale_attr_name` string  NULL,
    `sale_attr_value_name` string  NULL,
    PRIMARY KEY(`id`) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'scan.startup.mode' = 'earliest-offset',
    'hostname' = '192.168.244.129',
    'port' = '3306',
    'username' = 'root',
    'password' = '',
    'database-name' = 'gmall',
    'table-name' = 'sku_sale_attr_value',
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

CREATE TABLE IF NOT EXISTS ods.ods_sku_sale_attr_value_full(
    `id` bigint NOT NULL  COMMENT 'id',
    `sku_id` bigint  NULL COMMENT '库存单元id',
    `spu_id` int  NULL COMMENT 'spu_id(冗余)',
    `sale_attr_value_id` bigint  NULL COMMENT '销售属性值id',
    `sale_attr_id` bigint  NULL,
    `sale_attr_name` string  NULL,
    `sale_attr_value_name` string  NULL,
    PRIMARY KEY (`id`) NOT ENFORCED
    );

INSERT INTO ods.ods_sku_sale_attr_value_full(
    `id`,
    `sku_id`,
    `spu_id`,
    `sale_attr_value_id`,
    `sale_attr_id`,
    `sale_attr_name`,
    `sale_attr_value_name`
)
select
    `id`,
    `sku_id`,
    `spu_id`,
    `sale_attr_value_id`,
    `sale_attr_id`,
    `sale_attr_name`,
    `sale_attr_value_name`
from default_catalog.default_database.sku_sale_attr_value_full_mq;

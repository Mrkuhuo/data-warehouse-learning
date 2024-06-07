SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';

CREATE TABLE favor_info_full_mq (
    `id` bigint NOT NULL  COMMENT '编号',
    `user_id` bigint  NULL COMMENT '用户名称',
    `sku_id` bigint  NULL COMMENT 'skuid',
    `spu_id` bigint  NULL COMMENT '商品id',
    `is_cancel` string  NULL COMMENT '是否已取消 0 正常 1 已取消',
    `create_time` timestamp(3) NOT NULL   COMMENT '创建时间',
    `cancel_time` timestamp(3)  NULL COMMENT '修改时间',
     PRIMARY KEY(`id`) NOT ENFORCED
) WITH (
      'connector' = 'mysql-cdc',
      'scan.startup.mode' = 'earliest-offset',
      'hostname' = '192.168.244.129',
      'port' = '3306',
      'username' = 'root',
      'password' = '',
      'database-name' = 'gmall',
      'table-name' = 'favor_info',
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


CREATE TABLE IF NOT EXISTS iceberg_ods.ods_favor_info_full(
    `id` bigint NOT NULL  COMMENT '购物券编号',
    `k1` STRING COMMENT '分区字段',
    `user_id` bigint  NULL COMMENT '用户名称',
    `sku_id` bigint  NULL COMMENT 'skuid',
    `spu_id` bigint  NULL COMMENT '商品id',
    `is_cancel` string  NULL COMMENT '是否已取消 0 正常 1 已取消',
    `create_time` timestamp(3) NOT NULL   COMMENT '创建时间',
    `cancel_time` timestamp(3)  NULL COMMENT '修改时间',
    PRIMARY KEY (`id`,`k1` ) NOT ENFORCED
    )   PARTITIONED BY (`k1` ) WITH (
    'catalog-name'='hive_prod',
    'uri'='thrift://192.168.244.129:9083',
    'warehouse'='hdfs://192.168.244.129:9000/user/hive/warehouse/'
   );

INSERT INTO iceberg_ods.ods_favor_info_full  /*+ OPTIONS('upsert-enabled'='true') */(
    `id`,
    `k1`,
    `user_id`,
    `sku_id`,
    `spu_id`,
    `is_cancel`,
    `create_time`,
    `cancel_time`
)
select
    id,
    DATE_FORMAT(create_time, 'yyyy-MM-dd') AS k1,
    `user_id`,
    `sku_id`,
    `spu_id`,
    `is_cancel`,
    `create_time`,
    `cancel_time`
from default_catalog.default_database.favor_info_full_mq
where create_time is not null;
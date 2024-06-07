SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';

CREATE TABLE coupon_use_full_mq (
    `id` bigint NOT NULL  COMMENT '编号',
    `coupon_id` bigint  NULL COMMENT '购物券ID',
    `user_id` bigint  NULL COMMENT '用户ID',
    `order_id` bigint  NULL COMMENT '订单ID',
    `coupon_status` STRING NULL COMMENT '购物券状态（1：未使用 2：已使用）',
    `create_time` timestamp(3) NOT NULL  COMMENT '创建时间',
    `get_time` timestamp(3)  NULL COMMENT '获取时间',
    `using_time` timestamp(3)  NULL COMMENT '使用时间',
    `used_time` timestamp(3)  NULL COMMENT '支付时间',
    `expire_time` timestamp(3)  NULL COMMENT '过期时间',
     PRIMARY KEY(`id`) NOT ENFORCED
) WITH (
      'connector' = 'mysql-cdc',
      'scan.startup.mode' = 'earliest-offset',
      'hostname' = '192.168.244.129',
      'port' = '3306',
      'username' = 'root',
      'password' = '',
      'database-name' = 'gmall',
      'table-name' = 'coupon_use',
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


CREATE TABLE IF NOT EXISTS iceberg_ods.ods_coupon_use_full(
    `id` bigint NOT NULL  COMMENT '购物券编号',
    `k1` STRING COMMENT '分区字段',
    `coupon_id` bigint  NULL COMMENT '购物券ID',
    `user_id` bigint  NULL COMMENT '用户ID',
    `order_id` bigint  NULL COMMENT '订单ID',
    `coupon_status` STRING NULL COMMENT '购物券状态（1：未使用 2：已使用）',
    `create_time` timestamp(3) NOT NULL  COMMENT '创建时间',
    `get_time` timestamp(3)  NULL COMMENT '获取时间',
    `using_time` timestamp(3)  NULL COMMENT '使用时间',
    `used_time` timestamp(3)  NULL COMMENT '支付时间',
    `expire_time` timestamp(3)  NULL COMMENT '过期时间',
    PRIMARY KEY (`id`,`k1` ) NOT ENFORCED
    )   PARTITIONED BY (`k1` ) WITH (
    'catalog-name'='hive_prod',
    'uri'='thrift://192.168.244.129:9083',
    'warehouse'='hdfs://192.168.244.129:9000/user/hive/warehouse/'
   );

INSERT INTO iceberg_ods.ods_coupon_use_full  /*+ OPTIONS('upsert-enabled'='true') */(
    `id`,
    `k1`,
    `coupon_id`,
    `user_id`,
    `order_id`,
    `coupon_status`,
    `create_time`,
    `get_time`,
    `using_time`,
    `used_time`,
    `expire_time`
)
select
    id,
    DATE_FORMAT(create_time, 'yyyy-MM-dd') AS k1,
    `coupon_id`,
    `user_id`,
    `order_id`,
    `coupon_status`,
    `create_time`,
    `get_time`,
    `using_time`,
    `used_time`,
    `expire_time`
from default_catalog.default_database.coupon_use_full_mq
where create_time is not null;
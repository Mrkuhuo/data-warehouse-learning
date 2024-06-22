-- 订单明细表（增量表）
SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';

CREATE TABLE order_detail_full_mq (
    `id` bigint NOT NULL  COMMENT '编号',
    `order_id` bigint  NULL COMMENT '订单编号',
    `sku_id` bigint  NULL COMMENT 'sku_id',
    `sku_name` STRING  NULL COMMENT 'sku名称（冗余)',
    `img_url` STRING  NULL COMMENT '图片名称（冗余)',
    `order_price` decimal(10,2)  NULL COMMENT '购买价格(下单时sku价格）',
    `sku_num` bigint  NULL COMMENT '购买个数',
    `create_time` TIMESTAMP(3) NOT NULL   COMMENT '创建时间',
    `source_type` STRING  NULL COMMENT '来源类型',
    `source_id` bigint  NULL COMMENT '来源编号',
    `split_total_amount` decimal(16,2)  NULL,
    `split_activity_amount` decimal(16,2)  NULL,
    `split_coupon_amount` decimal(16,2)  NULL,
     PRIMARY KEY(`id`) NOT ENFORCED
) WITH (
      'connector' = 'mysql-cdc',
      'scan.startup.mode' = 'earliest-offset',
      'hostname' = '192.168.244.129',
      'port' = '3306',
      'username' = 'root',
      'password' = '',
      'database-name' = 'gmall',
      'table-name' = 'order_detail',
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

CREATE TABLE IF NOT EXISTS ods.ods_order_detail_full(
    `id` bigint NOT NULL  COMMENT '购物券编号',
    `k1` STRING COMMENT '分区字段',
    `order_id` bigint  NULL COMMENT '订单编号',
    `sku_id` bigint  NULL COMMENT 'sku_id',
    `sku_name` STRING  NULL COMMENT 'sku名称（冗余)',
    `img_url` STRING  NULL COMMENT '图片名称（冗余)',
    `order_price` decimal(10,2)  NULL COMMENT '购买价格(下单时sku价格）',
    `sku_num` bigint  NULL COMMENT '购买个数',
    `create_time` TIMESTAMP(3) NOT NULL   COMMENT '创建时间',
    `source_type` STRING  NULL COMMENT '来源类型',
    `source_id` bigint  NULL COMMENT '来源编号',
    `split_total_amount` decimal(16,2)  NULL,
    `split_activity_amount` decimal(16,2)  NULL,
    `split_coupon_amount` decimal(16,2)  NULL,
    PRIMARY KEY (`id`,`k1` ) NOT ENFORCED
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

INSERT INTO ods.ods_order_detail_full(
    `id`,
    `k1`,
    `order_id`,
    `sku_id`,
    `sku_name`,
    `img_url`,
    `order_price`,
    `sku_num`,
    `create_time`,
    `source_type`,
    `source_id`,
    `split_total_amount`,
    `split_activity_amount`,
    `split_coupon_amount`
)
select
    id,
    DATE_FORMAT(create_time, 'yyyy-MM-dd') AS k1,
    `order_id`,
    `sku_id`,
    `sku_name`,
    `img_url`,
    `order_price`,
    `sku_num`,
    `create_time`,
    `source_type`,
    `source_id`,
    `split_total_amount`,
    `split_activity_amount`,
    `split_coupon_amount`
from default_catalog.default_database.order_detail_full_mq
where create_time is not null;
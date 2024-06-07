SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';

CREATE TABLE cart_info_full_mq (
    `id` bigint NOT NULL  COMMENT '编号',
    `user_id` STRING  NULL COMMENT '用户id',
    `sku_id` bigint  NULL COMMENT 'skuid',
    `cart_price` decimal(10,2)  NULL COMMENT '放入购物车时价格',
    `sku_num` int  NULL COMMENT '数量',
    `img_url` STRING  NULL COMMENT '图片文件',
    `sku_name` STRING  NULL COMMENT 'sku名称 (冗余)',
    `is_checked` int  NULL,
    `create_time` TIMESTAMP(3)  NULL  COMMENT '创建时间',
    `operate_time` TIMESTAMP(3)  NULL  COMMENT '修改时间',
    `is_ordered` bigint  NULL COMMENT '是否已经下单',
    `order_time` TIMESTAMP(3)  NULL COMMENT '下单时间',
    `source_type` STRING  NULL COMMENT '来源类型',
    `source_id` bigint  NULL COMMENT '来源编号',
    PRIMARY KEY(`id`) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'scan.startup.mode' = 'earliest-offset',
    'hostname' = '192.168.244.129',
    'port' = '3306',
    'username' = 'root',
    'password' = '',
    'database-name' = 'gmall',
    'table-name' = 'cart_info',
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

CREATE TABLE IF NOT EXISTS iceberg_ods.ods_cart_info_full(
    `id` bigint NOT NULL  COMMENT '编号',
    `k1` STRING COMMENT '分区字段',
    `user_id` STRING  NULL COMMENT '用户id',
    `sku_id` bigint  NULL COMMENT 'skuid',
    `cart_price` decimal(10,2)  NULL COMMENT '放入购物车时价格',
    `sku_num` int  NULL COMMENT '数量',
    `img_url` STRING  NULL COMMENT '图片文件',
    `sku_name` STRING  NULL COMMENT 'sku名称 (冗余)',
    `is_checked` int  NULL,
    `create_time` TIMESTAMP(3)  NULL  COMMENT '创建时间',
    `operate_time` TIMESTAMP(3)  NULL  COMMENT '修改时间',
    `is_ordered` bigint  NULL COMMENT '是否已经下单',
    `order_time` TIMESTAMP(3)  NULL COMMENT '下单时间',
    `source_type` STRING  NULL COMMENT '来源类型',
    `source_id` bigint  NULL COMMENT '来源编号',
    PRIMARY KEY (`id`,`k1` ) NOT ENFORCED
)   PARTITIONED BY (`k1` ) WITH (
    'catalog-name'='hive_prod',
    'uri'='thrift://192.168.244.129:9083',
    'warehouse'='hdfs://192.168.244.129:9000/user/hive/warehouse/'
);

INSERT INTO iceberg_ods.ods_cart_info_full  /*+ OPTIONS('upsert-enabled'='true') */(
    `id`,
    `k1`,
    `user_id`,
    `sku_id`,
    `cart_price`,
    `sku_num`,
    `img_url`,
    `sku_name`,
    `is_checked`,
    `create_time`,
    `operate_time`,
    `is_ordered`,
    `order_time`,
    `source_type`,
    `source_id`
)
select
    `id`,
    DATE_FORMAT(create_time, 'yyyy-MM-dd') AS k1,
    `user_id`,
    `sku_id`,
    `cart_price`,
    `sku_num`,
    `img_url`,
    `sku_name`,
    `is_checked`,
    `create_time`,
    `operate_time`,
    `is_ordered`,
    `order_time`,
    `source_type`,
    `source_id`
from default_catalog.default_database.cart_info_full_mq
where create_time is not null;

SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';

CREATE TABLE comment_info_full_mq (
    `id` bigint NOT NULL  COMMENT '编号',
    `user_id` bigint  NULL COMMENT '用户id',
    `nick_name` STRING  NULL COMMENT '用户昵称',
    `head_img` STRING  NULL,
    `sku_id` bigint  NULL COMMENT 'skuid',
    `spu_id` bigint  NULL COMMENT '商品id',
    `order_id` bigint  NULL COMMENT '订单编号',
    `appraise` STRING NULL COMMENT '评价 1 好评 2 中评 3 差评',
    `comment_txt` STRING NULL COMMENT '评价内容',
    `create_time` timestamp(3) NOT NULL  COMMENT '创建时间',
    `operate_time` timestamp(3) NOT NULL  COMMENT '修改时间',
    PRIMARY KEY(`id`) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'scan.startup.mode' = 'earliest-offset',
    'hostname' = '192.168.244.129',
    'port' = '3306',
    'username' = 'root',
    'password' = '',
    'database-name' = 'gmall',
    'table-name' = 'comment_info',
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


CREATE TABLE IF NOT EXISTS iceberg_ods.ods_comment_info_full(
    `id` bigint NOT NULL  COMMENT '编号',
    `k1` STRING COMMENT '分区字段',
    `user_id` bigint  NULL COMMENT '用户id',
    `nick_name` STRING  NULL COMMENT '用户昵称',
    `head_img` STRING  NULL,
    `sku_id` bigint  NULL COMMENT 'skuid',
    `spu_id` bigint  NULL COMMENT '商品id',
    `order_id` bigint  NULL COMMENT '订单编号',
    `appraise` STRING NULL COMMENT '评价 1 好评 2 中评 3 差评',
    `comment_txt` STRING NULL COMMENT '评价内容',
    `create_time` timestamp(3) NOT NULL  COMMENT '创建时间',
    `operate_time` timestamp(3) NOT NULL  COMMENT '修改时间',
    PRIMARY KEY (`id`,`k1` ) NOT ENFORCED
)   PARTITIONED BY (`k1` ) WITH (
    'catalog-name'='hive_prod',
    'uri'='thrift://192.168.244.129:9083',
    'warehouse'='hdfs://192.168.244.129:9000/user/hive/warehouse/'
);

INSERT INTO iceberg_ods.ods_comment_info_full  /*+ OPTIONS('upsert-enabled'='true') */(
    `id`,
    `k1`,
    `user_id`,
    `nick_name`,
    `head_img`,
    `sku_id`,
    `spu_id`,
    `order_id`,
    `appraise`,
    `comment_txt`,
    `create_time`,
    `operate_time`
)
select
    id,
    DATE_FORMAT(create_time, 'yyyy-MM-dd') AS k1,
    `user_id`,
    `nick_name`,
    `head_img`,
    `sku_id`,
    `spu_id`,
    `order_id`,
    `appraise`,
    `comment_txt`,
    `create_time`,
    `operate_time`
from default_catalog.default_database.comment_info_full_mq
where create_time is not null;

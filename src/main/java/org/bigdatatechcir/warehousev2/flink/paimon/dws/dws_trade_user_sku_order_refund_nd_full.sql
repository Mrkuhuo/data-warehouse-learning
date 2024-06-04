SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

CREATE CATALOG paimon_hive WITH (
    'type' = 'paimon',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);

use CATALOG paimon_hive;

create  DATABASE IF NOT EXISTS dws;

CREATE TABLE IF NOT EXISTS dws.dws_trade_user_sku_order_refund_nd_full(
    `user_id`                     BIGINT COMMENT '用户id',
    `sku_id`                      BIGINT COMMENT 'sku_id',
    `k1`                          STRING COMMENT '分区字段',
    `sku_name`                    STRING COMMENT 'sku名称',
    `category1_id`                BIGINT COMMENT '一级分类id',
    `category1_name`              STRING COMMENT '一级分类名称',
    `category2_id`                BIGINT COMMENT '一级分类id',
    `category2_name`              STRING COMMENT '一级分类名称',
    `category3_id`                BIGINT COMMENT '一级分类id',
    `category3_name`              STRING COMMENT '一级分类名称',
    `tm_id`                       BIGINT COMMENT '品牌id',
    `tm_name`                     STRING COMMENT '品牌名称',
    `order_refund_count_30d`      BIGINT COMMENT '最近30日退单次数',
    `order_refund_num_30d`        BIGINT COMMENT '最近30日退单件数',
    `order_refund_amount_30d`     DECIMAL(16, 2) COMMENT '最近30日退单金额',
    PRIMARY KEY (`user_id`,`sku_id`,`k1` ) NOT ENFORCED
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

INSERT INTO dws.dws_trade_user_sku_order_refund_nd_full(
    user_id,
    sku_id,
    k1,
    sku_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    tm_id,
    tm_name,
    order_refund_count_30d,
    order_refund_num_30d,
    order_refund_amount_30d
    )
select
    user_id,
    sku_id,
    k1,
    sku_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    tm_id,
    tm_name,
    sum(order_refund_count_1d),
    sum(order_refund_num_1d),
    sum(order_refund_amount_1d)
from dws.dws_trade_user_sku_order_refund_1d_full
group by user_id,sku_id,k1,sku_name,category1_id,category1_name,category2_id,category2_name,category3_id,category3_name,tm_id,tm_name;
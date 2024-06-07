SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'execution.runtime-mode' = 'streaming';

CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);

use CATALOG iceberg_catalog;

create  DATABASE IF NOT EXISTS iceberg_dwd;

CREATE TABLE IF NOT EXISTS iceberg_dwd.dwd_tool_coupon_order_full(
    `id`         BIGINT COMMENT '编号',
    `k1`         STRING COMMENT '分区字段',
    `coupon_id`  BIGINT COMMENT '优惠券ID',
    `user_id`    BIGINT COMMENT 'user_id',
    `order_id`   BIGINT COMMENT 'order_id',
    `date_id`    STRING COMMENT '日期ID',
    `order_time` TIMESTAMP(3) COMMENT '使用下单时间',
    PRIMARY KEY (`id`,`k1` ) NOT ENFORCED
    )   PARTITIONED BY (`k1` ) WITH (
    'catalog-name'='hive_prod',
    'uri'='thrift://192.168.244.129:9083',
    'warehouse'='hdfs://192.168.244.129:9000/user/hive/warehouse/'
    );


insert into iceberg_dwd.dwd_tool_coupon_order_full /*+ OPTIONS('upsert-enabled'='true') */(
    id,
    k1,
    coupon_id,
    user_id,
    order_id,
    date_id,
    order_time
)
select
    id,
    k1,
    coupon_id,
    user_id,
    order_id,
    date_format(using_time,'yyyy-MM-dd') date_id,
    using_time
from iceberg_ods.ods_coupon_use_full /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/ ;
where using_time is not null;
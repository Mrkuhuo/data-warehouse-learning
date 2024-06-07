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

CREATE TABLE IF NOT EXISTS iceberg_dwd.dwd_trade_cart_full(
    `id`       BIGINT COMMENT '编号',
    `k1`       STRING COMMENT '分区字段',
    `user_id`  STRING COMMENT '用户id',
    `sku_id`   BIGINT COMMENT '商品id',
    `sku_name` STRING COMMENT '商品名称',
    `sku_num`  INT COMMENT '加购物车件数',
    PRIMARY KEY (`id`,`k1` ) NOT ENFORCED
    )   PARTITIONED BY (`k1` ) WITH (
    'catalog-name'='hive_prod',
    'uri'='thrift://192.168.244.129:9083',
    'warehouse'='hdfs://192.168.244.129:9000/user/hive/warehouse/'
    );


INSERT INTO iceberg_dwd.dwd_trade_cart_full /*+ OPTIONS('upsert-enabled'='true') */(
    id,
    k1,
    user_id,
    sku_id,
    sku_name,
    sku_num
    )
select
    id,
    k1,
    user_id,
    sku_id,
    sku_name,
    sku_num
from iceberg_ods.ods_cart_info_full /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/
where is_ordered=0;
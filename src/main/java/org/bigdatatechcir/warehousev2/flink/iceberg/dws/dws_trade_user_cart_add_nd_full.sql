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

create  DATABASE IF NOT EXISTS iceberg_dws;

CREATE TABLE IF NOT EXISTS iceberg_dws.dws_trade_user_cart_add_nd_full(
    `user_id`                   STRING COMMENT '用户id',
    `k1`                        STRING  COMMENT '分区字段',
    `cart_add_count_30d` BIGINT COMMENT '最近30日加购次数',
    `cart_add_num_30d`   BIGINT COMMENT '最近30日加购商品件数',
    PRIMARY KEY (`user_id`,`k1` ) NOT ENFORCED
    )   PARTITIONED BY (`k1` ) WITH (
    'catalog-name'='hive_prod',
    'uri'='thrift://192.168.244.129:9083',
    'warehouse'='hdfs://192.168.244.129:9000/user/hive/warehouse/'
    );

INSERT INTO iceberg_dws.dws_trade_user_cart_add_nd_full /*+ OPTIONS('upsert-enabled'='true') */(
    user_id,
    k1,
    cart_add_count_30d,
    cart_add_num_30d
    )
select
    user_id,
    k1,
    sum(cart_add_count_1d),
    sum(cart_add_num_1d)
from iceberg_dws.dws_trade_user_cart_add_1d_full /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/
group by user_id,k1;
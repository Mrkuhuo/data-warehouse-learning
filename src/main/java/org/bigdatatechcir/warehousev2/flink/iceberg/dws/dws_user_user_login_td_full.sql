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

CREATE TABLE IF NOT EXISTS iceberg_dws.dws_user_user_login_td_full(
    `user_id`         BIGINT COMMENT '用户id',
    `k1`              STRING COMMENT '分区字段',
    `login_date_last` STRING COMMENT '末次登录日期',
    `login_count_td`  BIGINT COMMENT '累计登录次数',
    PRIMARY KEY (`user_id`,`k1` ) NOT ENFORCED
    )   PARTITIONED BY (`k1` ) WITH (
    'catalog-name'='hive_prod',
    'uri'='thrift://192.168.244.129:9083',
    'warehouse'='hdfs://192.168.244.129:9000/user/hive/warehouse/'
    );


INSERT INTO iceberg_dws.dws_user_user_login_td_full /*+ OPTIONS('upsert-enabled'='true') */(
    user_id,
    k1,
    login_date_last,
    login_count_td
    )
select
    u.id,
    u.k1,
    login_date_last,
    login_count_td
from
    (
        select
            id,
            k1,
            create_time
        from iceberg_dim.dim_user_zip_full /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/
    )u
        left join
    (
        select
            user_id,
            max(k1) login_date_last,
            count(*) login_count_td
        from iceberg_dwd.dwd_user_login_full /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/
        group by user_id
    )l
    on cast(u.id as STRING)=l.user_id;
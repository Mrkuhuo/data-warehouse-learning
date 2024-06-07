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

CREATE TABLE IF NOT EXISTS iceberg_dwd.dwd_interaction_comment_full(
    `id`            BIGINT COMMENT '编号',
    `k1`            STRING COMMENT '分区字段',
    `user_id`       BIGINT COMMENT '用户ID',
    `sku_id`        BIGINT COMMENT 'sku_id',
    `order_id`      BIGINT COMMENT '订单ID',
    `date_id`       STRING COMMENT '日期ID',
    `create_time`   timestamp(3) COMMENT '评价时间',
    `appraise_code` STRING COMMENT '评价编码',
    `appraise_name` STRING COMMENT '评价名称',
    PRIMARY KEY (`id`,`k1` ) NOT ENFORCED
    )   PARTITIONED BY (`k1` ) WITH (
    'catalog-name'='hive_prod',
    'uri'='thrift://192.168.244.129:9083',
    'warehouse'='hdfs://192.168.244.129:9000/user/hive/warehouse/'
    );


insert into iceberg_dwd.dwd_interaction_comment_full /*+ OPTIONS('upsert-enabled'='true') */(
    id,
    k1,
    user_id,
    sku_id,
    order_id,
    date_id,
    create_time,
    appraise_code,
    appraise_name
)
select
    id,
    k1,
    user_id,
    sku_id,
    order_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time,
    appraise,
    dic_name
from
    (
        select
            id,
            k1,
            user_id,
            sku_id,
            order_id,
            create_time,
            appraise
        from iceberg_ods.ods_comment_info_full /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/
    )ci
        left join
    (
        select
            dic_code,
            dic_name
        from iceberg_ods.ods_base_dic_full /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/
    )dic
    on ci.appraise=dic.dic_code;
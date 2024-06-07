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

create  DATABASE IF NOT EXISTS iceberg_dim;

CREATE TABLE IF NOT EXISTS iceberg_dim.dim_user_zip_full(
    `id`           BIGINT COMMENT '用户id',
    `k1`           STRING COMMENT '分区字段',
    `login_name`   STRING COMMENT '用户名称',
    `nick_name`    STRING COMMENT '用户昵称',
    `name`         STRING COMMENT '用户姓名',
    `phone_num`    STRING COMMENT '手机号码',
    `email`        STRING COMMENT '邮箱',
    `user_level`   STRING COMMENT '用户等级',
    `birthday`     STRING COMMENT '生日',
    `gender`       STRING COMMENT '性别',
    `create_time`  TIMESTAMP(3) COMMENT '创建时间',
    `operate_time` TIMESTAMP(3) COMMENT '操作时间',
    `start_date`   CHAR(10)  COMMENT '开始日期',
    `end_date`     CHAR(10)  COMMENT '结束日期',
    PRIMARY KEY (`id`,`k1` ) NOT ENFORCED
    )   PARTITIONED BY (`k1` ) WITH (
    'catalog-name'='hive_prod',
    'uri'='thrift://192.168.244.129:9083',
    'warehouse'='hdfs://192.168.244.129:9000/user/hive/warehouse/'
    );

insert into iceberg_dim.dim_user_zip_full /*+ OPTIONS('upsert-enabled'='true') */(
    id,
    k1,
    login_name,
    nick_name,
    name,
    phone_num,
    email,
    user_level,
    birthday,
    gender,
    create_time,
    operate_time,
    start_date,
    end_date
    )
select
    id,
    k1,
    login_name,
    nick_name,
    md5(name),
    md5(phone_num),
    md5(email),
    user_level,
    birthday,
    gender,
    create_time,
    operate_time,
    '2020-06-14' start_date,
    '9999-12-31' end_date
from iceberg_ods.ods_user_info_full /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/;
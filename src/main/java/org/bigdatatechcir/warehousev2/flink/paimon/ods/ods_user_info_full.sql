-- 用户表（增量表）
SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';

CREATE TABLE user_info_full_mq (
    `id` bigint NOT NULL  COMMENT '编号',
    `login_name` STRING  NULL COMMENT '用户名称',
    `nick_name` STRING  NULL COMMENT '用户昵称',
    `passwd` STRING  NULL COMMENT '用户密码',
    `name` STRING  NULL COMMENT '用户姓名',
    `phone_num` STRING  NULL COMMENT '手机号',
    `email` STRING  NULL COMMENT '邮箱',
    `head_img` STRING  NULL COMMENT '头像',
    `user_level` STRING  NULL COMMENT '用户级别',
    `birthday` STRING  NULL COMMENT '用户生日',
    `gender` STRING  NULL COMMENT '性别 M男,F女',
    `create_time` timestamp(3) NOT NULL   COMMENT '创建时间',
    `operate_time` timestamp(3) NOT NULL   COMMENT '修改时间',
    `status` STRING  NULL COMMENT '状态',
    PRIMARY KEY(`id`) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'scan.startup.mode' = 'earliest-offset',
    'hostname' = '192.168.244.129',
    'port' = '3306',
    'username' = 'root',
    'password' = '',
    'database-name' = 'gmall',
    'table-name' = 'user_info',
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

CREATE TABLE IF NOT EXISTS ods.ods_user_info_full(
    `id` BIGINT COMMENT '活动id',
    `k1` STRING COMMENT '分区字段',
    `login_name` STRING  NULL COMMENT '用户名称',
    `nick_name` STRING  NULL COMMENT '用户昵称',
    `passwd` STRING  NULL COMMENT '用户密码',
    `name` STRING  NULL COMMENT '用户姓名',
    `phone_num` STRING  NULL COMMENT '手机号',
    `email` STRING  NULL COMMENT '邮箱',
    `head_img` STRING  NULL COMMENT '头像',
    `user_level` STRING  NULL COMMENT '用户级别',
    `birthday` STRING  NULL COMMENT '用户生日',
    `gender` STRING  NULL COMMENT '性别 M男,F女',
    `create_time` timestamp(3) NOT NULL   COMMENT '创建时间',
    `operate_time` timestamp(3) NOT NULL   COMMENT '修改时间',
    `status` STRING  NULL COMMENT '状态',
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

INSERT INTO ods.ods_user_info_full(
    `id`,
    `k1`,
    `login_name`,
    `nick_name`,
    `passwd`,
    `name`,
    `phone_num`,
    `email`,
    `head_img`,
    `user_level`,
    `birthday`,
    `gender`,
    `create_time`,
    `operate_time`,
    `status`
)select
    id,
    DATE_FORMAT(create_time, 'yyyy-MM-dd') AS k1,
    `login_name`,
    `nick_name`,
    `passwd`,
    `name`,
    `phone_num`,
    `email`,
    `head_img`,
    `user_level`,
    `birthday`,
    `gender`,
    `create_time`,
    `operate_time`,
    `status`
from default_catalog.default_database.user_info_full_mq
where create_time is not null;

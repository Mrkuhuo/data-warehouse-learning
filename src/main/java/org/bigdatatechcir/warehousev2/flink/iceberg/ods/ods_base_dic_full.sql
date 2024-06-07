SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';

CREATE TABLE base_dic_full_mq (
    `dic_code` STRING  NOT NULL COMMENT '编号',
    `dic_name` STRING  NULL COMMENT '编码名称',
    `parent_code` STRING  NULL COMMENT '父编号',
    `create_time` TIMESTAMP(3)  NULL  COMMENT '创建时间',
    `operate_time` TIMESTAMP(3)  NULL  COMMENT '修改日期',
    PRIMARY KEY(`dic_code`) NOT ENFORCED
) WITH (
      'connector' = 'mysql-cdc',
      'scan.startup.mode' = 'earliest-offset',
      'hostname' = '192.168.244.129',
      'port' = '3306',
      'username' = 'root',
      'password' = '',
      'database-name' = 'gmall',
      'table-name' = 'base_dic',
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


CREATE TABLE IF NOT EXISTS iceberg_ods.ods_base_dic_full(
    `dic_code` STRING  NOT NULL COMMENT '编号',
    `k1`  STRING COMMENT '分区字段',
    `dic_name` STRING  NULL COMMENT '编码名称',
    `parent_code` STRING  NULL COMMENT '父编号',
    `create_time` STRING  NULL  COMMENT '创建时间',
    `operate_time` STRING  NULL  COMMENT '修改日期',
    PRIMARY KEY (`dic_code`,`k1`) NOT ENFORCED
)PARTITIONED BY (`k1` ) WITH (
    'catalog-name'='hive_prod',
    'uri'='thrift://192.168.244.129:9083',
    'warehouse'='hdfs://192.168.244.129:9000/user/hive/warehouse/'
    );

INSERT INTO iceberg_ods.ods_base_dic_full  /*+ OPTIONS('upsert-enabled'='true') */(
    `dic_code`,
    `k1`,
    `dic_name`,
    `parent_code`,
    `create_time`,
    `operate_time`
)
select
    `dic_code`,
    DATE_FORMAT(create_time, 'yyyy-MM-dd') AS k1,
    `dic_name`,
    `parent_code`,
    DATE_FORMAT(create_time, 'yyyy-MM-dd HH:mm:ss') AS start_time,
    DATE_FORMAT(operate_time, 'yyyy-MM-dd HH:mm:ss') AS start_time
from default_catalog.default_database.base_dic_full_mq;

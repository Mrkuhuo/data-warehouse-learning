SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';

CREATE TABLE dim_date_full (
    `date_id`    VARCHAR(255) COMMENT '日期ID',
    `week_id`    int COMMENT '周ID,一年中的第几周',
    `week_day`   int COMMENT '周几',
    `day`        int COMMENT '每月的第几天',
    `month`      int COMMENT '一年中的第几月',
    `quarter`    int COMMENT '一年中的第几季度',
    `year`       int COMMENT '年份',
    `is_workday` int COMMENT '是否是工作日',
    `holiday_id` VARCHAR(255) COMMENT '节假日',
    PRIMARY KEY(`date_id`) NOT ENFORCED
) WITH (
      'connector' = 'mysql-cdc',
      'scan.startup.mode' = 'earliest-offset',
      'hostname' = '192.168.244.129',
      'port' = '3306',
      'username' = 'root',
      'password' = '',
      'database-name' = 'gmall',
      'table-name' = 'dim_date',
      'server-time-zone' = 'Asia/Shanghai'
      );

create catalog hudi_catalog with(
	'type' = 'hudi',
	'mode' = 'hms',
	'hive.conf.dir'='/opt/software/apache-hive-3.1.3-bin/conf'
);

use CATALOG hudi_catalog;

create  DATABASE IF NOT EXISTS hudi_dim;

CREATE TABLE IF NOT EXISTS hudi_dim.dim_date_full(
    `date_id`    VARCHAR(255) COMMENT '日期ID',
    `week_id`    int COMMENT '周ID,一年中的第几周',
    `week_day`   int COMMENT '周几',
    `day`        int COMMENT '每月的第几天',
    `month`      int COMMENT '一年中的第几月',
    `quarter`    int COMMENT '一年中的第几季度',
    `year`       int COMMENT '年份',
    `is_workday` int COMMENT '是否是工作日',
    `holiday_id` VARCHAR(255) COMMENT '节假日',
    PRIMARY KEY (`date_id` ) NOT ENFORCED
    );

INSERT INTO hudi_dim.dim_date_full(
    `date_id`,
    `week_id`,
    `week_day`,
    `day`,
    `month`,
    `quarter`,
    `year`,
    `is_workday`,
    `holiday_id`
    )
select
    `date_id`,
    `week_id`,
    `week_day`,
    `day`,
    `month`,
    `quarter`,
    `year`,
    `is_workday`,
    `holiday_id`
from default_catalog.default_database.dim_date_full;
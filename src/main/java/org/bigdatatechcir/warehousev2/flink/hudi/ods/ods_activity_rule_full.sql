SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';

CREATE TABLE activity_rule_full_mq (
   `id` int NOT NULL  COMMENT '编号',
   `activity_id` int  NULL COMMENT '类型',
   `activity_type` STRING  NULL COMMENT '活动类型',
   `condition_amount` decimal(16,2)  NULL COMMENT '满减金额',
   `condition_num` BIGINT  NULL COMMENT '满减件数',
   `benefit_amount` decimal(16,2)  NULL COMMENT '优惠金额',
   `benefit_discount` decimal(10,2)  NULL COMMENT '优惠折扣',
   `benefit_level` BIGINT  NULL COMMENT '优惠级别',
    PRIMARY KEY(`id`) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'scan.startup.mode' = 'earliest-offset',
    'hostname' = '192.168.244.129',
    'port' = '3306',
    'username' = 'root',
    'password' = '',
    'database-name' = 'gmall',
    'table-name' = 'activity_rule',
    'server-time-zone' = 'Asia/Shanghai'
      );

create catalog hudi_catalog with(
	'type' = 'hudi',
	'mode' = 'hms',
	'hive.conf.dir'='/opt/software/apache-hive-3.1.3-bin/conf'
);

use CATALOG hudi_catalog;

create  DATABASE IF NOT EXISTS hudi_ods;

CREATE TABLE IF NOT EXISTS hudi_ods.ods_activity_rule_full(
    `id`               INT COMMENT '编号',
    `activity_id`      INT COMMENT '类型',
    `activity_type`    STRING COMMENT '活动类型',
    `condition_amount` DECIMAL(16, 2) COMMENT '满减金额',
    `condition_num`    BIGINT COMMENT '满减件数',
    `benefit_amount`   DECIMAL(16, 2) COMMENT '优惠金额',
    `benefit_discount` DECIMAL(16, 2) COMMENT '优惠折扣',
    `benefit_level`    BIGINT COMMENT '优惠级别',
    PRIMARY KEY (`id`) NOT ENFORCED
    ) WITH (
          'connector' = 'hudi',
          'table.type' = 'MERGE_ON_READ',
          'read.streaming.enabled' = 'true',
          'read.streaming.check-interval' = '4',
          'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
    );

INSERT INTO hudi_ods.ods_activity_rule_full(
    `id`,
    `activity_id`,
    `activity_type`,
    `condition_amount`,
    `condition_num`,
    `benefit_amount`,
    `benefit_discount`,
    `benefit_level`
)
select
    `id`,
    `activity_id`,
    `activity_type`,
    `condition_amount`,
    `condition_num`,
    `benefit_amount`,
    `benefit_discount`,
    `benefit_level`
from default_catalog.default_database.activity_rule_full_mq;

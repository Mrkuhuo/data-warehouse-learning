SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';

CREATE TABLE payment_info_full_mq (
    `id` int NOT NULL  COMMENT '编号',
    `out_trade_no` string  NULL COMMENT '对外业务编号',
    `order_id` bigint  NULL COMMENT '订单编号',
    `user_id` bigint  NULL,
    `payment_type` string  NULL COMMENT '支付类型（微信 支付宝）',
    `trade_no` string  NULL COMMENT '交易编号',
    `total_amount` decimal(10,2)  NULL COMMENT '支付金额',
    `subject` string  NULL COMMENT '交易内容',
    `payment_status` string  NULL COMMENT '支付状态',
    `create_time` timestamp(3) NOT NULL   COMMENT '创建时间',
    `callback_time` timestamp(3)  NULL COMMENT '回调时间',
    `callback_content` string  COMMENT '回调信息',
     PRIMARY KEY(`id`) NOT ENFORCED
) WITH (
      'connector' = 'mysql-cdc',
      'scan.startup.mode' = 'earliest-offset',
      'hostname' = '192.168.244.129',
      'port' = '3306',
      'username' = 'root',
      'password' = '',
      'database-name' = 'gmall',
      'table-name' = 'payment_info',
      'server-time-zone' = 'Asia/Shanghai'
      );

create catalog hudi_catalog with(
	'type' = 'hudi',
	'mode' = 'hms',
	'hive.conf.dir'='/opt/software/apache-hive-3.1.3-bin/conf'
);

use CATALOG hudi_catalog;

create  DATABASE IF NOT EXISTS hudi_ods;

CREATE TABLE IF NOT EXISTS hudi_ods.ods_payment_info_full(
    `id` bigint NOT NULL  COMMENT '购物券编号',
    `k1` STRING COMMENT '分区字段',
    `out_trade_no` string  NULL COMMENT '对外业务编号',
    `order_id` bigint  NULL COMMENT '订单编号',
    `user_id` bigint  NULL,
    `payment_type` string  NULL COMMENT '支付类型（微信 支付宝）',
    `trade_no` string  NULL COMMENT '交易编号',
    `total_amount` decimal(10,2)  NULL COMMENT '支付金额',
    `subject` string  NULL COMMENT '交易内容',
    `payment_status` string  NULL COMMENT '支付状态',
    `create_time` timestamp(3) NOT NULL   COMMENT '创建时间',
    `callback_time` timestamp(3)  NULL COMMENT '回调时间',
    `callback_content` string  COMMENT '回调信息',
    PRIMARY KEY (`id`,`k1` ) NOT ENFORCED
    )   PARTITIONED BY (`k1` ) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
    );

INSERT INTO hudi_ods.ods_payment_info_full(
    `id`,
    `k1`,
    `out_trade_no`,
    `order_id`,
    `user_id`,
    `payment_type`,
    `trade_no`,
    `total_amount`,
    `subject`,
    `payment_status`,
    `create_time`,
    `callback_time`,
    `callback_content`
)
select
    id,
    DATE_FORMAT(create_time, 'yyyy-MM-dd') AS k1,
    `out_trade_no`,
    `order_id`,
    `user_id`,
    `payment_type`,
    `trade_no`,
    `total_amount`,
    `subject`,
    `payment_status`,
    `create_time`,
    `callback_time`,
    `callback_content`
from default_catalog.default_database.payment_info_full_mq
where create_time is not null;
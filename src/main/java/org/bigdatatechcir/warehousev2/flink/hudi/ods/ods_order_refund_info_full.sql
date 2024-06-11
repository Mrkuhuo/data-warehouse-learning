SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';

CREATE TABLE order_refund_info_full_mq (
    `id` bigint NOT NULL  COMMENT '编号',
    `user_id` bigint  NULL COMMENT '用户id',
    `order_id` bigint  NULL COMMENT '订单id',
    `sku_id` bigint  NULL COMMENT 'skuid',
    `refund_type` string  NULL COMMENT '退款类型',
    `refund_num` bigint  NULL COMMENT '退货件数',
    `refund_amount` decimal(16,2)  NULL COMMENT '退款金额',
    `refund_reason_type` string  NULL COMMENT '原因类型',
    `refund_reason_txt` string  NULL COMMENT '原因内容',
    `refund_status` string  NULL COMMENT '退款状态（0：待审批 1：已退款）',
    `create_time` timestamp(3) NOT NULL   COMMENT '创建时间',
     PRIMARY KEY(`id`) NOT ENFORCED
) WITH (
      'connector' = 'mysql-cdc',
      'scan.startup.mode' = 'earliest-offset',
      'hostname' = '192.168.244.129',
      'port' = '3306',
      'username' = 'root',
      'password' = '',
      'database-name' = 'gmall',
      'table-name' = 'order_refund_info',
      'server-time-zone' = 'Asia/Shanghai'
      );

create catalog hudi_catalog with(
	'type' = 'hudi',
	'mode' = 'hms',
	'hive.conf.dir'='/opt/software/apache-hive-3.1.3-bin/conf'
);

use CATALOG hudi_catalog;

create  DATABASE IF NOT EXISTS hudi_ods;

CREATE TABLE IF NOT EXISTS hudi_ods.ods_order_refund_info_full(
    `id` bigint NOT NULL  COMMENT '购物券编号',
    `k1` STRING COMMENT '分区字段',
    `user_id` bigint  NULL COMMENT '用户id',
    `order_id` bigint  NULL COMMENT '订单id',
    `sku_id` bigint  NULL COMMENT 'skuid',
    `refund_type` string  NULL COMMENT '退款类型',
    `refund_num` bigint  NULL COMMENT '退货件数',
    `refund_amount` decimal(16,2)  NULL COMMENT '退款金额',
    `refund_reason_type` string  NULL COMMENT '原因类型',
    `refund_reason_txt` string  NULL COMMENT '原因内容',
    `refund_status` string  NULL COMMENT '退款状态（0：待审批 1：已退款）',
    `create_time` timestamp(3) NOT NULL   COMMENT '创建时间',
    PRIMARY KEY (`id`,`k1` ) NOT ENFORCED
    )   PARTITIONED BY (`k1` ) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
    );

INSERT INTO hudi_ods.ods_order_refund_info_full(
    `id`,
    `k1`,
    `user_id`,
    `order_id`,
    `sku_id`,
    `refund_type`,
    `refund_num`,
    `refund_amount`,
    `refund_reason_type`,
    `refund_reason_txt`,
    `refund_status`,
    `create_time`
)
select
    id,
    DATE_FORMAT(create_time, 'yyyy-MM-dd') AS k1,
    `user_id`,
    `order_id`,
    `sku_id`,
    `refund_type`,
    `refund_num`,
    `refund_amount`,
    `refund_reason_type`,
    `refund_reason_txt`,
    `refund_status`,
    `create_time`
from default_catalog.default_database.order_refund_info_full_mq
where create_time is not null;
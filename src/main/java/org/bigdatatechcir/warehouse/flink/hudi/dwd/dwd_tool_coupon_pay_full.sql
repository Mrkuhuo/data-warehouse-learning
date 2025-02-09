SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

create catalog hudi_catalog with(
	'type' = 'hudi',
	'mode' = 'hms',
	'hive.conf.dir'='/opt/software/apache-hive-3.1.3-bin/conf'
);

use CATALOG hudi_catalog;

create  DATABASE IF NOT EXISTS hudi_dwd;

CREATE TABLE IF NOT EXISTS hudi_dwd.dwd_tool_coupon_pay_full(
    `id`           BIGINT COMMENT '编号',
    `k1`           STRING COMMENT '分区字段',
    `coupon_id`    BIGINT COMMENT '优惠券ID',
    `user_id`      BIGINT COMMENT 'user_id',
    `order_id`     BIGINT COMMENT 'order_id',
    `date_id`      STRING COMMENT '日期ID',
    `payment_time` TIMESTAMP(3) COMMENT '使用下单时间',
    PRIMARY KEY (`id`,`k1` ) NOT ENFORCED
    )   PARTITIONED BY (`k1` ) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
    );


insert into hudi_dwd.dwd_tool_coupon_pay_full(
    id,
    k1,
    coupon_id,
    user_id,
    order_id,
    date_id,
    payment_time
)
select
    id,
    k1,
    coupon_id,
    user_id,
    order_id,
    date_format(used_time,'yyyy-MM-dd') date_id,
    used_time
from hudi_ods.ods_coupon_use_full
where used_time is not null;
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

create  DATABASE IF NOT EXISTS hudi_dws;

CREATE TABLE IF NOT EXISTS hudi_dws.dws_trade_activity_order_nd_full(
    `activity_id`                BIGINT COMMENT '活动id',
    `k1`                         STRING  COMMENT '分区字段',
    `activity_name`              STRING COMMENT '活动名称',
    `activity_type_code`         STRING COMMENT '活动类型编码',
    `activity_type_name`         STRING COMMENT '活动类型名称',
    `start_date`                 STRING COMMENT '发布日期',
    `original_amount_30d`        DECIMAL(16, 2) COMMENT '参与活动订单原始金额',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '参与活动订单优惠金额',
    PRIMARY KEY (`activity_id`,`k1` ) NOT ENFORCED
    )   PARTITIONED BY (`k1` ) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
    );


INSERT INTO hudi_dws.dws_trade_activity_order_nd_full(activity_id, k1, activity_name, activity_type_code, activity_type_name, start_date, original_amount_30d, activity_reduce_amount_30d)
select
    act.activity_id,
    od.k1,
    activity_name,
    activity_type_code,
    activity_type_name,
    date_format(start_time,'yyyy-MM-dd'),
    sum(split_original_amount),
    sum(split_activity_amount)
from
    (
        select
            activity_id,
            activity_name,
            activity_type_code,
            activity_type_name,
            start_time
        from hudi_dim.dim_activity_full
        group by activity_id, activity_name, activity_type_code, activity_type_name,start_time
    )act
        left join
    (
        select
            activity_id,
            k1,
            order_id,
            split_original_amount,
            split_activity_amount
        from hudi_dwd.dwd_trade_order_detail_full
        where activity_id is not null
    )od
    on act.activity_id=od.activity_id
group by act.activity_id,od.k1,activity_name,activity_type_code,activity_type_name,start_time;
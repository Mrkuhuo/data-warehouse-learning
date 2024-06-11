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

CREATE TABLE IF NOT EXISTS hudi_dwd.dwd_interaction_favor_add_full(
    `id`          BIGINT COMMENT '编号',
    `k1`          STRING COMMENT '分区字段',
    `user_id`     BIGINT COMMENT '用户id',
    `sku_id`      BIGINT COMMENT 'sku_id',
    `date_id`     STRING COMMENT '日期id',
    `create_time` timestamp(3) COMMENT '收藏时间',
    PRIMARY KEY (`id`,`k1` ) NOT ENFORCED
    )   PARTITIONED BY (`k1` ) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
    );


insert into hudi_dwd.dwd_interaction_favor_add_full(
    id,
    k1,
    user_id,
    sku_id,
    date_id,
    create_time
)
select
    id,
    k1,
    user_id,
    sku_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time
from hudi_ods.ods_favor_info_full;
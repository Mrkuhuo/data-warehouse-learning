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

CREATE TABLE IF NOT EXISTS hudi_dws.dws_traffic_session_page_view_1d_full(
    `session_id`     STRING COMMENT '会话id',
    `mid_id`         STRING comment '设备id',
    `k1`             STRING COMMENT '分区字段',
    `brand`          string comment '手机品牌',
    `model`          string comment '手机型号',
    `operate_system` string comment '操作系统',
    `version_code`   string comment 'app版本号',
    `channel`        string comment '渠道',
    `during_time_1d` BIGINT COMMENT '最近1日访问时长',
    `page_count_1d`  BIGINT COMMENT '最近1日访问页面数',
    PRIMARY KEY (`session_id`,`mid_id`,`k1` ) NOT ENFORCED
    )   PARTITIONED BY (`k1`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
    );


INSERT INTO hudi_dws.dws_traffic_session_page_view_1d_full(session_id, mid_id, k1, brand, model, operate_system, version_code, channel, during_time_1d, page_count_1d)
select
    session_id,
    mid_id,
    k1,
    brand,
    model,
    operate_system,
    version_code,
    channel,
    sum(during_time),
    count(*)
from hudi_dwd.dwd_traffic_page_view_full
group by session_id,mid_id,k1,brand,model,operate_system,version_code,channel;
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

CREATE TABLE IF NOT EXISTS hudi_dws.dws_traffic_page_visitor_page_view_nd_full(
    `mid_id`          string COMMENT '访客id',
    `k1`              string  COMMENT '分区字段',
    `brand`           string comment '手机品牌',
    `model`           string comment '手机型号',
    `operate_system`  string comment '操作系统',
    `page_id`         string COMMENT '页面id',
    `during_time_30d` BIGINT COMMENT '最近30日浏览时长',
    `view_count_30d`  BIGINT COMMENT '最近30日访问次数',
    PRIMARY KEY (`mid_id`,`k1` ) NOT ENFORCED
    )   PARTITIONED BY (`k1`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
    );

INSERT INTO hudi_dws.dws_traffic_page_visitor_page_view_nd_full(
    mid_id,
    k1,
    brand,
    model,
    operate_system,
    page_id,
    during_time_30d,
    view_count_30d
    )
select
    mid_id,
    k1,
    brand,
    model,
    operate_system,
    page_id,
    sum(during_time_1d),
    sum(view_count_1d)
from hudi_dws.dws_traffic_page_visitor_page_view_1d_full
group by mid_id,k1,brand,model,operate_system,page_id;
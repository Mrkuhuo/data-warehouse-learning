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

CREATE TABLE IF NOT EXISTS hudi_dws.dws_user_user_login_td_full(
    `user_id`         BIGINT COMMENT '用户id',
    `k1`              STRING COMMENT '分区字段',
    `login_date_last` STRING COMMENT '末次登录日期',
    `login_count_td`  BIGINT COMMENT '累计登录次数',
    PRIMARY KEY (`user_id`,`k1` ) NOT ENFORCED
    )   PARTITIONED BY (`k1`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
    );


INSERT INTO hudi_dws.dws_user_user_login_td_full(user_id, k1, login_date_last, login_count_td)
select
    u.id,
    u.k1,
    login_date_last,
    login_count_td
from
    (
        select
            id,
            k1,
            create_time
        from hudi_dim.dim_user_zip_full
    )u
        left join
    (
        select
            user_id,
            max(k1) login_date_last,
            count(*) login_count_td
        from hudi_dwd.dwd_user_login_full
        group by user_id
    )l
    on cast(u.id as STRING)=l.user_id;
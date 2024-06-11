SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'batch';

create catalog hudi_catalog with(
	'type' = 'hudi',
	'mode' = 'hms',
	'hive.conf.dir'='/opt/software/apache-hive-3.1.3-bin/conf'
);

use CATALOG hudi_catalog;

create  DATABASE IF NOT EXISTS hudi_dwd;

CREATE TABLE IF NOT EXISTS hudi_dwd.dwd_user_login_full(
    `k1`             STRING COMMENT '分区字段',
    `user_id`        STRING COMMENT '用户ID',
    `date_id`        STRING COMMENT '日期ID',
    `login_time`     STRING COMMENT '登录时间',
    `channel`        STRING COMMENT '应用下载渠道',
    `province_id`    BIGINT COMMENT '省份id',
    `version_code`   STRING COMMENT '应用版本',
    `mid_id`         STRING COMMENT '设备id',
    `brand`          STRING COMMENT '设备品牌',
    `model`          STRING COMMENT '设备型号',
    `operate_system` STRING COMMENT '设备操作系统',
    PRIMARY KEY (`k1`,`user_id`,`date_id` ) NOT ENFORCED
    )   PARTITIONED BY (`k1` ) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
    );

ALTER TABLE hudi_dwd.dwd_user_login_full SET (
    'sink.parallelism' = '10'
    );

INSERT INTO hudi_dwd.dwd_user_login_full(
    k1,
    user_id,
    date_id,
    login_time,
    channel,
    province_id,
    version_code,
    mid_id,
    brand,
    model,
    operate_system
    )
select
    k1,
    user_id,
    DATE_FORMAT(FROM_UNIXTIME(cast(ts / 1000 as BIGINT)), 'yyyy-MM-dd') date_id,
    DATE_FORMAT(FROM_UNIXTIME(cast(ts / 1000 as BIGINT)), 'yyyy-MM-dd HH:mm:ss') login_time,
    channel,
    province_id,
    version_code,
    mid_id,
    brand,
    model,
    operate_system
from
    (
        select
            user_id,
            k1,
            channel,
            area_code,
            version_code,
            mid_id,
            brand,
            model,
            operate_system,
            ts
        from
            (
                select
                    user_id,
                    k1,
                    channel,
                    area_code,
                    version_code,
                    mid_id,
                    brand,
                    model,
                    operate_system,
                    ts,
                    row_number() over (partition by session_id order by ts) rn
                from
                    (
                        select
                            user_id,
                            k1,
                            channel,
                            area_code,
                            version_code,
                            mid_id,
                            brand,
                            model,
                            operate_system,
                            ts,
                            concat(mid_id,'-',CAST(LAST_VALUE(session_start_point) over (partition by mid_id order by ts) as STRING)) session_id
                        from
                            (
                                select
                                    common_uid user_id,
                                    k1,
                                    common_ch channel,
                                    common_ar area_code,
                                    common_vc version_code,
                                    common_mid mid_id,
                                    common_ba brand,
                                    common_md model,
                                    common_os operate_system,
                                    ts,
                                    ts session_start_point
                                from hudi_ods.ods_log_inc
                                where  page_last_page_id is not null
                            )t1
                    )t2
                where user_id is not null
            )t3
        where rn=1
    )t4
        left join
    (
        select
            id province_id,
            area_code
        from hudi_ods.ods_base_province_full
    )bp
    on t4.area_code=bp.area_code;
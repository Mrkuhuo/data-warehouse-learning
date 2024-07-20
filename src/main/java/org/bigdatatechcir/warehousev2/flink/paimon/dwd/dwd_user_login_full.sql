-- 用户域用户登录事务事实表
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

CREATE CATALOG paimon_hive WITH (
    'type' = 'paimon',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);

use CATALOG paimon_hive;

create  DATABASE IF NOT EXISTS dwd;

CREATE TABLE IF NOT EXISTS dwd.dwd_user_login_full(
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
    'connector' = 'paimon',
    'metastore.partitioned-table' = 'true',
    'file.format' = 'parquet',
    'write-buffer-size' = '512mb',
    'write-buffer-spillable' = 'true' ,
    'partition.expiration-time' = '1 d',
    'partition.expiration-check-interval' = '1 h',
    'partition.timestamp-formatter' = 'yyyy-MM-dd',
    'partition.timestamp-pattern' = '$k1'
    );

ALTER TABLE dwd.dwd_user_login_full SET (
    'sink.parallelism' = '10'
    );

INSERT INTO dwd.dwd_user_login_full(
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
                                from ods.ods_log_inc
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
        from ods.ods_base_province_full
    )bp
    on t4.area_code=bp.area_code;
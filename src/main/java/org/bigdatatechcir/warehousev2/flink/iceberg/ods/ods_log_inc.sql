SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';

CREATE TABLE kafka_source (
    kafka_partition INT METADATA FROM 'partition',
    kafka_offset INT METADATA FROM 'offset',
    kafka_timestamp TIMESTAMP(3) METADATA FROM 'timestamp',
    `common` ROW<ar STRING, ba STRING, ch STRING, is_new STRING, md STRING, mid STRING, os STRING, uid STRING, vc STRING> NULL,
    `start`  ROW<entry STRING, loading_time STRING, open_ad_id STRING , open_ad_ms STRING, open_ad_skip_ms STRING> NULL,
    `page`   ROW<during_time BIGINT, item STRING, item_type STRING, last_page_id  STRING ,page_id STRING, source_type STRING> NULL,
    `actions` STRING,
    `displays` STRING,
    `err` ROW<error_code BIGINT, msg STRING>,
    `ts` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'ODS_BASE_LOG',
    'properties.bootstrap.servers' = '192.168.244.129:9092',
    'properties.group.id' = 'ODS_BASE_LOG',
    -- 'scan.startup.mode' = 'group-offsets',
    'scan.startup.mode' = 'earliest-offset',
    'properties.enable.auto.commit'='true',
    'properties.auto.commit.interval.ms'='5000',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true',
    'json.fail-on-missing-field' = 'false'
);

CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);

use CATALOG iceberg_catalog;

create  DATABASE IF NOT EXISTS iceberg_ods;


CREATE TABLE IF NOT EXISTS iceberg_ods.ods_log_inc(
    `id`                            STRING,
    `k1`                            STRING,
    `common_ar`                     STRING,
    `common_ba`                     STRING,
    `common_ch`                     STRING,
    `common_is_new`                 STRING,
    `common_md`                     STRING,
    `common_mid`                    STRING,
    `common_os`                     STRING,
    `common_uid`                    STRING,
    `common_vc`                     STRING,
    `start_entry`                   STRING,
    `start_loading_time`            STRING,
    `start_open_ad_id`              STRING,
    `start_open_ad_ms`              STRING,
    `start_open_ad_skip_ms`         STRING,
    `page_during_time`              BIGINT,
    `page_item`                     STRING,
    `page_item_type`                STRING,
    `page_last_page_id`             STRING,
    `page_page_id`                  STRING,
    `page_source_type`              STRING,
    `actions`                       STRING COMMENT '动作信息',
    `displays`                      STRING COMMENT '曝光信息',
    `err_error_code`                BIGINT,
    `err_msg`                       STRING,
    `ts`                            BIGINT  COMMENT '时间戳',
    PRIMARY KEY (`id`) NOT ENFORCED
);

insert into iceberg_ods.ods_log_inc  /*+ OPTIONS('upsert-enabled'='true') */(
    `id`, `k1`,`common_ar`,`common_ba`,`common_ch`,`common_is_new`,`common_md`,`common_mid`,`common_os`,`common_uid`,`common_vc`,`start_entry`,`start_loading_time`,`start_open_ad_id`,`start_open_ad_ms`,`start_open_ad_skip_ms`,`page_during_time`,`page_item`,`page_item_type`,`page_last_page_id`,`page_page_id`,`page_source_type`,`actions`,`displays`,`err_error_code`,`err_msg`,`ts`)
select
    CONCAT(cast(kafka_partition as string), cast(kafka_offset as string), cast(kafka_timestamp as string)) as id,
    DATE_FORMAT(FROM_UNIXTIME(cast(ts / 1000 as BIGINT)), 'yyyy-MM-dd') AS k1,
    `common`.ar,
    `common`.ba,
    `common`.ch,
    `common`.is_new,
    `common`.md,
    `common`.mid,
    `common`.os,
    `common`.uid,
    `common`.vc,
    `start`.entry,
    `start`.loading_time,
    `start`.open_ad_id,
    `start`.open_ad_ms,
    `start`.open_ad_skip_ms,
    `page`.during_time,
    `page`.item,
    `page`.item_type,
    `page`.last_page_id,
    `page`.page_id,
    `page`.source_type,
    `actions`,
    `displays`,
    `err`.error_code,
    `err`.msg,
    `ts`
from default_catalog.default_database.kafka_source;
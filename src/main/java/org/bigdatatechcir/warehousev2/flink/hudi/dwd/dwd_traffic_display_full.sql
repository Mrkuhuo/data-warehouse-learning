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

CREATE TABLE IF NOT EXISTS hudi_dwd.dwd_traffic_display_full(
    `id`                STRING,
    `k1`                STRING NOT NULL   COMMENT '分区字段',
    `province_id`       BIGINT COMMENT '省份id',
    `brand`             STRING COMMENT '手机品牌',
    `channel`           STRING COMMENT '渠道',
    `is_new`            STRING COMMENT '是否首次启动',
    `model`             STRING COMMENT '手机型号',
    `mid_id`            STRING COMMENT '设备id',
    `operate_system`    STRING COMMENT '操作系统',
    `user_id`           STRING COMMENT '会员id',
    `version_code`      STRING COMMENT 'app版本号',
    `during_time`       BIGINT COMMENT 'app版本号',
    `page_item`         STRING COMMENT '目标id ',
    `page_item_type`    STRING COMMENT '目标类型',
    `last_page_id`      STRING COMMENT '上页类型',
    `page_id`           STRING COMMENT '页面ID ',
    `source_type`       STRING COMMENT '来源类型',
    `date_id`           STRING COMMENT '日期id',
    `display_time`      STRING COMMENT '曝光时间',
    `display_type`      STRING COMMENT '曝光类型',
    `display_item`      STRING COMMENT '曝光对象id ',
    `display_item_type` STRING COMMENT 'app版本号',
    `display_order`     BIGINT COMMENT '曝光顺序',
    `display_pos_id`    BIGINT COMMENT '曝光位置',
    PRIMARY KEY (`id`,`k1` ) NOT ENFORCED
)   PARTITIONED BY (`k1` ) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
    );

CREATE TEMPORARY FUNCTION json_displays_array_parser AS 'org.bigdatatechcir.warehousev2.flink.udf.JsonDisplaysArrayParser';

insert into hudi_dwd.dwd_traffic_display_full(
    `id`,
    `k1`,
    `province_id`,
    `brand`,
    `channel`,
    `is_new`,
    `model`,
    `mid_id`,
    `operate_system`,
    `user_id`,
    `version_code`,
    `during_time`,
    `page_item`,
    `page_item_type`,
    `last_page_id`,
    `page_id`,
    `source_type`,
    `date_id`,
    `display_time`,
    `display_type`,
    `display_item`,
    `display_item_type`,
    `display_order`,
    `display_pos_id`
)
select
    id,
    k1,
    province_id,
    brand,
    channel,
    common_is_new,
    model,
    mid_id,
    operate_system,
    user_id,
    version_code,
    page_during_time,
    page_item,
    page_item_type,
    page_last_page_id,
    page_page_id,
    page_source_type,
    DATE_FORMAT(FROM_UNIXTIME(cast(ts / 1000 as BIGINT)), 'yyyy-MM-dd') date_id,
    DATE_FORMAT(FROM_UNIXTIME(cast(ts / 1000 as BIGINT)), 'yyyy-MM-dd HH:mm:ss') display_time,
    display_type,
    display_item,
    display_item_type,
    display_order,
    display_pos_id
from
    (

        select
            id,
            k1,
            common_ar area_code,
            common_ba brand,
            common_ch channel,
            common_is_new,
            common_md model,
            common_mid mid_id,
            common_os operate_system,
            common_uid user_id,
            common_vc version_code,
            page_during_time,
            page_item,
            page_item_type,
            page_last_page_id,
            page_page_id,
            page_source_type,
            json_displays_array_parser(`displays`).`display_type` as display_type,
            json_displays_array_parser(`displays`).`item` as display_item,
            json_displays_array_parser(`displays`).`item_type` as display_item_type,
            json_displays_array_parser(`displays`).`order` as display_order,
            json_displays_array_parser(`displays`).`pos_id` as display_pos_id,
            ts
        from  hudi_ods.ods_log_inc
    )log
        left join
    (
        select
            id province_id,
            area_code
        from hudi_ods.ods_base_province_full
    )bp
    on log.area_code=bp.area_code;
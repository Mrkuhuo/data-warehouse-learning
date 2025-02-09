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

CREATE TABLE IF NOT EXISTS hudi_dwd.dwd_trade_cart_add_full(
    `id`               BIGINT COMMENT '编号',
    `k1`               STRING COMMENT '分区字段',
    `user_id`          STRING COMMENT '用户id',
    `sku_id`           BIGINT COMMENT '商品id',
    `date_id`          STRING COMMENT '时间id',
    `create_time`      timestamp(3) COMMENT '加购时间',
    `source_id`        BIGINT COMMENT '来源类型ID',
    `source_type_code` STRING COMMENT '来源类型编码',
    `source_type_name` STRING COMMENT '来源类型名称',
    `sku_num`          BIGINT COMMENT '加购物车件数',
    PRIMARY KEY (`id`,`k1` ) NOT ENFORCED
    )   PARTITIONED BY (`k1` ) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
    );


INSERT INTO hudi_dwd.dwd_trade_cart_add_full(
    id,
    k1,
    user_id,
    sku_id,
    date_id,
    create_time,
    source_id,
    source_type_code,
    source_type_name,
    sku_num
    )
select
    id,
    date_format(create_time,'yyyy-MM-dd') as k1,
    user_id,
    sku_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time,
    source_id,
    source_type,
    dic.dic_name,
    sku_num
from
    (
        select
            id,
            user_id,
            sku_id,
            create_time,
            source_id,
            source_type,
            sku_num
        from hudi_ods.ods_cart_info_full
    )ci
        left join
    (
        select
            dic_code,
            dic_name
        from hudi_ods.ods_base_dic_full
        where parent_code='24'
    )dic
    on ci.source_type=dic.dic_code;
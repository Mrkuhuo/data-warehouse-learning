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

CREATE TABLE IF NOT EXISTS hudi_dwd.dwd_trade_order_refund_full(
    `id`                      BIGINT COMMENT '编号',
    `k1`                      STRING COMMENT '分区字段',
    `user_id`                 BIGINT COMMENT '用户ID',
    `order_id`                BIGINT COMMENT '订单ID',
    `sku_id`                  BIGINT COMMENT '商品ID',
    `province_id`             BIGINT COMMENT '地区ID',
    `date_id`                 STRING COMMENT '日期ID',
    `create_time`             TIMESTAMP(3) COMMENT '退单时间',
    `refund_type_code`        STRING COMMENT '退单类型编码',
    `refund_type_name`        STRING COMMENT '退单类型名称',
    `refund_reason_type_code` STRING COMMENT '退单原因类型编码',
    `refund_reason_type_name` STRING COMMENT '退单原因类型名称',
    `refund_reason_txt`       STRING COMMENT '退单原因描述',
    `refund_num`              BIGINT COMMENT '退单件数',
    `refund_amount`           DECIMAL(16, 2) COMMENT '退单金额',
    PRIMARY KEY (`id`,`k1` ) NOT ENFORCED
    )   PARTITIONED BY (`k1` ) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
    );


INSERT INTO hudi_dwd.dwd_trade_order_refund_full(
    id,
    k1,
    user_id,
    order_id,
    sku_id,
    province_id,
    date_id,
    create_time,
    refund_type_code,
    refund_type_name,
    refund_reason_type_code,
    refund_reason_type_name,
    refund_reason_txt,
    refund_num,
    refund_amount)
select
    ri.id,
    k1,
    user_id,
    order_id,
    sku_id,
    province_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time,
    refund_type,
    type_dic.dic_name,
    refund_reason_type,
    reason_dic.dic_name,
    refund_reason_txt,
    refund_num,
    refund_amount
from
    (
        select
            id,
            k1,
            user_id,
            order_id,
            sku_id,
            refund_type,
            refund_num,
            refund_amount,
            refund_reason_type,
            refund_reason_txt,
            create_time
        from hudi_ods.ods_order_refund_info_full
    )ri
        left join
    (
        select
            id,
            province_id
        from hudi_ods.ods_order_info_full
    )oi
    on ri.order_id=oi.id
        left join
    (
        select
            dic_code,
            dic_name
        from hudi_ods.ods_base_dic_full
        where parent_code = '15'
    )type_dic
    on ri.refund_type=type_dic.dic_code
        left join
    (
        select
            dic_code,
            dic_name
        from hudi_ods.ods_base_dic_full
        where  parent_code = '13'
    )reason_dic
    on ri.refund_reason_type=reason_dic.dic_code;
SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

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

CREATE TABLE IF NOT EXISTS dwd.dwd_trade_refund_pay_suc_full(
    `id`                BIGINT COMMENT '编号',
    `k1`                STRING COMMENT '分区字段',
    `user_id`           BIGINT COMMENT '用户ID',
    `order_id`          BIGINT COMMENT '订单编号',
    `sku_id`            BIGINT COMMENT 'SKU编号',
    `province_id`       BIGINT COMMENT '地区ID',
    `payment_type_code` STRING COMMENT '支付类型编码',
    `payment_type_name` STRING COMMENT '支付类型名称',
    `date_id`           STRING COMMENT '日期ID',
    `callback_time`     TIMESTAMP(3) COMMENT '支付成功时间',
    `refund_num`        DECIMAL(16, 2) COMMENT '退款件数',
    `refund_amount`     DECIMAL(16, 2) COMMENT '退款金额',
    PRIMARY KEY (`id`,`k1` ) NOT ENFORCED
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


INSERT INTO dwd.dwd_trade_refund_pay_suc_full(
    id,
    k1,
    user_id,
    order_id,
    sku_id,
    province_id,
    payment_type_code,
    payment_type_name,
    date_id,
    callback_time,
    refund_num,
    refund_amount
    )
select
    rp.id,
    k1,
    user_id,
    rp.order_id,
    rp.sku_id,
    province_id,
    payment_type,
    dic_name,
    date_format(callback_time,'yyyy-MM-dd') date_id,
    callback_time,
    refund_num,
    total_amount
from
    (
        select
            id,
            k1,
            order_id,
            sku_id,
            payment_type,
            callback_time,
            total_amount
        from ods.ods_refund_payment_full
        -- where refund_status='1602'
    )rp
        left join
    (
        select
            id,
            user_id,
            province_id
        from ods.ods_order_info_full
    )oi
    on rp.order_id=oi.id
        left join
    (
        select
            order_id,
            sku_id,
            refund_num
        from ods.ods_order_refund_info_full
    )ri
    on rp.order_id=ri.order_id
        and rp.sku_id=ri.sku_id
        left join
    (
        select
            dic_code,
            dic_name
        from ods.ods_base_dic_full
        where parent_code='11'
    )dic
    on rp.payment_type=dic.dic_code;
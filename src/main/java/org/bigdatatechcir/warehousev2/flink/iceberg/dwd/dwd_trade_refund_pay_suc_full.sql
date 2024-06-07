SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'execution.runtime-mode' = 'streaming';

CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);

use CATALOG iceberg_catalog;

create  DATABASE IF NOT EXISTS iceberg_dwd;

CREATE TABLE IF NOT EXISTS iceberg_dwd.dwd_trade_refund_pay_suc_full(
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
    'catalog-name'='hive_prod',
    'uri'='thrift://192.168.244.129:9083',
    'warehouse'='hdfs://192.168.244.129:9000/user/hive/warehouse/'
    );


INSERT INTO iceberg_dwd.dwd_trade_refund_pay_suc_full /*+ OPTIONS('upsert-enabled'='true') */(
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
        from iceberg_ods.ods_refund_payment_full /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/
        -- where refund_status='1602'
    )rp
        left join
    (
        select
            id,
            user_id,
            province_id
        from iceberg_ods.ods_order_info_full /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/
    )oi
    on rp.order_id=oi.id
        left join
    (
        select
            order_id,
            sku_id,
            refund_num
        from iceberg_ods.ods_order_refund_info_full /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/
    )ri
    on rp.order_id=ri.order_id
        and rp.sku_id=ri.sku_id
        left join
    (
        select
            dic_code,
            dic_name
        from iceberg_ods.ods_base_dic_full /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/
        where parent_code='11'
    )dic
    on rp.payment_type=dic.dic_code;
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

CREATE TABLE IF NOT EXISTS iceberg_dwd.dwd_trade_order_refund_full(
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
    'catalog-name'='hive_prod',
    'uri'='thrift://192.168.244.129:9083',
    'warehouse'='hdfs://192.168.244.129:9000/user/hive/warehouse/'
    );


INSERT INTO iceberg_dwd.dwd_trade_order_refund_full /*+ OPTIONS('upsert-enabled'='true') */(
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
        from iceberg_ods.ods_order_refund_info_full /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/
    )ri
        left join
    (
        select
            id,
            province_id
        from iceberg_ods.ods_order_info_full /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/
    )oi
    on ri.order_id=oi.id
        left join
    (
        select
            dic_code,
            dic_name
        from iceberg_ods.ods_base_dic_full /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/
        where parent_code = '15'
    )type_dic
    on ri.refund_type=type_dic.dic_code
        left join
    (
        select
            dic_code,
            dic_name
        from iceberg_ods.ods_base_dic_full /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/
        where  parent_code = '13'
    )reason_dic
    on ri.refund_reason_type=reason_dic.dic_code;
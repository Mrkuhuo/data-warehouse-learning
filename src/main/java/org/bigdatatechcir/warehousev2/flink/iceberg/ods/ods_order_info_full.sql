SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';

CREATE TABLE order_info_full_mq (
    `id` bigint NOT NULL  COMMENT '编号',
    `consignee` STRING  NULL COMMENT '收货人',
    `consignee_tel` STRING  NULL COMMENT '收件人电话',
    `total_amount` decimal(10,2)  NULL COMMENT '总金额',
    `order_status` STRING  NULL COMMENT '订单状态',
    `user_id` bigint  NULL COMMENT '用户id',
    `payment_way` STRING  NULL COMMENT '付款方式',
    `delivery_address` STRING  NULL COMMENT '送货地址',
    `order_comment` STRING  NULL COMMENT '订单备注',
    `out_trade_no` STRING  NULL COMMENT '订单交易编号（第三方支付用)',
    `trade_body` STRING  NULL COMMENT '订单描述(第三方支付用)',
    `create_time` timestamp(3) NOT NULL   COMMENT '创建时间',
    `operate_time` timestamp(3) NOT NULL   COMMENT '操作时间',
    `expire_time` timestamp(3)  NULL COMMENT '失效时间',
    `process_status` STRING  NULL COMMENT '进度状态',
    `tracking_no` STRING  NULL COMMENT '物流单编号',
    `parent_order_id` bigint  NULL COMMENT '父订单编号',
    `img_url` STRING  NULL COMMENT '图片路径',
    `province_id` int  NULL COMMENT '地区',
    `activity_reduce_amount` decimal(16,2)  NULL COMMENT '促销金额',
    `coupon_reduce_amount` decimal(16,2)  NULL COMMENT '优惠券',
    `original_total_amount` decimal(16,2)  NULL COMMENT '原价金额',
    `feight_fee` decimal(16,2)  NULL COMMENT '运费',
    `feight_fee_reduce` decimal(16,2)  NULL COMMENT '运费减免',
    `refundable_time` timestamp(3)  NULL COMMENT '可退款日期（签收后30天）',
     PRIMARY KEY(`id`) NOT ENFORCED
) WITH (
      'connector' = 'mysql-cdc',
      'scan.startup.mode' = 'earliest-offset',
      'hostname' = '192.168.244.129',
      'port' = '3306',
      'username' = 'root',
      'password' = '',
      'database-name' = 'gmall',
      'table-name' = 'order_info',
      'server-time-zone' = 'Asia/Shanghai'
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


CREATE TABLE IF NOT EXISTS iceberg_ods.ods_order_info_full(
    `id` bigint NOT NULL  COMMENT '购物券编号',
    `k1` STRING COMMENT '分区字段',
    `consignee` STRING  NULL COMMENT '收货人',
    `consignee_tel` STRING  NULL COMMENT '收件人电话',
    `total_amount` decimal(10,2)  NULL COMMENT '总金额',
    `order_status` STRING  NULL COMMENT '订单状态',
    `user_id` bigint  NULL COMMENT '用户id',
    `payment_way` STRING  NULL COMMENT '付款方式',
    `delivery_address` STRING  NULL COMMENT '送货地址',
    `order_comment` STRING  NULL COMMENT '订单备注',
    `out_trade_no` STRING  NULL COMMENT '订单交易编号（第三方支付用)',
    `trade_body` STRING  NULL COMMENT '订单描述(第三方支付用)',
    `create_time` timestamp(3) NOT NULL   COMMENT '创建时间',
    `operate_time` timestamp(3) NOT NULL   COMMENT '操作时间',
    `expire_time` timestamp(3)  NULL COMMENT '失效时间',
    `process_status` STRING  NULL COMMENT '进度状态',
    `tracking_no` STRING  NULL COMMENT '物流单编号',
    `parent_order_id` bigint  NULL COMMENT '父订单编号',
    `img_url` STRING  NULL COMMENT '图片路径',
    `province_id` int  NULL COMMENT '地区',
    `activity_reduce_amount` decimal(16,2)  NULL COMMENT '促销金额',
    `coupon_reduce_amount` decimal(16,2)  NULL COMMENT '优惠券',
    `original_total_amount` decimal(16,2)  NULL COMMENT '原价金额',
    `feight_fee` decimal(16,2)  NULL COMMENT '运费',
    `feight_fee_reduce` decimal(16,2)  NULL COMMENT '运费减免',
    `refundable_time` timestamp(3)  NULL COMMENT '可退款日期（签收后30天）',
    PRIMARY KEY (`id`,`k1` ) NOT ENFORCED
    )   PARTITIONED BY (`k1` ) WITH (
    'catalog-name'='hive_prod',
    'uri'='thrift://192.168.244.129:9083',
    'warehouse'='hdfs://192.168.244.129:9000/user/hive/warehouse/'
   );

INSERT INTO iceberg_ods.ods_order_info_full  /*+ OPTIONS('upsert-enabled'='true') */(
    `id`,
    `k1`,
    `consignee`,
    `consignee_tel`,
    `total_amount`,
    `order_status`,
    `user_id`,
    `payment_way`,
    `delivery_address`,
    `order_comment`,
    `out_trade_no`,
    `trade_body`,
    `create_time`,
    `operate_time`,
    `expire_time`,
    `process_status`,
    `tracking_no`,
    `parent_order_id`,
    `img_url`,
    `province_id`,
    `activity_reduce_amount`,
    `coupon_reduce_amount`,
    `original_total_amount`,
    `feight_fee`,
    `feight_fee_reduce`,
    `refundable_time`
)
select
    id,
    DATE_FORMAT(create_time, 'yyyy-MM-dd') AS k1,
    `consignee`,
    `consignee_tel`,
    `total_amount`,
    `order_status`,
    `user_id`,
    `payment_way`,
    `delivery_address`,
    `order_comment`,
    `out_trade_no`,
    `trade_body`,
    `create_time`,
    `operate_time`,
    `expire_time`,
    `process_status`,
    `tracking_no`,
    `parent_order_id`,
    `img_url`,
    `province_id`,
    `activity_reduce_amount`,
    `coupon_reduce_amount`,
    `original_total_amount`,
    `feight_fee`,
    `feight_fee_reduce`,
    `refundable_time`
from default_catalog.default_database.order_info_full_mq
where create_time is not null;
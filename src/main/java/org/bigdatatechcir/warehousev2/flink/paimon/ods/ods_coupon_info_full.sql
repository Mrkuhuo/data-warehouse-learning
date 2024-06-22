-- 优惠券领用表（增量表）
SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';

CREATE TABLE coupon_info_full_mq (
    `id` bigint NOT NULL  COMMENT '购物券编号',
    `coupon_name` STRING  NULL COMMENT '购物券名称',
    `coupon_type` STRING NULL COMMENT '购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券',
    `condition_amount` decimal(10,2)  NULL COMMENT '满额数（3）',
    `condition_num` bigint  NULL COMMENT '满件数（4）',
    `activity_id` bigint  NULL COMMENT '活动编号',
    `benefit_amount` decimal(16,2)  NULL COMMENT '减金额（1 3）',
    `benefit_discount` decimal(10,2)  NULL COMMENT '折扣（2 4）',
    `create_time` timestamp(3) NOT NULL   COMMENT '创建时间',
    `range_type` STRING  NULL COMMENT '范围类型 1、商品(spuid) 2、品类(三级分类id) 3、品牌',
    `limit_num` int NOT NULL  COMMENT '最多领用次数',
    `taken_count` int NOT NULL  COMMENT '已领用次数',
    `start_time` timestamp(3)  NULL COMMENT '可以领取的开始日期',
    `end_time` timestamp(3)  NULL COMMENT '可以领取的结束日期',
    `operate_time` timestamp(3) NOT NULL  COMMENT '修改时间',
    `expire_time` timestamp(3)  NULL COMMENT '过期时间',
    `range_desc` STRING NULL COMMENT '范围描述',
    PRIMARY KEY(`id`) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'scan.startup.mode' = 'earliest-offset',
    'hostname' = '192.168.244.129',
    'port' = '3306',
    'username' = 'root',
    'password' = '',
    'database-name' = 'gmall',
    'table-name' = 'coupon_info',
    'server-time-zone' = 'Asia/Shanghai'
);

CREATE CATALOG paimon_hive WITH (
    'type' = 'paimon',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);

use CATALOG paimon_hive;

create  DATABASE IF NOT EXISTS ods;

CREATE TABLE IF NOT EXISTS ods.ods_coupon_info_full(
    `id` bigint NOT NULL  COMMENT '购物券编号',
    `k1` STRING COMMENT '分区字段',
    `coupon_name` STRING  NULL COMMENT '购物券名称',
    `coupon_type` STRING NULL COMMENT '购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券',
    `condition_amount` decimal(10,2)  NULL COMMENT '满额数（3）',
    `condition_num` bigint  NULL COMMENT '满件数（4）',
    `activity_id` bigint  NULL COMMENT '活动编号',
    `benefit_amount` decimal(16,2)  NULL COMMENT '减金额（1 3）',
    `benefit_discount` decimal(10,2)  NULL COMMENT '折扣（2 4）',
    `create_time` timestamp(3) NOT NULL   COMMENT '创建时间',
    `range_type` STRING  NULL COMMENT '范围类型 1、商品(spuid) 2、品类(三级分类id) 3、品牌',
    `limit_num` int NOT NULL  COMMENT '最多领用次数',
    `taken_count` int NOT NULL  COMMENT '已领用次数',
    `start_time` timestamp(3)  NULL COMMENT '可以领取的开始日期',
    `end_time` timestamp(3)  NULL COMMENT '可以领取的结束日期',
    `operate_time` timestamp(3) NOT NULL  COMMENT '修改时间',
    `expire_time` timestamp(3)  NULL COMMENT '过期时间',
    `range_desc` STRING NULL COMMENT '范围描述',
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

INSERT INTO ods.ods_coupon_info_full(
    `id`,
    `k1`,
    `coupon_name`,
    `coupon_type`,
    `condition_amount`,
    `condition_num`,
    `activity_id`,
    `benefit_amount`,
    `benefit_discount`,
    `create_time`,
    `range_type`,
    `limit_num`,
    `taken_count`,
    `start_time`,
    `end_time`,
    `operate_time`,
    `expire_time`,
    `range_desc`
)
select
    id,
    DATE_FORMAT(create_time, 'yyyy-MM-dd') AS k1,
    `coupon_name`,
    `coupon_type`,
    `condition_amount`,
    `condition_num`,
    `activity_id`,
    `benefit_amount`,
    `benefit_discount`,
    `create_time`,
    `range_type`,
    `limit_num`,
    `taken_count`,
    `start_time`,
    `end_time`,
    `operate_time`,
    `expire_time`,
    `range_desc`
from default_catalog.default_database.coupon_info_full_mq
where create_time is not null;

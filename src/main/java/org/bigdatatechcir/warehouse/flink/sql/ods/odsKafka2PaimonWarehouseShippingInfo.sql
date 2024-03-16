-- 从kafka 中获取数据
CREATE TABLE IF NOT EXISTS warehouse_shipping_info(
     `ship_id` BIGINT NULL comment '主键ID',
     `ship_name` VARCHAR NULL comment '物流公司名称',
     `ship_contact` VARCHAR NULL comment '物流公司联系人',
     `telephone` VARCHAR NULL comment '物流公司联系电话',
     `price` FLOAT NULL comment '配送价格',
     `event_time` TIMESTAMP NULL comment '最后修改时间'
) WITH (
    'connector' = 'kafka',
    'topic' = 'warehouse_shipping_info',
    'properties.bootstrap.servers' = '192.168.154.131:9092',
    'properties.group.id' = 'warehouse_shipping_info',
    'scan.startup.mode' = 'group-offsets',
    'properties.auto.offset.reset' = 'earliest',
    'properties.enable.auto.commit'='true',
    'properties.auto.commit.interval.ms'='5000',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true',
    'json.fail-on-missing-field' = 'false'
);

-- 创建CATALOG
CREATE CATALOG catalog_paimon WITH (
    'type'='paimon',
    'warehouse'='file:/opt/software/paimon_catelog'
);

-- 切换CATALOG
USE CATALOG catalog_paimon;

-- 创建database
create  DATABASE IF NOT EXISTS ods;

-- 切换database
use ods;

-- 创建paimon表
CREATE  TABLE IF NOT EXISTS ods.ods_warehouse_shipping_info (
    `ship_id` BIGINT,
    `ship_name` STRING,
    `ship_contact` STRING,
    `telephone` STRING,
    `price` FLOAT,
    `event_time` TIMESTAMP
);

-- 是指checkpoint时间
SET 'execution.checkpointing.interval' = '10 s';

-- sql逻辑代码
insert into ods_warehouse_shipping_info select * from default_catalog.default_database.warehouse_shipping_info;
-- 从kafka 中获取数据
CREATE TABLE IF NOT EXISTS product_supplier_info(
     `supplier_id` BIGINT NULL comment '供应商ID',
     `supplier_code` VARCHAR NULL comment '供应商编码',
     `supplier_name` VARCHAR NULL comment '供应商名称',
     `supplier_type` INT NULL comment '供应商类型：1.自营，2.平台',
     `link_man` VARCHAR NULL comment '供应商联系人',
     `phone_number` VARCHAR NULL comment '联系电话',
     `bank_name` VARCHAR NULL comment '供应商开户银行名称',
     `bank_account` VARCHAR NULL comment '银行账号',
     `address` VARCHAR NULL comment '供应商地址',
     `supplier_status` INT NULL comment '状态：0禁止，1启用',
     `event_time` TIMESTAMP NULL comment '最后修改时间'
) WITH (
    'connector' = 'kafka',
    'topic' = 'product_supplier_info',
    'properties.bootstrap.servers' = '192.168.154.131:9092',
    'properties.group.id' = 'product_supplier_info',
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
CREATE  TABLE IF NOT EXISTS ods.ods_product_supplier_info (
     `supplier_id` BIGINT,
     `supplier_code` VARCHAR,
     `supplier_name` VARCHAR,
     `supplier_type` INT,
     `link_man` VARCHAR,
     `phone_number` VARCHAR,
     `bank_name` VARCHAR,
     `bank_account` VARCHAR,
     `address` VARCHAR,
     `supplier_status` INT,
     `event_time` TIMESTAMP
);

-- 是指checkpoint时间
SET 'execution.checkpointing.interval' = '10 s';

-- sql逻辑代码
insert into ods_product_supplier_info select * from default_catalog.default_database.product_supplier_info;
-- 从kafka 中获取数据
CREATE TABLE IF NOT EXISTS product_brand_info(
     `brand_id` BIGINT NULL comment '品牌ID',
     `brand_name` VARCHAR NULL comment '品牌名称',
     `telephone` VARCHAR NULL comment '联系电话',
     `brand_web` VARCHAR NULL comment '品牌网络',
     `brand_logo` VARCHAR NULL comment '品牌logo',
     `brand_desc` VARCHAR NULL comment '品牌描述',
     `brand_status` INT NULL comment '品牌状态,0禁用,1启用',
     `brand_order` INT NULL comment '排序',
     `event_time` TIMESTAMP NULL comment '最后修改时间'
) WITH (
    'connector' = 'kafka',
    'topic' = 'product_brand_info',
    'properties.bootstrap.servers' = '192.168.154.131:9092',
    'properties.group.id' = 'product_brand_info',
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
CREATE  TABLE IF NOT EXISTS ods.ods_product_brand_info (
     `brand_id` BIGINT,
     `brand_name` VARCHAR,
     `telephone` VARCHAR,
     `brand_web` VARCHAR,
     `brand_logo` VARCHAR,
     `brand_desc` VARCHAR,
     `brand_status` INT,
     `brand_order` INT,
     `event_time` TIMESTAMP
);

-- 是指checkpoint时间
SET 'execution.checkpointing.interval' = '10 s';

-- sql逻辑代码
insert into ods_product_brand_info select * from default_catalog.default_database.product_brand_info;
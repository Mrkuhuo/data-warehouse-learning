-- 从kafka 中获取数据
CREATE TABLE IF NOT EXISTS generate_customer_addr(
     `customer_addr_id` BIGINT NULL comment '自增主键ID',
     `zip` VARCHAR NULL comment '邮编',
     `province` VARCHAR NULL comment '省份',
     `city` VARCHAR NULL comment '城市',
     `district` VARCHAR NULL comment '区/县',
     `address` VARCHAR NULL comment '具体的地址门牌号',
     `is_default` INT NULL comment '是否默认',
     `event_time` TIMESTAMP NULL comment '数据生产时间',
     `customer_id` bigint NULL comment 'customer_login表的自增ID'
) WITH (
    'connector' = 'kafka',
    'topic' = 'generate_customer_addr',
    'properties.bootstrap.servers' = '192.168.154.131:9092',
    'properties.group.id' = 'generate_customer_addr',
    'scan.startup.mode' = 'group-offsets',
    'properties.auto.offset.reset' = 'earliest',
    'properties.enable.auto.commit'='true',
    'properties.auto.commit.interval.ms'='5000',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true',
    'json.fail-on-missing-field' = 'false'
);

-- 创建CATALOG
CREATE CATALOG my_catalog_ods WITH (
    'type'='paimon',
    'warehouse'='file:/opt/software/paimon_catelog'
);

-- 切换CATALOG
USE CATALOG my_catalog_ods;

-- 创建database
create  DATABASE IF NOT EXISTS ods;

-- 切换database
use ods;

-- 创建paimon表
CREATE  TABLE IF NOT EXISTS ods.ods_generate_customer_addr (
    customer_addr_id BIGINT,
    zip STRING,
	province STRING,
	city STRING,
    district STRING,
    address STRING,
    is_default int,
    event_time TIMESTAMP,
	customer_id BIGINT
);

-- 是指checkpoint时间
SET 'execution.checkpointing.interval' = '10 s';

-- sql逻辑代码
insert into ods_generate_customer_addr select * from default_catalog.default_database.generate_customer_addr;
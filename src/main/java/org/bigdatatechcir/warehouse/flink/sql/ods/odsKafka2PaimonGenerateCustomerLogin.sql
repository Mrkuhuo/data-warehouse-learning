-- 从kafka 中获取数据
CREATE TABLE IF NOT EXISTS generate_customer_login(
     `login_name` VARCHAR NULL comment '登录名称',
     `password` VARCHAR NULL comment '用户密码',
     `user_stats` bigint NULL comment '用户状态',
     `event_time` TIMESTAMP NULL comment '数据产生时间',
     `customer_id` bigint NULL comment '用户ID'
) WITH (
    'connector' = 'kafka',
    'topic' = 'generate_customer_login',
    'properties.bootstrap.servers' = '192.168.154.131:9092',
    'properties.group.id' = 'generate_customer_login',
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
CREATE  TABLE IF NOT EXISTS ods.ods_generate_customer_login (
    login_name STRING,
    password STRING,
	user_stats BIGINT,
	event_time TIMESTAMP,
	customer_id BIGINT
);

-- 是指checkpoint时间
SET 'execution.checkpointing.interval' = '10 s';

-- sql逻辑代码
insert into ods_generate_customer_login select * from default_catalog.default_database.generate_customer_login;
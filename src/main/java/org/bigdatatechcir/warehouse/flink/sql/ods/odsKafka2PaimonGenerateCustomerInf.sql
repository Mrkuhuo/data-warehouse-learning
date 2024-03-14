-- 从kafka 中获取数据
CREATE TABLE IF NOT EXISTS generate_customer_inf(
     `customer_inf_id` BIGINT NULL comment '自增主键ID',
     `customer_name` VARCHAR NULL comment '用户真实姓名',
     `identity_card_type` INT NULL comment '证件类型：1 身份证，2 军官证，3 护照',
     `identity_card_no` VARCHAR NULL comment '证件号码',
     `mobile_phone` VARCHAR NULL comment '手机号',
     `customer_email` VARCHAR NULL comment '邮箱',
     `gender` VARCHAR NULL comment '性别',
     `user_point` INT NULL comment '用户积分',
     `register_time` TIMESTAMP NULL comment '注册时间',
     `birthday` TIMESTAMP NULL comment '会员生日',
     `customer_level` INT NULL comment '会员级别：1 普通会员，2 青铜，3白银，4黄金，5钻石',
     `user_money` INT NULL comment '用户余额',
     `event_time` TIMESTAMP NULL comment '最后修改时间',
     `customer_id` bigint NULL comment 'customer_login表的自增ID'
) WITH (
    'connector' = 'kafka',
    'topic' = 'generate_customer_inf',
    'properties.bootstrap.servers' = '192.168.154.131:9092',
    'properties.group.id' = 'generate_customer_inf',
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
CREATE  TABLE IF NOT EXISTS ods.ods_generate_customer_inf (
     `customer_inf_id` BIGINT,
     `customer_name` STRING,
     `identity_card_type` INT,
     `identity_card_no` STRING,
     `mobile_phone` STRING,
     `customer_email` STRING,
     `gender` STRING,
     `user_point` INT,
     `register_time` TIMESTAMP,
     `birthday` TIMESTAMP,
     `customer_level` INT,
     `user_money` INT,
     `event_time` TIMESTAMP,
     `customer_id` bigint
);

-- 是指checkpoint时间
SET 'execution.checkpointing.interval' = '10 s';

-- sql逻辑代码
insert into ods_generate_customer_inf select * from default_catalog.default_database.generate_customer_inf;
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
CREATE  TABLE IF NOT EXISTS ods.ods_customer_inf (
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

-- 创建database
create  DATABASE IF NOT EXISTS dwd;

-- 切换database
use dwd;

-- 创建paimon表
CREATE  TABLE IF NOT EXISTS dwd.dwd_customer_inf (
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
INSERT INTO dwd.dwd_customer_inf
SELECT
  customer_inf_id, -- 保留原始值，无需清洗
  TRIM(customer_name) AS customer_name, -- 清理客户姓名，去除两侧空格
  identity_card_type, -- 身份证类型和性别字段直接保留原始值，后续可能根据业务需求验证或转换
  TRIM(identity_card_no) AS identity_card_no, -- 清理身份证号和手机号码，去除两侧空格
  TRIM(mobile_phone) AS mobile_phone,
  TRIM(customer_email) AS customer_email, -- 清理电子邮件地址，去除两侧空格
  gender,
  user_point, -- 直接保留积分和用户等级字段的原始值
  register_time, -- 确保register_time和birthday是合法的TIMESTAMP格式，如有必要，进行转换
  birthday,
  customer_level,
  CAST(ROUND(user_money, 2) AS INT) AS user_money, -- 转换金额为整数（假设user_money是以小数形式存储的）
  event_time, -- 保留event_time字段原始值，如有必要，也可进行相同类型的转换
  customer_id -- 保留原始值，无需清洗
FROM
  ods.ods_customer_inf;
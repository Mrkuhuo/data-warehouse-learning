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
     customer_inf_id BIGINT,
     customer_name STRING,
     identity_card_type INT,
     identity_card_no STRING,
     mobile_phone STRING,
     customer_email STRING,
     gender STRING,
     user_point INT,
     register_time TIMESTAMP,
     birthday TIMESTAMP,
     customer_level INT,
     user_money INT,
     event_time TIMESTAMP,
     customer_id bigint
);

-- 创建database
create  DATABASE IF NOT EXISTS dwd;

-- 切换database
use dwd;

-- DROP TABLE IF EXISTS dwd.dwd_customer_inf ;

-- 创建paimon表
CREATE  TABLE IF NOT EXISTS dwd.dwd_customer_inf (
     customer_inf_id BIGINT,
     customer_name STRING,
     identity_card_type INT,
     identity_card_no STRING,
     mobile_phone STRING,
     customer_email STRING,
     gender STRING,
     user_point INT,
     register_time TIMESTAMP,
     birthday TIMESTAMP,
     customer_level INT,
     user_money INT,
     event_time STRING,
     customer_id bigint
);

-- 是指checkpoint时间
SET 'execution.checkpointing.interval' = '10 s';

-- sql逻辑代码
INSERT INTO dwd.dwd_customer_inf
SELECT
    -- 保留原始值，无需清洗
    customer_inf_id, 
    -- 清理客户姓名，去除两侧空格
    TRIM(customer_name) AS customer_name, 
    -- 身份证类型和性别字段直接保留原始值，后续可能根据业务需求验证或转换
    identity_card_type, 
    -- 清理身份证号和手机号码，去除两侧空格
    TRIM(identity_card_no) AS identity_card_no, 
    TRIM(mobile_phone) AS mobile_phone,
     -- 清理电子邮件地址，去除两侧空格
    TRIM(customer_email) AS customer_email,
    gender,
    -- 直接保留积分和用户等级字段的原始值
    user_point, 
    -- 确保register_time和birthday是合法的TIMESTAMP格式，如有必要，进行转换
    register_time, 
    birthday,
    customer_level,
    -- 转换金额为整数（假设user_money是以小数形式存储的）
    CAST(ROUND(user_money, 2) AS INT) AS user_money, 
    -- 保留event_time字段原始值，如有必要，也可进行相同类型的转换
    DATE_FORMAT(event_time, 'yyyy-MM-dd'), 
    -- 保留原始值，无需清洗
    customer_id 
FROM
  ods.ods_customer_inf;
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

-- 创建paimon ods表
CREATE  TABLE IF NOT EXISTS ods.ods_customer_login (
    login_name STRING,
    password STRING,
	user_stats BIGINT,
	event_time TIMESTAMP,
	customer_id BIGINT
);

-- 创建database
create  DATABASE IF NOT EXISTS dwd;

use dwd;

-- DROP TABLE IF EXISTS dwd.dwd_customer_login ;
-- 创建paimon dwd表
CREATE  TABLE IF NOT EXISTS dwd.dwd_customer_login (
    login_name STRING,
    password_hash STRING,
	user_stats BIGINT,
	event_time STRING,
	customer_id BIGINT
);

-- 是指checkpoint时间
SET 'execution.checkpointing.interval' = '10 s';

-- sql逻辑代码
INSERT INTO dwd.dwd_customer_login
SELECT
    -- 清理登录名，去除两侧空格
    TRIM(login_name) AS login_name,
    -- 清理密码，去除两侧空格
    TRIM(password) AS password,
    -- 处理user_stats字段，将NULL值转化为0
    COALESCE(user_stats, 0) AS user_stats,
    DATE_FORMAT(event_time, 'yyyy-MM-dd'),
    -- 清洗customer_id，只保留合法的大于0的整数值
    CASE
        WHEN customer_id > 0 THEN customer_id
        ELSE NULL
    END AS customer_id
FROM
  ods.ods_customer_login;
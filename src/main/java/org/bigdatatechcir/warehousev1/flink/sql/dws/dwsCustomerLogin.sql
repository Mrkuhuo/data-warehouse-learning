-- 创建CATALOG
CREATE CATALOG catalog_paimon WITH (
    'type'='paimon',
    'warehouse'='file:/opt/software/paimon_catelog'
);

-- 切换CATALOG
USE CATALOG catalog_paimon;
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

-- 创建database
create  DATABASE IF NOT EXISTS dws;

use dws;

-- DROP TABLE IF EXISTS dws.dws_customer_login ;
-- 创建paimon dwd表
CREATE  TABLE IF NOT EXISTS dws.dws_customer_login (
    login_name STRING,
    password_hash STRING,
	user_stats BIGINT,
	event_time STRING,
	customer_id BIGINT
);

-- 是指checkpoint时间
SET 'execution.checkpointing.interval' = '10 s';

INSERT INTO dws.dws_customer_login
SELECT
    login_name,
    password_hash,
	user_stats,
	event_time,
	customer_id
FROM
	dwd.dwd_customer_login;
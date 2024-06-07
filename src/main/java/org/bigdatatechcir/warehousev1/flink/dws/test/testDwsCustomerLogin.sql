-- ./sql-client.sh

-- 创建CATALOG
CREATE CATALOG catalog_paimon WITH (
    'type'='paimon',
    'warehouse'='file:/opt/software/paimon_catelog'
);

-- 切换CATALOG
USE CATALOG catalog_paimon;

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

-- 批量读取数据
SET 'sql-client.execution.result-mode' = 'tableau';

SET 'execution.runtime-mode' = 'batch';

SELECT * FROM dws.dws_customer_login;

-- 流式读取数据

SET 'execution.runtime-mode' = 'streaming';

SELECT * FROM dws.dws_customer_login;

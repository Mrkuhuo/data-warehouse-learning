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

-- 切换database
use dws;

-- DROP TABLE IF EXISTS dws.dws_customer_inf ;

-- 创建paimon表
CREATE  TABLE IF NOT EXISTS dws.dws_customer_inf (
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

-- 批量读取数据
SET 'sql-client.execution.result-mode' = 'tableau';

SET 'execution.runtime-mode' = 'batch';

SELECT * FROM dws.dws_customer_inf;

-- 流式读取数据

SET 'execution.runtime-mode' = 'streaming';

SELECT * FROM dws.dws_customer_inf;


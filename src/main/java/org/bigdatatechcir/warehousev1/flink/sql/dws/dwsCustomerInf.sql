-- 创建CATALOG
CREATE CATALOG catalog_paimon WITH (
    'type'='paimon',
    'warehouse'='file:/opt/software/paimon_catelog'
);

-- 切换CATALOG
USE CATALOG catalog_paimon;

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


-- 是指checkpoint时间
SET 'execution.checkpointing.interval' = '10 s';

INSERT INTO dws.dws_customer_inf
SELECT
     customer_inf_id ,
     customer_name ,
     identity_card_type ,
     identity_card_no ,
     mobile_phone ,
     customer_email ,
     gender ,
     user_point ,
     register_time ,
     birthday ,
     customer_level ,
     user_money ,
     event_time ,
     customer_id
FROM
	dwd.dwd_customer_inf;
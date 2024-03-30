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

-- DROP TABLE IF EXISTS dwd.dwd_order_master ;

-- 创建paimon表
CREATE  TABLE IF NOT EXISTS dwd.dwd_order_master (
     order_id BIGINT,
     order_sn BIGINT,
     payment_method int,
     order_money FLOAT,
     district_money FLOAT,
     shipping_money FLOAT,
     payment_money FLOAT,
     shipping_sn VARCHAR,
     create_time TIMESTAMP,
     shipping_time TIMESTAMP,
     pay_time TIMESTAMP,
     receive_time TIMESTAMP,
     order_status INT,
     order_point INT,
     event_time STRING,
     customer_id BIGINT,
     shipping_comp_name BIGINT,
     product_id BIGINT
);

-- 创建database
create  DATABASE IF NOT EXISTS dws;

-- 切换database
use dws;

-- DROP TABLE IF EXISTS dws.dws_order_master ;

-- 创建paimon表
CREATE  TABLE IF NOT EXISTS dws.dws_order_master (
     customer_id BIGINT,
     modified_day STRING,
     order_sum FLOAT,
     primary key(customer_id, modified_day)  NOT ENFORCED
);

-- 是指checkpoint时间
SET 'execution.checkpointing.interval' = '10 s';

INSERT INTO dws.dws_order_master
SELECT
    customer_id,
    event_time,
    SUM(order_money) as order_sum
FROM
	dwd.dwd_order_master
    GROUP BY customer_id, event_time;
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

-- DROP Table dws.dws_customer_addr;

-- 创建paimon dws.dws_customer_addr表
CREATE  TABLE IF NOT EXISTS dws.dws_customer_addr (
    customer_id BIGINT ,
    event_day STRING,
	address_count BIGINT
);

-- 创建paimon dws.dws_customer_inf表
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

-- 创建paimon dws.dws_customer_login 表
CREATE  TABLE IF NOT EXISTS dws.dws_customer_login (
    login_name STRING,
    password_hash STRING,
	user_stats BIGINT,
	event_time STRING,
	customer_id BIGINT
);

-- 创建paimondws.dws_customer_login_log表
CREATE  TABLE IF NOT EXISTS dws.dws_customer_login_log (
     customer_id BIGINT NULL comment '登陆日志ID',
     login_day STRING NULL comment '用户登陆天',
     login_count BIGINT NULL comment '登陆次数'
);

-- 创建paimon dws.dws_order_cart表
CREATE  TABLE IF NOT EXISTS dws.dws_order_cart (
     customer_id BIGINT NULL comment '用户ID',
     modified_day STRING NULL comment '添加购物车时间',
     cart_count BIGINT NULL comment '购物车数量'
);

-- 创建paimon dws.dws_order_master表
CREATE  TABLE IF NOT EXISTS dws.dws_order_master (
     customer_id BIGINT,
     modified_day STRING,
     order_sum FLOAT
);

-- 创建database
create  DATABASE IF NOT EXISTS ads;

use ads;

-- 创建paimon表
CREATE TABLE IF NOT EXISTS ads.ads_customer (
     customer_id BIGINT,
     customer_name STRING,
     address_count BIGINT,
     login_count BIGINT,
     cart_count BIGINT,
     order_sum FLOAT,
     modified_day STRING,
     primary key(customer_id, modified_day)  NOT ENFORCED
);


-- 是指checkpoint时间
SET 'execution.checkpointing.interval' = '10 s';

set 'table.exec.sink.upsert-materialize' = 'NONE';

INSERT INTO
	ads.ads_customer ( customer_id, customer_name, address_count, login_count, cart_count, order_sum, modified_day )
SELECT
	login.customer_id,
	inf.customer_name,
	addr.address_count,
	log.login_count,
	cart.cart_count,
	master.order_sum,
	login.event_time
FROM
	dws.dws_customer_login login
LEFT JOIN dws.dws_customer_addr addr ON
	login.customer_id = addr.customer_id
LEFT JOIN dws.dws_customer_login_log log ON
	login.customer_id = log.customer_id
LEFT JOIN dws.dws_customer_inf inf ON
	login.customer_id = inf.customer_id
LEFT JOIN dws.dws_order_cart cart ON
	login.customer_id = cart.customer_id
LEFT JOIN dws.dws_order_master master ON
	login.customer_id = master.customer_id
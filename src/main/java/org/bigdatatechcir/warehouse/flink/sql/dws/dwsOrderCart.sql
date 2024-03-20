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

-- DROP TABLE IF EXISTS dwd.dwd_order_cart ;

-- 创建paimon表
CREATE  TABLE IF NOT EXISTS dwd.dwd_order_cart (
     cart_id BIGINT NULL comment '购物车ID',
     product_amount BIGINT NULL comment '加入购物车商品数量',
     price FLOAT NULL comment '商品价格',
     add_time TIMESTAMP NULL comment '加入购物车时间',
     event_time STRING NULL comment '最后修改时间',
     customer_id BIGINT NULL comment '用户ID',
     product_id BIGINT NULL comment '商品ID'
);

-- 创建database
create  DATABASE IF NOT EXISTS dws;

-- 切换database
use dws;

-- DROP TABLE IF EXISTS dws.dws_order_cart ;

-- 创建paimon表
CREATE  TABLE IF NOT EXISTS dws.dws_order_cart (
     customer_id BIGINT NULL comment '用户ID',
     modified_day STRING NULL comment '添加购物车时间',
     cart_count BIGINT NULL comment '购物车数量',
     primary key(customer_id, modified_day)  NOT ENFORCED
);

-- 是指checkpoint时间
SET 'execution.checkpointing.interval' = '10 s';

INSERT INTO dws.dws_order_cart
SELECT
    customer_id,
    event_time,
    COUNT(cart_id) as login_count
FROM
	dwd.dwd_order_cart
    GROUP BY customer_id, event_time;
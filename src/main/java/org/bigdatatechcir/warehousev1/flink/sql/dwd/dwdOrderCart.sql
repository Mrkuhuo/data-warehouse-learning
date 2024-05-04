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
CREATE  TABLE IF NOT EXISTS ods.ods_order_cart (
     cart_id BIGINT NULL comment '购物车ID',
     product_amount BIGINT NULL comment '加入购物车商品数量',
     price FLOAT NULL comment '商品价格',
     add_time TIMESTAMP NULL comment '加入购物车时间',
     event_time TIMESTAMP NULL comment '最后修改时间',
     customer_id BIGINT NULL comment '用户ID',
     product_id BIGINT NULL comment '商品ID'
);

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

-- 是指checkpoint时间
SET 'execution.checkpointing.interval' = '10 s';

-- sql逻辑代码
INSERT INTO dwd.dwd_order_cart
SELECT
    -- 保留原始值，无需清洗
    cart_id, 
    -- 直接保留商品数量字段的原始值，后续可能根据业务需求验证或转换
    product_amount, 
    -- 转换价格为浮点数（假设price是以字符串形式存储的
    TRY_CAST(price AS FLOAT) AS price, 
    -- 确保add_time是合法的TIMESTAMP格式，如有必要，进行转换
    add_time, 
    -- 确保event_time是合法的TIMESTAMP格式，如有必要，进行转换
    DATE_FORMAT(event_time, 'yyyy-MM-dd'), 
    -- 保留原始值，无需清洗
    customer_id, 
    product_id

FROM
    ods.ods_order_cart;
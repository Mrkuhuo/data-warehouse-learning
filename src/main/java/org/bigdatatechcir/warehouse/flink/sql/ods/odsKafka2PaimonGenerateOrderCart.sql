-- 从kafka 中获取数据
CREATE TABLE IF NOT EXISTS generate_order_cart(
     `cart_id` BIGINT NULL comment '购物车ID',
     `product_amount` BIGINT NULL comment '加入购物车商品数量',
     `price` FLOAT NULL comment '商品价格',
     `add_time` TIMESTAMP NULL comment '加入购物车时间',
     `event_time` TIMESTAMP NULL comment '最后修改时间',
     `customer_id` BIGINT NULL comment '用户ID',
     `product_id` BIGINT NULL comment '商品ID'
) WITH (
    'connector' = 'kafka',
    'topic' = 'generate_order_cart',
    'properties.bootstrap.servers' = '192.168.154.131:9092',
    'properties.group.id' = 'generate_order_cart',
    'scan.startup.mode' = 'group-offsets',
    'properties.auto.offset.reset' = 'earliest',
    'properties.enable.auto.commit'='true',
    'properties.auto.commit.interval.ms'='5000',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true',
    'json.fail-on-missing-field' = 'false'
);

-- 创建CATALOG
CREATE CATALOG my_catalog_ods WITH (
    'type'='paimon',
    'warehouse'='file:/opt/software/paimon_catelog'
);

-- 切换CATALOG
USE CATALOG my_catalog_ods;

-- 创建database
create  DATABASE IF NOT EXISTS ods;

-- 切换database
use ods;

-- 创建paimon表
CREATE  TABLE IF NOT EXISTS ods.ods_generate_order_cart (
     `cart_id` BIGINT NULL comment '购物车ID',
     `product_amount` BIGINT NULL comment '加入购物车商品数量',
     `price` FLOAT NULL comment '商品价格',
     `add_time` TIMESTAMP NULL comment '加入购物车时间',
     `event_time` TIMESTAMP NULL comment '最后修改时间',
     `customer_id` BIGINT NULL comment '用户ID',
     `product_id` BIGINT NULL comment '商品ID'
);

-- 是指checkpoint时间
SET 'execution.checkpointing.interval' = '10 s';

-- sql逻辑代码
insert into ods_generate_order_cart select * from default_catalog.default_database.generate_order_cart;
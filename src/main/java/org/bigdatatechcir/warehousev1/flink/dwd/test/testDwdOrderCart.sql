-- ./sql-client.sh

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

-- 创建paimon表
CREATE  TABLE IF NOT EXISTS dwd.dwd_order_cart (
     `cart_id` BIGINT NULL comment '购物车ID',
     `product_amount` BIGINT NULL comment '加入购物车商品数量',
     `price` FLOAT NULL comment '商品价格',
     `add_time` TIMESTAMP NULL comment '加入购物车时间',
     `event_time` STRING NULL comment '最后修改时间',
     `customer_id` BIGINT NULL comment '用户ID',
     `product_id` BIGINT NULL comment '商品ID'
);

-- 批量读取数据
SET 'sql-client.execution.result-mode' = 'tableau';

SET 'execution.runtime-mode' = 'batch';

SELECT * FROM dwd.dwd_order_cart;

-- 流式读取数据

SET 'execution.runtime-mode' = 'streaming';

SELECT * FROM dwd.dwd_order_cart;
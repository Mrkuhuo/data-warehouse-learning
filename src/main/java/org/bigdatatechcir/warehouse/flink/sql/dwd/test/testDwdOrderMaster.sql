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
CREATE  TABLE IF NOT EXISTS dwd.dwd_order_master (
     `order_id` BIGINT,
     `order_sn` BIGINT,
     `payment_method` int,
     `order_money` FLOAT,
     `district_money` FLOAT,
     `shipping_money` FLOAT,
     `payment_money` FLOAT,
     `shipping_sn` VARCHAR,
     `create_time` TIMESTAMP,
     `shipping_time` TIMESTAMP,
     `pay_time` TIMESTAMP,
     `receive_time` TIMESTAMP,
     `order_status` INT,
     `order_point` INT,
     `event_time` TIMESTAMP,
     `customer_id` BIGINT,
     `shipping_comp_name` BIGINT
);

-- 批量读取数据
SET 'sql-client.execution.result-mode' = 'tableau';

SET 'execution.runtime-mode' = 'batch';

SELECT * FROM dwd.dwd_order_master;

-- 流式读取数据

SET 'execution.runtime-mode' = 'streaming';

SELECT * FROM dwd.dwd_order_master;


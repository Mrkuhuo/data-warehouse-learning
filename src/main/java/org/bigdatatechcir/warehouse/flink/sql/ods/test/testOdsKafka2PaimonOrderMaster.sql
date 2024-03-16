-- ./sql-client.sh

-- 执行以下代码

CREATE CATALOG catalog_paimon WITH (
    'type'='paimon',
    'warehouse'='file:/opt/software/paimon_catelog'
);

USE CATALOG catalog_paimon;

create  DATABASE IF NOT EXISTS ods;

use ods;

-- 创建paimon表
CREATE  TABLE IF NOT EXISTS ods.ods_order_master (
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

SELECT * FROM ods_order_master;

-- 流式读取数据

SET 'execution.runtime-mode' = 'streaming';

SELECT * FROM ods_order_master;
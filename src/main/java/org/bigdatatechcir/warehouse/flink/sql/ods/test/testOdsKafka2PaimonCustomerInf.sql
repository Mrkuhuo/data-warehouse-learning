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
CREATE  TABLE IF NOT EXISTS ods.ods_customer_inf (
     `customer_inf_id` BIGINT,
     `customer_name` STRING,
     `identity_card_type` INT,
     `identity_card_no` STRING,
     `mobile_phone` STRING,
     `customer_email` STRING,
     `gender` STRING,
     `user_point` INT,
     `register_time` TIMESTAMP,
     `birthday` TIMESTAMP,
     `customer_level` INT,
     `user_money` INT,
     `event_time` TIMESTAMP,
     `customer_id` bigint
);

-- 批量读取数据
SET 'sql-client.execution.result-mode' = 'tableau';

SET 'execution.runtime-mode' = 'batch';

SELECT * FROM ods_customer_inf;

-- 流式读取数据

SET 'execution.runtime-mode' = 'streaming';

SELECT * FROM ods_customer_inf;
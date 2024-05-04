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
CREATE  TABLE IF NOT EXISTS ods.ods_product_brand_info (
     `brand_id` BIGINT,
     `brand_name` VARCHAR,
     `telephone` VARCHAR,
     `brand_web` VARCHAR,
     `brand_logo` VARCHAR,
     `brand_desc` VARCHAR,
     `brand_status` INT,
     `brand_order` INT,
     `event_time` TIMESTAMP
);


-- 批量读取数据
SET 'sql-client.execution.result-mode' = 'tableau';

SET 'execution.runtime-mode' = 'batch';

SELECT * FROM ods_product_brand_info;

-- 流式读取数据

SET 'execution.runtime-mode' = 'streaming';

SELECT * FROM ods_product_brand_info;
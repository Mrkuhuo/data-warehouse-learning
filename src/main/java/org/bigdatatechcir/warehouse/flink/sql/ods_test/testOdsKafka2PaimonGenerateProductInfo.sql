-- ./sql-client.sh

-- 执行以下代码

CREATE CATALOG my_catalog_ods WITH (
    'type'='paimon',
    'warehouse'='file:/opt/software/paimon_catelog'
);

USE CATALOG my_catalog_ods;

create  DATABASE IF NOT EXISTS ods;

use ods;

-- 创建paimon表
CREATE  TABLE IF NOT EXISTS ods.ods_generate_product_info (
     `product_id` BIGINT,
     `product_core` BIGINT,
     `product_name` STRING,
     `bar_code` STRING,
     `one_category_id` INT,
     `two_category_id` INT,
     `three_category_id` INT,
     `price` FLOAT NULL,
     `average_cost` FLOAT,
     `publish_status` INT,
     `audit_status` INT,
     `weight` FLOAT,
     `length` FLOAT,
     `height` FLOAT,
     `width` FLOAT,
     `color_type` STRING,
     `production_date` TIMESTAMP,
     `shelf_life` INT,
     `descript` STRING,
     `indate` TIMESTAMP,
     `event_time` TIMESTAMP,
     `brand_id` BIGINT,
     `supplier_id` BIGINT
);


-- 批量读取数据
SET 'sql-client.execution.result-mode' = 'tableau';

SET 'execution.runtime-mode' = 'batch';

SELECT * FROM ods_generate_product_info;

-- 流式读取数据

SET 'execution.runtime-mode' = 'streaming';

SELECT * FROM ods_generate_product_info;
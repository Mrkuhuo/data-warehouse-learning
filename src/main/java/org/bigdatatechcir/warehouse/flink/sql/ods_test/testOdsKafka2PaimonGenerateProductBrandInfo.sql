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
CREATE  TABLE IF NOT EXISTS ods.ods_generate_product_brand_info (
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

SELECT * FROM ods_generate_product_brand_info;

-- 流式读取数据

SET 'execution.runtime-mode' = 'streaming';

SELECT * FROM ods_generate_product_brand_info;
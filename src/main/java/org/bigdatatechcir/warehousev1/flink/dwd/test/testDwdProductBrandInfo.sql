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
CREATE  TABLE IF NOT EXISTS dwd.dwd_product_brand_info (
     `brand_id` BIGINT,
     `brand_name` VARCHAR,
     `telephone` VARCHAR,
     `brand_web` VARCHAR,
     `brand_logo` VARCHAR,
     `brand_desc` VARCHAR,
     `brand_status` INT,
     `brand_order` INT,
     `event_time` STRING
);

-- 批量读取数据
SET 'sql-client.execution.result-mode' = 'tableau';

SET 'execution.runtime-mode' = 'batch';

SELECT * FROM dwd.dwd_product_brand_info;

-- 流式读取数据

SET 'execution.runtime-mode' = 'streaming';

SELECT * FROM dwd.dwd_product_brand_info;
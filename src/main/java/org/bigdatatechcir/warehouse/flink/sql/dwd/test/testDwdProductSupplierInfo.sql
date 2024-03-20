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
-- ./sql-client.sh

-- 切换database
use dwd;

-- 创建paimon表
CREATE  TABLE IF NOT EXISTS dwd.dwd_product_supplier_info (
     `supplier_id` BIGINT,
     `supplier_code` VARCHAR,
     `supplier_name` VARCHAR,
     `supplier_type` INT,
     `link_man` VARCHAR,
     `phone_number` VARCHAR,
     `bank_name` VARCHAR,
     `bank_account` VARCHAR,
     `address` VARCHAR,
     `supplier_status` INT,
     `event_time` STRING
);

-- 批量读取数据
SET 'sql-client.execution.result-mode' = 'tableau';

SET 'execution.runtime-mode' = 'batch';

SELECT * FROM dwd.dwd_product_supplier_info;

-- 流式读取数据

SET 'execution.runtime-mode' = 'streaming';

SELECT * FROM dwd.dwd_product_supplier_info;
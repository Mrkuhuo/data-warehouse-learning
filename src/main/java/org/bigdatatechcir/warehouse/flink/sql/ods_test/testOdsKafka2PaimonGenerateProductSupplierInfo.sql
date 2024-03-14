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
CREATE  TABLE IF NOT EXISTS ods.ods_generate_product_supplier_info (
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
     `event_time` TIMESTAMP
);


-- 批量读取数据
SET 'sql-client.execution.result-mode' = 'tableau';

SET 'execution.runtime-mode' = 'batch';

SELECT * FROM ods_generate_product_supplier_info;

-- 流式读取数据

SET 'execution.runtime-mode' = 'streaming';

SELECT * FROM ods_generate_product_supplier_info;
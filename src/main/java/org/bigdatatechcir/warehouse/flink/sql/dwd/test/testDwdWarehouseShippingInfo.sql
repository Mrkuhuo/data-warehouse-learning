-- ./sql-client.sh

-- 创建CATALOG
CREATE CATALOG catalog_paimon WITH (
    'type'='paimon',
    'warehouse'='file:/opt/software/paimon_catelog'
);

-- 切换CATALOG
USE CATALOG catalog_paimon;

-- 创建database
create  DATABASE IF NOT EXISTS ods;

-- 切换database
use ods;

-- 创建paimon表
CREATE  TABLE IF NOT EXISTS ods.ods_warehouse_shipping_info (
    `ship_id` BIGINT,
    `ship_name` STRING,
    `ship_contact` STRING,
    `telephone` STRING,
    `price` FLOAT,
    `event_time` TIMESTAMP
);

-- 创建database
create  DATABASE IF NOT EXISTS dwd;

-- 切换database
use dwd;

-- 创建paimon表
CREATE  TABLE IF NOT EXISTS dwd.dwd_warehouse_shipping_info (
    `ship_id` BIGINT,
    `ship_name` STRING,
    `ship_contact` STRING,
    `telephone` STRING,
    `price` FLOAT,
    `event_time` TIMESTAMP
);

-- 批量读取数据
SET 'sql-client.execution.result-mode' = 'tableau';

SET 'execution.runtime-mode' = 'batch';

SELECT * FROM dwd.dwd_warehouse_shipping_info

-- 流式读取数据

SET 'execution.runtime-mode' = 'streaming';

SELECT * FROM dwd.dwd_warehouse_shipping_info
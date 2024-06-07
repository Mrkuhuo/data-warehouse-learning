-- ./sql-client.sh

-- 创建CATALOG
CREATE CATALOG catalog_paimon WITH (
    'type'='paimon',
    'warehouse'='file:/opt/software/paimon_catelog'
);

-- 切换CATALOG
USE CATALOG catalog_paimon;

-- 创建database
create  DATABASE IF NOT EXISTS dws;

-- 切换database
use dws;

-- DROP TABLE IF EXISTS dws.dws_order_master ;

-- 创建paimon表
CREATE  TABLE IF NOT EXISTS dws.dws_order_master (
     customer_id BIGINT,
     modified_day STRING,
     order_sum FLOAT,
     primary key(customer_id, modified_day)  NOT ENFORCED
);

-- 批量读取数据
SET 'sql-client.execution.result-mode' = 'tableau';

SET 'execution.runtime-mode' = 'batch';

SELECT * FROM dws.dws_order_master;

-- 流式读取数据

SET 'execution.runtime-mode' = 'streaming';

SELECT * FROM dws.dws_order_master;

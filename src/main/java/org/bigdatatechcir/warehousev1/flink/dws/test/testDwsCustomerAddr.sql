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

use dws;

-- DROP Table dws.dws_customer_addr;

-- 创建paimon dws表
CREATE  TABLE IF NOT EXISTS dws.dws_customer_addr (
    customer_id BIGINT ,
    event_day STRING,
	address_count BIGINT,
    primary key(customer_id, event_day)  NOT ENFORCED
);

-- 批量读取数据
SET 'sql-client.execution.result-mode' = 'tableau';

SET 'execution.runtime-mode' = 'batch';

SELECT * FROM dws.dws_customer_addr;

-- 流式读取数据

SET 'execution.runtime-mode' = 'streaming';

SELECT * FROM dws.dws_customer_addr;
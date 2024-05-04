-- 创建CATALOG
CREATE CATALOG catalog_paimon WITH (
    'type'='paimon',
    'warehouse'='file:/opt/software/paimon_catelog'
);

-- 切换CATALOG
USE CATALOG catalog_paimon;

-- 创建database
create  DATABASE IF NOT EXISTS ads;

use ads;

-- 创建paimon表
CREATE TABLE IF NOT EXISTS ads.ads_customer (
     customer_id BIGINT,
     customer_name STRING,
     address_count BIGINT,
     login_count BIGINT,
     cart_count BIGINT,
     order_sum FLOAT,
     modified_day STRING,
     primary key(customer_id, modified_day)  NOT ENFORCED
);

-- 批量读取数据
SET 'sql-client.execution.result-mode' = 'tableau';

SET 'execution.runtime-mode' = 'batch';

SELECT * FROM ads.ads_customer;

-- 流式读取数据

SET 'execution.runtime-mode' = 'streaming';

SELECT * FROM ads.ads_customer;
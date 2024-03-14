-- ./sql-client.sh

-- 执行以下代码

CREATE CATALOG my_catalog_ods WITH (
    'type'='paimon',
    'warehouse'='file:/opt/software/paimon_catelog'
);

USE CATALOG my_catalog_ods;

create  DATABASE IF NOT EXISTS ods;

use ods;

CREATE  TABLE IF NOT EXISTS ods.ods_generate_customer_login (
    login_name STRING,
    password STRING,
	user_stats BIGINT,
	event_time TIMESTAMP,
	customer_id BIGINT
);


-- 批量读取数据
SET 'sql-client.execution.result-mode' = 'tableau';

SET 'execution.runtime-mode' = 'batch';

SELECT * FROM ods_generate_customer_login;

-- 流式读取数据

SET 'execution.runtime-mode' = 'streaming';

SELECT * FROM ods_generate_customer_login;
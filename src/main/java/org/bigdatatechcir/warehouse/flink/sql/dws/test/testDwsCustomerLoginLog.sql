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

-- DROP TABLE IF EXISTS dws.dws_customer_login_log ;

-- 创建paimon表
CREATE  TABLE IF NOT EXISTS dws.dws_customer_login_log (
     customer_id BIGINT NULL comment '登陆日志ID',
     login_day STRING NULL comment '用户登陆天',
     login_count BIGINT NULL comment '登陆次数',
     primary key(customer_id, login_day)  NOT ENFORCED
);

-- 批量读取数据
SET 'sql-client.execution.result-mode' = 'tableau';

SET 'execution.runtime-mode' = 'batch';

SELECT * FROM dws.dws_customer_login_log;

-- 流式读取数据

SET 'execution.runtime-mode' = 'streaming';

SELECT * FROM dws.dws_customer_login_log;
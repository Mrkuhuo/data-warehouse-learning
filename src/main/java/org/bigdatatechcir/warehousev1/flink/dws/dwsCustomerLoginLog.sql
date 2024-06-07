-- 创建CATALOG
CREATE CATALOG catalog_paimon WITH (
    'type'='paimon',
    'warehouse'='file:/opt/software/paimon_catelog'
);

-- 切换CATALOG
USE CATALOG catalog_paimon;


create  DATABASE IF NOT EXISTS dwd;

-- 切换database
use dwd;


-- DROP TABLE IF EXISTS dwd.dwd_customer_login_log ;

-- 创建paimon表
CREATE  TABLE IF NOT EXISTS dwd.dwd_customer_login_log (
     login_id BIGINT NULL comment '登陆日志ID',
     login_time TIMESTAMP NULL comment '用户登陆时间',
     ipv4_public STRING NULL comment '登陆IP',
     login_type INT NULL comment '登陆类型：0未成功，1成功',
     event_time STRING NULL comment '事件时间',
     customer_id BIGINT NULL comment '登陆用户ID'
);

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

-- 是指checkpoint时间
SET 'execution.checkpointing.interval' = '10 s';

INSERT INTO dws.dws_customer_login_log
SELECT
    customer_id,
    event_time,
    COUNT(customer_id) as login_count
FROM
	dwd.dwd_customer_login_log
    GROUP BY customer_id, event_time;
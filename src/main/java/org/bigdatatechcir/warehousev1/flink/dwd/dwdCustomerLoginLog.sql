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
CREATE  TABLE IF NOT EXISTS ods.ods_customer_login_log (
     login_id BIGINT NULL comment '登陆日志ID',
     login_time TIMESTAMP NULL comment '用户登陆时间',
     ipv4_public STRING NULL comment '登陆IP',
     login_type INT NULL comment '登陆类型：0未成功，1成功',
     event_time TIMESTAMP NULL comment '事件时间',
     customer_id BIGINT NULL comment '登陆用户ID'
);

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

-- 是指checkpoint时间
SET 'execution.checkpointing.interval' = '10 s';

-- sql逻辑代码
INSERT INTO dwd.dwd_customer_login_log
SELECT
    -- 保留原始值，无需清洗
    login_id, 
    -- 确保login_time是合法的TIMESTAMP格式，如有必要，进行转换
    login_time, 
    -- 清理IPv4地址，去除两侧空格
    TRIM(ipv4_public) AS ipv4_public, 
    -- 登录类型字段直接保留原始值，后续可能根据业务需求验证或转换
    login_type, 
    -- 确保event_time是合法的TIMESTAMP格式，如有必要，进行转换
    DATE_FORMAT(event_time, 'yyyy-MM-dd'), 
    -- 保留原始值，无需清洗
    customer_id 
FROM
    ods.ods_customer_login_log
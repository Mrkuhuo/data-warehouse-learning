-- ./sql-client.sh

-- 执行以下代码

CREATE CATALOG catalog_paimon WITH (
    'type'='paimon',
    'warehouse'='file:/opt/software/paimon_catelog'
);

USE CATALOG catalog_paimon;

create  DATABASE IF NOT EXISTS ods;

use ods;

-- 创建paimon表
CREATE  TABLE IF NOT EXISTS ods.ods_customer_login_log (
     `login_id` BIGINT NULL comment '登陆日志ID',
     `login_time` TIMESTAMP NULL comment '用户登陆时间',
     `ipv4_public` STRING NULL comment '登陆IP',
     `login_type` INT NULL comment '登陆类型：0未成功，1成功',
     `event_time` TIMESTAMP NULL comment '事件时间',
     `customer_id` BIGINT NULL comment '登陆用户ID'
);


-- 批量读取数据
SET 'sql-client.execution.result-mode' = 'tableau';

SET 'execution.runtime-mode' = 'batch';

SELECT * FROM ods_customer_login_log;

-- 流式读取数据

SET 'execution.runtime-mode' = 'streaming';

SELECT * FROM ods_customer_login_log;
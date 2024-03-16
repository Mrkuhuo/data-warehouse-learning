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

-- 是指checkpoint时间
SET 'execution.checkpointing.interval' = '10 s';

-- sql逻辑代码
INSERT INTO dwd.dwd_warehouse_shipping_info
SELECT

    -- 保留原始值，无需清洗
    ship_id,

    -- 清理仓库名称和联系人姓名，去除两侧空格
    TRIM(ship_name) AS ship_name,
    TRIM(ship_contact) AS ship_contact,

    -- 清理电话号码，去除不必要的字符（如空格、破折号等），具体取决于数据格式
    REGEXP_REPLACE(telephone, '[^0-9]', '') AS telephone,

    -- 转换价格字段为浮点数（假设price是以字符串形式存储的）
    TRY_CAST(price AS FLOAT) AS price,

    -- 确保event_time是合法的TIMESTAMP格式，如有必要，进行转换
    event_time

FROM
    ods.ods_warehouse_shipping_info
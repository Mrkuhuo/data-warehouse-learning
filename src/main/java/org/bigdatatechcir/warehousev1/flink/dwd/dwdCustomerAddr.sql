-- 创建CATALOG
CREATE CATALOG my_catalog_ods WITH (
    'type'='paimon',
    'warehouse'='file:/opt/software/paimon_catelog'
);

-- 切换CATALOG
USE CATALOG my_catalog_ods;

-- 创建database
create  DATABASE IF NOT EXISTS ods;

-- 切换database
use ods;

-- 创建paimon表
CREATE  TABLE IF NOT EXISTS ods.ods_customer_addr (
    customer_addr_id BIGINT,
    zip STRING,
	province STRING,
	city STRING,
    district STRING,
    address STRING,
    is_default int,
    event_time TIMESTAMP,
	customer_id BIGINT
);

-- 创建database
create  DATABASE IF NOT EXISTS dwd;

-- 切换database
use dwd;

-- DROP TABLE dwd.dwd_customer_addr;
-- 创建paimon表
CREATE  TABLE IF NOT EXISTS dwd.dwd_customer_addr (
    customer_addr_id BIGINT,
    zip STRING,
	province STRING,
	city STRING,
    district STRING,
    address STRING,
    is_default int,
    event_time STRING,
	customer_id BIGINT
);

-- 是指checkpoint时间
SET 'execution.checkpointing.interval' = '10 s';

-- sql逻辑代码
INSERT INTO dwd.dwd_customer_addr
SELECT

    -- 保留原始值，无需清洗
    customer_addr_id,

    -- 清理邮编，去除两侧空格
    TRIM(zip) AS zip,

    -- 清理省份、城市和区县名称，去除两侧空格
    TRIM(province) AS province,
    TRIM(city) AS city,
    TRIM(district) AS district,

    -- 清理地址信息，去除两侧空格
    TRIM(address) AS address,

    -- 对is_default字段进行转换，确保其为整数值（假设is_default为0或1的字符串形式）
    CASE
        WHEN is_default IN ('0', '1') THEN CAST(is_default AS INT)
        ELSE NULL
    END AS is_default,
    DATE_FORMAT(event_time, 'yyyy-MM-dd'),

    -- 保留原始值，无需清洗
    customer_id

FROM
    ods.ods_customer_addr;
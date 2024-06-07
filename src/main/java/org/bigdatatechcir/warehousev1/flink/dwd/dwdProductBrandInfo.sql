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

DROP table IF  EXISTS ods.ods_product_brand_info;

-- 创建paimon表
CREATE  TABLE IF NOT EXISTS ods.ods_product_brand_info (
                                                           brand_id BIGINT,
                                                           brand_name VARCHAR,
                                                           telephone VARCHAR,
                                                           brand_web VARCHAR,
                                                           brand_logo VARCHAR,
                                                           brand_desc VARCHAR,
                                                           brand_status INT,
                                                           brand_order INT,
                                                           event_time TIMESTAMP
);

-- 创建database
create  DATABASE IF NOT EXISTS dwd;

-- 切换database
use dwd;

DROP TABLE IF EXISTS dwd.dwd_product_brand_info ;

-- 创建paimon表
CREATE  TABLE IF NOT EXISTS dwd.dwd_product_brand_info (
                                                           brand_id BIGINT,
                                                           brand_name VARCHAR,
                                                           telephone VARCHAR,
                                                           brand_web VARCHAR,
                                                           brand_logo VARCHAR,
                                                           brand_desc VARCHAR,
                                                           brand_status INT,
                                                           brand_order INT,
                                                           event_time STRING
);

-- 是指checkpoint时间
SET 'execution.checkpointing.interval' = '10 s';

-- sql逻辑代码
INSERT INTO dwd.dwd_product_brand_info
SELECT

    -- 保留原始值，无需清洗
    brand_id,

    -- 清理品牌名称，去除两侧空格
    TRIM(brand_name) AS brand_name,

    -- 清理电话号码，去除不必要的字符（如空格、破折号等），具体取决于数据格式
    REGEXP_REPLACE(telephone, '[^0-9]', '') AS telephone,

    -- 清理网址，确保格式正确（例如，前缀http或https，去除末尾的斜杠等）
    brand_web,

    -- 对于logo地址，通常不需要清洗，但在实际场景中可能需要验证链接的有效性
    brand_logo,

    -- 清理品牌描述，去除两侧空格
    TRIM(brand_desc) AS brand_desc,

    -- 直接保留品牌状态和排序字段的原始值，后续可能根据业务需求验证或转换
    brand_status,
    brand_order,

    -- 确保event_time是合法的TIMESTAMP格式，如有必要，进行转换
    DATE_FORMAT(event_time, 'yyyy-MM-dd')

FROM
    ods.ods_product_brand_info
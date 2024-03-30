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

DROP TABLE IF EXISTS ods.ods_product_info;
-- 创建paimon表
CREATE  TABLE IF NOT EXISTS ods.ods_product_info (
                                                     product_id BIGINT,
                                                     product_core BIGINT,
                                                     product_name STRING,
                                                     bar_code STRING,
                                                     one_category_id INT,
                                                     two_category_id INT,
                                                     three_category_id INT,
                                                     price FLOAT NULL,
                                                     average_cost FLOAT,
                                                     publish_status INT,
                                                     audit_status INT,
                                                     weight FLOAT,
                                                     length FLOAT,
                                                     height FLOAT,
                                                     width FLOAT,
                                                     color_type STRING,
                                                     production_date TIMESTAMP,
                                                     shelf_life INT,
                                                     descript STRING,
                                                     indate TIMESTAMP,
                                                     event_time TIMESTAMP,
                                                     brand_id BIGINT,
                                                     supplier_id BIGINT
);

-- 创建database
create  DATABASE IF NOT EXISTS dwd;

-- 切换database
use dwd;

DROP TABLE IF EXISTS dwd.dwd_product_info ;

-- 创建paimon表
CREATE  TABLE IF NOT EXISTS dwd.dwd_product_info (
                                                     product_id BIGINT,
                                                     product_core BIGINT,
                                                     product_name STRING,
                                                     bar_code STRING,
                                                     one_category_id INT,
                                                     two_category_id INT,
                                                     three_category_id INT,
                                                     price FLOAT NULL,
                                                     average_cost FLOAT,
                                                     publish_status INT,
                                                     audit_status INT,
                                                     weight FLOAT,
                                                     length FLOAT,
                                                     height FLOAT,
                                                     width FLOAT,
                                                     color_type STRING,
                                                     production_date TIMESTAMP,
                                                     shelf_life INT,
                                                     descript STRING,
                                                     indate TIMESTAMP,
                                                     event_time STRING,
                                                     brand_id BIGINT,
                                                     supplier_id BIGINT
);

-- 是指checkpoint时间
SET 'execution.checkpointing.interval' = '10 s';

-- sql逻辑代码
INSERT INTO dwd.dwd_product_info
SELECT

    -- 保留原始值，无需清洗
    product_id,
    product_core,

    -- 清理产品名称，去除两侧空格
    TRIM(product_name) AS product_name,

    -- 清理条形码，保持原样（通常条形码不允许有额外空格）
    bar_code,

    -- 直接保留一级分类、二级分类、三级分类ID字段的原始值，后续可能根据业务需求验证或转换
    one_category_id,
    two_category_id,
    three_category_id,

    -- 转换价格、平均成本、重量、长度、高度、宽度字段为浮点数（假设这些字段是以字符串形式存储的）
    TRY_CAST(price AS FLOAT) AS price,
    TRY_CAST(average_cost AS FLOAT) AS average_cost,
    publish_status,
    audit_status,
    TRY_CAST(weight AS FLOAT) AS weight,
    TRY_CAST(length AS FLOAT) AS length,
    TRY_CAST(height AS FLOAT) AS height,
    TRY_CAST(width AS FLOAT) AS width,

    -- 清理颜色类型，去除两侧空格
    TRIM(color_type) AS color_type,

    -- 确保生产日期是合法的TIMESTAMP格式，如有必要，进行转换
    production_date,

    -- 直接保留保质期、发布状态、审核状态字段的原始值，后续可能根据业务需求验证或转换
    shelf_life,


    -- 清理描述信息，去除两侧空格
    TRIM(descript) AS descript,

    -- 确保入库日期是合法的TIMESTAMP格式，如有必要，进行转换
    indate,

    -- 确保事件时间是合法的TIMESTAMP格式，如有必要，进行转换
    DATE_FORMAT(event_time, 'yyyy-MM-dd'),

    -- 保留原始值，无需清洗
    brand_id,
    supplier_id

FROM
    ods.ods_product_info;
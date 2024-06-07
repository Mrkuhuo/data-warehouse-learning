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

DROP TABLE IF  EXISTS ods.ods_product_supplier_info;

-- 创建paimon表
CREATE  TABLE IF NOT EXISTS ods.ods_product_supplier_info (
                                                              supplier_id BIGINT,
                                                              supplier_code VARCHAR,
                                                              supplier_name VARCHAR,
                                                              supplier_type INT,
                                                              link_man VARCHAR,
                                                              phone_number VARCHAR,
                                                              bank_name VARCHAR,
                                                              bank_account VARCHAR,
                                                              address VARCHAR,
                                                              supplier_status INT,
                                                              event_time TIMESTAMP
);

-- 创建database
create  DATABASE IF NOT EXISTS dwd;

-- 切换database
use dwd;

DROP TABLE IF EXISTS dwd.dwd_product_supplier_info ;

-- 创建paimon表
CREATE  TABLE IF NOT EXISTS dwd.dwd_product_supplier_info (
                                                              supplier_id BIGINT,
                                                              supplier_code VARCHAR,
                                                              supplier_name VARCHAR,
                                                              supplier_type INT,
                                                              link_man VARCHAR,
                                                              phone_number VARCHAR,
                                                              bank_name VARCHAR,
                                                              bank_account VARCHAR,
                                                              address VARCHAR,
                                                              supplier_status INT,
                                                              event_time STRING
);

-- 是指checkpoint时间
SET 'execution.checkpointing.interval' = '10 s';

-- sql逻辑代码
INSERT INTO dwd.dwd_product_supplier_info
SELECT

    -- 保留原始值，无需清洗
    supplier_id,

    -- 清理供应商编码和供应商名称，去除两侧空格
    TRIM(supplier_code) AS supplier_code,
    TRIM(supplier_name) AS supplier_name,

    -- 直接保留供应商类型字段的原始值，后续可能根据业务需求验证或转换
    supplier_type,

    -- 清理联系人姓名，去除两侧空格
    TRIM(link_man) AS link_man,

    -- 清理电话号码，去除不必要的字符（如空格、破折号等），具体取决于数据格式
    REGEXP_REPLACE(phone_number, '[^0-9]', '') AS phone_number,

    -- 清理银行名称，去除两侧空格
    TRIM(bank_name) AS bank_name,

    -- 对银行账户进行简单的格式检查和清理，例如确保只有数字和特定符号（例如减号）
    REGEXP_REPLACE(bank_account, '[^0-9-]', '') AS bank_account,

    -- 清理地址信息，去除两侧空格
    TRIM(address) AS address,

    -- 直接保留供应商状态字段的原始值，后续可能根据业务需求验证或转换
    supplier_status,

    -- 确保event_time是合法的TIMESTAMP格式，如有必要，进行转换
    DATE_FORMAT(event_time, 'yyyy-MM-dd')

FROM
    ods.ods_product_supplier_info
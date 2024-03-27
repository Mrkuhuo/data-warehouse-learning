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
CREATE  TABLE IF NOT EXISTS ods.ods_order_master (
     order_id BIGINT,
     order_sn BIGINT,
     payment_method int,
     order_money FLOAT,
     district_money FLOAT,
     shipping_money FLOAT,
     payment_money FLOAT,
     shipping_sn VARCHAR,
     create_time TIMESTAMP,
     shipping_time TIMESTAMP,
     pay_time TIMESTAMP,
     receive_time TIMESTAMP,
     order_status INT,
     order_point INT,
     event_time TIMESTAMP,
     customer_id BIGINT,
     shipping_comp_name BIGINT
);

-- 创建database
create  DATABASE IF NOT EXISTS dwd;

-- 切换database
use dwd;

-- DROP TABLE IF EXISTS dwd.dwd_order_master ;

-- 创建paimon表
CREATE  TABLE IF NOT EXISTS dwd.dwd_order_master (
     order_id BIGINT,
     order_sn BIGINT,
     payment_method int,
     order_money FLOAT,
     district_money FLOAT,
     shipping_money FLOAT,
     payment_money FLOAT,
     shipping_sn VARCHAR,
     create_time TIMESTAMP,
     shipping_time TIMESTAMP,
     pay_time TIMESTAMP,
     receive_time TIMESTAMP,
     order_status INT,
     order_point INT,
     event_time STRING,
     customer_id BIGINT,
     shipping_comp_name BIGINT,
     product_id BIGINT
);

-- 是指checkpoint时间
SET 'execution.checkpointing.interval' = '10 s';

-- sql逻辑代码
INSERT INTO dwd.dwd_order_master
SELECT

    -- 保留原始值，无需清洗
    order_id,
    order_sn,

    -- 直接保留支付方式字段的原始值，后续可能根据业务需求验证或转换
    payment_method,
    -- 转换订单金额、优惠金额、运费金额、实付金额为浮点数（假设这些字段是以字符串形式存储的）
    TRY_CAST(order_money AS FLOAT) AS order_money,
    TRY_CAST(district_money AS FLOAT) AS district_money,
    TRY_CAST(shipping_money AS FLOAT) AS shipping_money,
    TRY_CAST(payment_money AS FLOAT) AS payment_money,

    -- 清理物流单号，去除两侧空格
    TRIM(shipping_sn) AS shipping_sn,

    -- 确保创建时间、发货时间、支付时间、收货时间是合法的TIMESTAMP格式，如有必要，进行转换
    create_time,
    shipping_time,
    pay_time,
    receive_time,

    -- 直接保留订单状态和订单积分字段的原始值，后续可能根据业务需求验证或转换
    order_status,
    order_point,

    -- 保留事件时间字段原始值，如有必要，也可进行相同类型的转换
    DATE_FORMAT(event_time, 'yyyy-MM-dd'),

    -- 保留原始值，无需清洗
    customer_id,

    -- 清理物流公司名称，去除两侧空格
    shipping_comp_name,
    product_id

FROM
    ods.ods_order_master
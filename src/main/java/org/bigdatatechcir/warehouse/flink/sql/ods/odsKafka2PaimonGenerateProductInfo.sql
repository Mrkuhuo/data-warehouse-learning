-- 从kafka 中获取数据
CREATE TABLE IF NOT EXISTS generate_product_info(
     `product_id` BIGINT NULL comment '商品ID',
     `product_core` BIGINT NULL comment '商品编码',
     `product_name` VARCHAR NULL comment '商品名称',
     `bar_code` VARCHAR NULL comment '国条码',
     `one_category_id` INT NULL comment '一级分类ID',
     `two_category_id` INT NULL comment '二级分类ID',
     `three_category_id` INT NULL comment '三级分类ID',
     `price` FLOAT NULL comment '商品销售价格',
     `average_cost` FLOAT NULL comment '商品加权平均成本',
     `publish_status` INT NULL comment '上下架状态：0下架1上架',
     `audit_status` INT NULL comment '审核状态：0未审核，1已审核',
     `weight` FLOAT NULL comment '商品重量',
     `length` FLOAT NULL comment '商品长度',
     `height` FLOAT NULL comment '商品高度',
     `width` FLOAT NULL comment '商品宽度',
     `color_type` VARCHAR NULL comment '颜色',
     `production_date` TIMESTAMP NULL comment '生产日期',
     `shelf_life` INT NULL comment '商品有效期',
     `descript` VARCHAR NULL comment '商品描述',
     `indate` TIMESTAMP NULL comment '商品录入时间',
     `event_time` TIMESTAMP NULL comment '最后修改时间',
     `brand_id` BIGINT NULL comment '品牌表的ID',
     `supplier_id` BIGINT NULL comment '商品的供应商ID'
) WITH (
    'connector' = 'kafka',
    'topic' = 'generate_product_info',
    'properties.bootstrap.servers' = '192.168.154.131:9092',
    'properties.group.id' = 'generate_product_info',
    'scan.startup.mode' = 'group-offsets',
    'properties.auto.offset.reset' = 'earliest',
    'properties.enable.auto.commit'='true',
    'properties.auto.commit.interval.ms'='5000',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true',
    'json.fail-on-missing-field' = 'false'
);

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
CREATE  TABLE IF NOT EXISTS ods.ods_generate_product_info (
     `product_id` BIGINT,
     `product_core` BIGINT,
     `product_name` STRING,
     `bar_code` STRING,
     `one_category_id` INT,
     `two_category_id` INT,
     `three_category_id` INT,
     `price` FLOAT NULL,
     `average_cost` FLOAT,
     `publish_status` INT,
     `audit_status` INT,
     `weight` FLOAT,
     `length` FLOAT,
     `height` FLOAT,
     `width` FLOAT,
     `color_type` STRING,
     `production_date` TIMESTAMP,
     `shelf_life` INT,
     `descript` STRING,
     `indate` TIMESTAMP,
     `event_time` TIMESTAMP,
     `brand_id` BIGINT,
     `supplier_id` BIGINT
);

-- 是指checkpoint时间
SET 'execution.checkpointing.interval' = '10 s';

-- sql逻辑代码
insert into ods_generate_product_info select * from default_catalog.default_database.generate_product_info;
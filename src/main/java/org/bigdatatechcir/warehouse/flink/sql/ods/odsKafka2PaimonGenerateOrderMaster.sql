-- 从kafka 中获取数据
CREATE TABLE IF NOT EXISTS generate_order_master(
     `order_id` BIGINT NULL comment '订单ID',
     `order_sn` BIGINT NULL comment '订单编号',
     `payment_method` int NULL comment '支付方式：1现金，2余额，3网银，4支付宝，5微信',
     `order_money` FLOAT NULL comment '订单金额',
     `district_money` FLOAT NULL comment '优惠金额',
     `shipping_money` FLOAT NULL comment '运费金额',
     `payment_money` FLOAT NULL comment '支付金额',
     `shipping_sn` VARCHAR NULL comment '快递单号',
     `create_time` TIMESTAMP NULL comment '下单时间',
     `shipping_time` TIMESTAMP NULL comment '发货时间',
     `pay_time` TIMESTAMP NULL comment '支付时间',
     `receive_time` TIMESTAMP NULL comment '收货时间',
     `order_status` INT NULL comment '订单状态',
     `order_point` INT NULL comment '订单积分',
     `event_time` TIMESTAMP NULL comment '事件时间',
     `customer_id` BIGINT NULL comment '下单人ID',
     `shipping_comp_name` BIGINT NULL comment '快递公司名称'
) WITH (
    'connector' = 'kafka',
    'topic' = 'generate_order_master',
    'properties.bootstrap.servers' = '192.168.154.131:9092',
    'properties.group.id' = 'generate_order_master',
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
CREATE  TABLE IF NOT EXISTS ods.ods_generate_order_master (
     `order_id` BIGINT,
     `order_sn` BIGINT,
     `payment_method` int,
     `order_money` FLOAT,
     `district_money` FLOAT,
     `shipping_money` FLOAT,
     `payment_money` FLOAT,
     `shipping_sn` VARCHAR,
     `create_time` TIMESTAMP,
     `shipping_time` TIMESTAMP,
     `pay_time` TIMESTAMP,
     `receive_time` TIMESTAMP,
     `order_status` INT,
     `order_point` INT,
     `event_time` TIMESTAMP,
     `customer_id` BIGINT,
     `shipping_comp_name` BIGINT
);

-- 是指checkpoint时间
SET 'execution.checkpointing.interval' = '10 s';

-- sql逻辑代码
insert into ods_generate_order_master select * from default_catalog.default_database.generate_order_master;
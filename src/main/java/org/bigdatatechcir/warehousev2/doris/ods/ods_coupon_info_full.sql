-- DROP TABLE IF EXISTS ods.ods_coupon_info_full;
CREATE  TABLE ods.ods_coupon_info_full
(
    `id`               VARCHAR(255) COMMENT '购物券编号',
    `k1`               DATE NOT NULL   COMMENT '分区字段',
    `coupon_name`      STRING COMMENT '购物券名称',
    `coupon_type`      STRING COMMENT '购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券',
    `condition_amount` DECIMAL(16, 2) COMMENT '满额数',
    `condition_num`    BIGINT COMMENT '满件数',
    `activity_id`      STRING COMMENT '活动编号',
    `benefit_amount`   DECIMAL(16, 2) COMMENT '减金额',
    `benefit_discount` DECIMAL(16, 2) COMMENT '折扣',
    `create_time`      STRING COMMENT '创建时间',
    `range_type`       STRING COMMENT '范围类型 1、商品 2、品类 3、品牌',
    `limit_num`        BIGINT COMMENT '最多领用次数',
    `taken_count`      BIGINT COMMENT '已领用次数',
    `start_time`       STRING COMMENT '开始领取时间',
    `end_time`         STRING COMMENT '结束领取时间',
    `operate_time`     STRING COMMENT '修改时间',
    `expire_time`      STRING COMMENT '过期时间'
)
    ENGINE=OLAP
UNIQUE KEY(`id`,`k1`)
COMMENT '优惠券信息表'
PARTITION BY RANGE(`k1`) ()
DISTRIBUTED BY HASH(`id`)
PROPERTIES
(
    "replication_allocation" = "tag.location.default: 1",
    "is_being_synced" = "false",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-60",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "32",
    "dynamic_partition.create_history_partition" = "true"
);
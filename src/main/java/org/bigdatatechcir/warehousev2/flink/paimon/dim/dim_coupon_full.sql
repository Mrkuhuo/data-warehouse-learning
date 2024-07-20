-- 优惠券维度表
SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

CREATE CATALOG paimon_hive WITH (
    'type' = 'paimon',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);

use CATALOG paimon_hive;

create  DATABASE IF NOT EXISTS dim;

CREATE TABLE IF NOT EXISTS dim.dim_coupon_full(
    `id`               BIGINT COMMENT '购物券编号',
    `k1`               STRING COMMENT '分区字段',
    `coupon_name`      STRING COMMENT '购物券名称',
    `coupon_type_code` STRING COMMENT '购物券类型编码',
    `coupon_type_name` STRING COMMENT '购物券类型名称',
    `condition_amount` DECIMAL(16, 2) COMMENT '满额数',
    `condition_num`    BIGINT COMMENT '满件数',
    `activity_id`      BIGINT COMMENT '活动编号',
    `benefit_amount`   DECIMAL(16, 2) COMMENT '减金额',
    `benefit_discount` DECIMAL(16, 2) COMMENT '折扣',
    `benefit_rule`     STRING COMMENT '优惠规则:满元*减*元，满*件打*折',
    `create_time`      TIMESTAMP(3) COMMENT '创建时间',
    `range_type_code`  STRING COMMENT '优惠范围类型编码',
    `range_type_name`  STRING COMMENT '优惠范围类型名称',
    `limit_num`        BIGINT COMMENT '最多领取次数',
    `taken_count`      BIGINT COMMENT '已领取次数',
    `start_time`       TIMESTAMP(3) COMMENT '可以领取的开始日期',
    `end_time`         TIMESTAMP(3) COMMENT '可以领取的结束日期',
    `operate_time`     TIMESTAMP(3) COMMENT '修改时间',
    `expire_time`      TIMESTAMP(3) COMMENT '过期时间',
    PRIMARY KEY (`id`,`k1` ) NOT ENFORCED
    )   PARTITIONED BY (`k1` ) WITH (
   'connector' = 'paimon',
   'metastore.partitioned-table' = 'true',
   'file.format' = 'parquet',
   'write-buffer-size' = '512mb',
   'write-buffer-spillable' = 'true' ,
   'partition.expiration-time' = '1 d',
   'partition.expiration-check-interval' = '1 h',
   'partition.timestamp-formatter' = 'yyyy-MM-dd',
   'partition.timestamp-pattern' = '$k1'
   );

insert into dim.dim_coupon_full(id, k1, coupon_name, coupon_type_code, coupon_type_name, condition_amount, condition_num, activity_id, benefit_amount, benefit_discount, benefit_rule, create_time, range_type_code, range_type_name, limit_num, taken_count, start_time, end_time, operate_time, expire_time)
select
    id,
    k1,
    coupon_name,
    coupon_type,
    coupon_dic.dic_name,
    condition_amount,
    condition_num,
    activity_id,
    benefit_amount,
    benefit_discount,
    case coupon_type
        when '3201' then concat('满',cast(condition_amount as STRING),'元减', cast(benefit_amount as STRING),'元')
        when '3202' then concat('满',cast(condition_num as STRING),'件打',cast((10*(1-benefit_discount)) as STRING),'折')
        when '3203' then concat('减',cast(benefit_amount as STRING),'元')
        end benefit_rule,
    create_time,
    range_type,
    range_dic.dic_name,
    limit_num,
    taken_count,
    start_time,
    end_time,
    operate_time,
    expire_time
from
    (
        select
            id,
            k1,
            coupon_name,
            coupon_type,
            condition_amount,
            condition_num,
            activity_id,
            benefit_amount,
            benefit_discount,
            create_time,
            range_type,
            limit_num,
            taken_count,
            start_time,
            end_time,
            operate_time,
            expire_time
        from ods.ods_coupon_info_full
    )ci
        left join
    (
        select
            dic_code,
            dic_name
        from ods.ods_base_dic_full
        where parent_code='32'
    )coupon_dic
on ci.coupon_type=coupon_dic.dic_code
    left join
    (
    select
    dic_code,
    dic_name
    from ods.ods_base_dic_full
    where parent_code='33'
    )range_dic
    on ci.range_type=range_dic.dic_code;
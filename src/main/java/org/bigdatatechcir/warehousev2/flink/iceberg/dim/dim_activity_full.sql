SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'execution.runtime-mode' = 'streaming';

CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);

use CATALOG iceberg_catalog;

create  DATABASE IF NOT EXISTS iceberg_dim;

CREATE TABLE IF NOT EXISTS iceberg_dim.dim_activity_full(
    `activity_rule_id`   INT COMMENT '活动规则ID',
    `activity_id`        BIGINT COMMENT '活动ID',
    `k1`                 STRING  COMMENT '分区字段',
    `activity_name`      STRING COMMENT '活动名称',
    `activity_type_code` STRING COMMENT '活动类型编码',
    `activity_type_name` STRING COMMENT '活动类型名称',
    `activity_desc`      STRING COMMENT '活动描述',
    `start_time`         STRING COMMENT '开始时间',
    `end_time`           STRING COMMENT '结束时间',
    `create_time`        STRING COMMENT '创建时间',
    `condition_amount`   DECIMAL(16, 2) COMMENT '满减金额',
    `condition_num`      BIGINT COMMENT '满减件数',
    `benefit_amount`     DECIMAL(16, 2) COMMENT '优惠金额',
    `benefit_discount`   DECIMAL(16, 2) COMMENT '优惠折扣',
    `benefit_rule`       STRING COMMENT '优惠规则',
    `benefit_level`      BIGINT COMMENT '优惠级别',
    PRIMARY KEY (`activity_rule_id`,`activity_id`,`k1` ) NOT ENFORCED
    )   PARTITIONED BY (`k1` ) WITH (
    'catalog-name'='hive_prod',
    'uri'='thrift://192.168.244.129:9083',
    'warehouse'='hdfs://192.168.244.129:9000/user/hive/warehouse/'
    );

insert into iceberg_dim.dim_activity_full /*+ OPTIONS('upsert-enabled'='true') */(
    activity_rule_id,
    activity_id,
    k1,
    activity_name,
    activity_type_code,
    activity_type_name,
    activity_desc,
    start_time,
    end_time,
    create_time,
    condition_amount,
    condition_num,
    benefit_amount,
    benefit_discount,
    benefit_rule,
    benefit_level
    )
select
    rule.id,
    info.id,
    info.k1,
    activity_name,
    rule.activity_type,
    dic.dic_name,
    activity_desc,
    start_time,
    end_time,
    create_time,
    condition_amount,
    condition_num,
    benefit_amount,
    benefit_discount,
    case rule.activity_type
        when '3101' then concat('满',cast(condition_amount as STRING),'元减',cast(benefit_amount as STRING),'元')
        when '3102' then concat('满',cast(condition_num as STRING),'件打',cast((10*(1-benefit_discount)) as STRING),'折')
        when '3103' then concat('打',cast((10*(1-benefit_discount)) as STRING),'折')
        end benefit_rule,
    benefit_level
from
    (
        select
            id,
            activity_id,
            activity_type,
            condition_amount,
            condition_num,
            benefit_amount,
            benefit_discount,
            benefit_level
        from iceberg_ods.ods_activity_rule_full /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/
    )rule
        left join
    (
        select
            id,
            k1,
            activity_name,
            activity_type,
            activity_desc,
            start_time,
            end_time,
            create_time
        from iceberg_ods.ods_activity_info_full /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/
    )info
on rule.activity_id=info.id
    left join
    (
    select
    dic_code,
    dic_name
    from iceberg_ods.ods_base_dic_full
    where  parent_code='31'
    )dic
    on rule.activity_type=dic.dic_code;
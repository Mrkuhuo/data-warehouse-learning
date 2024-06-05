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

create  DATABASE IF NOT EXISTS ads;

CREATE TABLE IF NOT EXISTS ads.ads_coupon_stats_full(
    `dt`          STRING COMMENT '统计日期',
    `coupon_id`   BIGINT COMMENT '优惠券ID',
    `coupon_name` STRING COMMENT '优惠券名称',
    `start_date`  STRING COMMENT '发布日期',
    `rule_name`   STRING COMMENT '优惠规则，例如满100元减10元',
    `reduce_rate` DECIMAL(16, 2) COMMENT '补贴率'
    );

INSERT INTO ads.ads_coupon_stats_full(
    dt,
    coupon_id,
    coupon_name,
    start_date,
    rule_name,
    reduce_rate
    )
select
    k1,
    coupon_id,
    coupon_name,
    start_date,
    coupon_rule,
    cast(coupon_reduce_amount_30d/original_amount_30d as decimal(16,2))
from dws.dws_trade_coupon_order_nd_full;
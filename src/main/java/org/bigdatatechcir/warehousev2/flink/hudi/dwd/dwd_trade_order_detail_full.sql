SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

create catalog hudi_catalog with(
	'type' = 'hudi',
	'mode' = 'hms',
	'hive.conf.dir'='/opt/software/apache-hive-3.1.3-bin/conf'
);

use CATALOG hudi_catalog;

create  DATABASE IF NOT EXISTS hudi_dwd;

CREATE TABLE IF NOT EXISTS hudi_dwd.dwd_trade_order_detail_full(
    `id`                    BIGINT COMMENT '编号',
    `k1`                    STRING COMMENT '分区字段',
    `order_id`              BIGINT COMMENT '订单id',
    `user_id`               BIGINT COMMENT '用户id',
    `sku_id`                BIGINT COMMENT '商品id',
    `province_id`           BIGINT COMMENT '省份id',
    `activity_id`           BIGINT COMMENT '参与活动规则id',
    `activity_rule_id`      BIGINT COMMENT '参与活动规则id',
    `coupon_id`             BIGINT COMMENT '使用优惠券id',
    `date_id`               STRING COMMENT '下单日期id',
    `create_time`           timestamp(3) COMMENT '下单时间',
    `source_id`             BIGINT COMMENT '来源编号',
    `source_type_code`      STRING COMMENT '来源类型编码',
    `source_type_name`      STRING COMMENT '来源类型名称',
    `sku_num`               BIGINT COMMENT '商品数量',
    `split_original_amount` DECIMAL(16, 2) COMMENT '原始价格',
    `split_activity_amount` DECIMAL(16, 2) COMMENT '活动优惠分摊',
    `split_coupon_amount`   DECIMAL(16, 2) COMMENT '优惠券优惠分摊',
    `split_total_amount`    DECIMAL(16, 2) COMMENT '最终价格分摊',
    PRIMARY KEY (`id`,`k1` ) NOT ENFORCED
    )   PARTITIONED BY (`k1` ) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
    );


INSERT INTO hudi_dwd.dwd_trade_order_detail_full(id, k1, order_id, user_id, sku_id, province_id, activity_id, activity_rule_id, coupon_id, date_id, create_time, source_id, source_type_code, source_type_name, sku_num, split_original_amount, split_activity_amount, split_coupon_amount, split_total_amount)
select
    od.id,
    k1,
    order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    date_format(create_time, 'yyyy-MM-dd') date_id,
    create_time,
    source_id,
    source_type,
    dic_name,
    sku_num,
    split_original_amount,
    split_activity_amount,
    split_coupon_amount,
    split_total_amount
from
    (
        select
            id,
            k1,
            order_id,
            sku_id,
            create_time,
            source_id,
            source_type,
            sku_num,
            sku_num * order_price split_original_amount,
            split_total_amount,
            split_activity_amount,
            split_coupon_amount
        from hudi_ods.ods_order_detail_full
    ) od
        left join
    (
        select
            id,
            user_id,
            province_id
        from hudi_ods.ods_order_info_full
    ) oi
    on od.order_id = oi.id
        left join
    (
        select
            order_detail_id,
            activity_id,
            activity_rule_id
        from hudi_ods.ods_order_detail_activity_full
    ) act
    on od.id = act.order_detail_id
        left join
    (
        select
            order_detail_id,
            coupon_id
        from hudi_ods.ods_order_detail_coupon_full
    ) cou
    on od.id = cou.order_detail_id
        left join
    (
        select
            dic_code,
            dic_name
        from hudi_ods.ods_base_dic_full
        where parent_code='24'
    )dic
    on od.source_type=dic.dic_code;
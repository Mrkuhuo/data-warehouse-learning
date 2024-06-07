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

CREATE TABLE IF NOT EXISTS iceberg_dim.dim_sku_full(
    `id`                   BIGINT COMMENT 'sku_id',
    `k1`                   STRING COMMENT '分区字段',
    `price`                DECIMAL(16, 2) COMMENT '商品价格',
    `sku_name`             STRING COMMENT '商品名称',
    `sku_desc`             STRING COMMENT '商品描述',
    `weight`               DECIMAL(16, 2) COMMENT '重量',
    `is_sale`              INT COMMENT '是否在售',
    `spu_id`               BIGINT COMMENT 'spu编号',
    `spu_name`             STRING COMMENT 'spu名称',
    `category3_id`         BIGINT COMMENT '三级分类id',
    `category3_name`       STRING COMMENT '三级分类名称',
    `category2_id`         BIGINT COMMENT '二级分类id',
    `category2_name`       STRING COMMENT '二级分类名称',
    `category1_id`         BIGINT COMMENT '一级分类id',
    `category1_name`       STRING COMMENT '一级分类名称',
    `tm_id`                BIGINT COMMENT '品牌id',
    `tm_name`              STRING COMMENT '品牌名称',
    `attr_ids`             STRING COMMENT '平台属性',
    `sale_attr_ids`        STRING COMMENT '销售属性',
    `create_time`           TIMESTAMP(3) COMMENT '创建时间',
    PRIMARY KEY (`id`,`k1` ) NOT ENFORCED
    )   PARTITIONED BY (`k1` ) WITH (
    'catalog-name'='hive_prod',
    'uri'='thrift://192.168.244.129:9083',
    'warehouse'='hdfs://192.168.244.129:9000/user/hive/warehouse/'
    );

INSERT INTO iceberg_dim.dim_sku_full /*+ OPTIONS('upsert-enabled'='true') */(
    id,
    k1,
    price,
    sku_name,
    sku_desc,
    weight,
    is_sale,
    spu_id,
    spu_name,
    category3_id,
    category3_name,
    category2_id,
    category2_name,
    category1_id,
    category1_name,
    tm_id,
    tm_name,
    attr_ids,
    sale_attr_ids,
    create_time
)
SELECT
    s.id,
    s.k1,
    s.price,
    s.sku_name,
    s.sku_desc,
    s.weight,
    s.is_sale,
    s.spu_id,
    sp.spu_name,
    s.category3_id,
    c3.name AS category3_name,
    c3.category2_id,
    c2.name AS category2_name,
    c2.category1_id,
    c1.name AS category1_name,
    s.tm_id,
    tm.tm_name,
    cast(a.attr_ids as STRING),
    cast(sa.sale_attr_ids as STRING),
    s.create_time
FROM
    (
        SELECT
            id,
            k1,
            price,
            sku_name,
            sku_desc,
            weight,
            is_sale,
            spu_id,
            category3_id,
            tm_id,
            create_time
        FROM iceberg_ods.ods_sku_info_full /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/
    ) s
        LEFT JOIN (
        SELECT
            id,
            spu_name
        FROM iceberg_ods.ods_spu_info_full /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/
    ) sp ON s.spu_id = sp.id
        LEFT JOIN (
        SELECT
            id,
            name,
            category2_id
        FROM iceberg_ods.ods_base_category3_full /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/
    ) c3 ON s.category3_id = c3.id
        LEFT JOIN (
        SELECT
            id,
            name,
            category1_id
        FROM iceberg_ods.ods_base_category2_full /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/
    ) c2 ON c3.category2_id = c2.id
        LEFT JOIN (
        SELECT
            id,
            name
        FROM iceberg_ods.ods_base_category1_full /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/
    ) c1 ON c2.category1_id = c1.id
        LEFT JOIN (
        SELECT
            id,
            tm_name
        FROM iceberg_ods.ods_base_trademark_full /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/
    ) tm ON s.tm_id = tm.id
        LEFT JOIN (
        SELECT
            sku_id,
            collect(id) AS attr_ids
        FROM iceberg_ods.ods_sku_attr_value_full /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/
        GROUP BY sku_id
    ) a ON s.id = a.sku_id
        LEFT JOIN (
        SELECT
            sku_id,
            collect(id) AS sale_attr_ids
        FROM iceberg_ods.ods_sku_sale_attr_value_full /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/
        GROUP BY sku_id
    ) sa ON s.id = sa.sku_id;
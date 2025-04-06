-- ===============================================================================
-- 活动维度表(dim_activity_full)
-- 功能描述：整合活动相关维度信息，为活动分析提供维度支持
-- 更新策略：全量快照，每日更新
-- 主键策略：活动ID + 日期分区
-- 分区策略：按日期范围分区
-- 应用场景：支持活动效果分析、活动规则评估、活动归因分析等业务场景
-- ===============================================================================

-- DROP TABLE IF EXISTS dim.dim_activity_full;
CREATE TABLE dim.dim_activity_full
(
    `id`              VARCHAR(255) COMMENT '活动ID，活动唯一标识',
    `k1`              DATE NOT NULL COMMENT '分区字段，数据日期',
    `activity_name`   STRING COMMENT '活动名称，如"618大促"、"双11狂欢节"等',
    `activity_type`   STRING COMMENT '活动类型，如"满减"、"折扣"、"秒杀"等',
    `activity_desc`   STRING COMMENT '活动描述，详细活动说明',
    `start_time`      DATETIME COMMENT '活动开始时间，活动生效起始时间',
    `end_time`        DATETIME COMMENT '活动结束时间，活动失效时间',
    `create_time`     DATETIME COMMENT '创建时间，活动记录创建时间',
    `rule_id`         VARCHAR(255) COMMENT '活动规则ID，关联规则表',
    `rule_name`       STRING COMMENT '规则名称，如"满300减50"、"每满100减30"等',
    `benefit_amount`  DECIMAL(16, 2) COMMENT '优惠金额，活动提供的优惠额度',
    `benefit_discount` DECIMAL(16, 2) COMMENT '优惠折扣，如0.8表示8折',
    `benefit_level`   STRING COMMENT '优惠等级，如"超级优惠"、"特惠"等',
    `benefit_desc`    STRING COMMENT '优惠描述，详细说明活动优惠内容',
    `limit_amount`    DECIMAL(16, 2) COMMENT '限制金额，参与活动的最低消费额',
    `limit_num`       BIGINT COMMENT '限制数量，活动限购数量',
    `status`          TINYINT COMMENT '状态，1:有效 0:无效',
    `sku_id_list`     ARRAY<STRING> COMMENT '适用商品ID列表，活动覆盖的商品范围',
    `category_id_list` ARRAY<STRING> COMMENT '适用品类ID列表，活动覆盖的品类范围'
)
    ENGINE=OLAP
UNIQUE KEY(`id`,`k1`) -- 使用活动ID和日期作为联合主键，确保单一日期内一个活动只有一条记录
COMMENT '活动维度表 - 整合活动相关维度信息'
PARTITION BY RANGE(`k1`) () -- 按日期范围分区，便于管理历史数据
DISTRIBUTED BY HASH(`id`) -- 按活动ID哈希分布，确保相关查询高效
PROPERTIES
(
    "replication_allocation" = "tag.location.default: 1", -- 默认副本分配策略
    "is_being_synced" = "false", -- 是否正在同步
    "storage_format" = "V2", -- 使用V2存储格式，提升性能
    "light_schema_change" = "true", -- 允许轻量级的schema变更
    "disable_auto_compaction" = "false", -- 不禁用自动压缩
    "enable_single_replica_compaction" = "false", -- 不启用单副本压缩
    "dynamic_partition.enable" = "true", -- 启用动态分区
    "dynamic_partition.time_unit" = "DAY", -- 动态分区时间单位为天
    "dynamic_partition.start" = "-60", -- 保留过去60天的分区
    "dynamic_partition.end" = "3", -- 预创建未来3天的分区
    "dynamic_partition.prefix" = "p", -- 分区前缀
    "dynamic_partition.buckets" = "32", -- 每个分区的分桶数
    "dynamic_partition.create_history_partition" = "true" -- 创建历史分区
);
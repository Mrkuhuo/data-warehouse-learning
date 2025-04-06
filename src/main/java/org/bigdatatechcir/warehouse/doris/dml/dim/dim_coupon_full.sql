-- ===============================================================================
-- 优惠券维度表(dim_coupon_full)
-- 功能描述：整合优惠券相关维度信息，为优惠券分析提供维度支持
-- 更新策略：全量快照，每日更新
-- 主键策略：优惠券ID + 日期分区
-- 分区策略：按日期范围分区
-- 应用场景：支持优惠券效果分析、领取使用分析、促销ROI分析等业务场景
-- ===============================================================================

-- DROP TABLE IF EXISTS dim.dim_coupon_full;
CREATE TABLE dim.dim_coupon_full
(
    `id`                 VARCHAR(255) COMMENT '优惠券ID，优惠券唯一标识',
    `k1`                 DATE NOT NULL COMMENT '分区字段，数据日期',
    `coupon_name`        STRING COMMENT '优惠券名称，如"新人专享券"、"满减券"等',
    `coupon_type`        STRING COMMENT '优惠券类型，如"满减券"、"满折券"、"无门槛券"等',
    `coupon_desc`        STRING COMMENT '优惠券描述，详细说明优惠券使用条件和优惠内容',
    `condition_amount`   DECIMAL(16, 2) COMMENT '使用门槛金额，满多少可用，0表示无门槛',
    `condition_num`      BIGINT COMMENT '使用门槛件数，满多少件可用，0表示无门槛',
    `benefit_amount`     DECIMAL(16, 2) COMMENT '优惠金额，优惠券面额',
    `benefit_discount`   DECIMAL(16, 2) COMMENT '优惠折扣，如0.8表示8折',
    `benefit_desc`       STRING COMMENT '优惠描述，详细说明优惠内容',
    `create_time`        DATETIME COMMENT '创建时间，优惠券创建时间',
    `range_type`         STRING COMMENT '适用范围类型，如"全品类"、"限品类"、"限商品"等',
    `limit_num`          BIGINT COMMENT '每人限领数量，单个用户可领取的张数限制',
    `taken_count`        BIGINT COMMENT '已领取数量，当前已被领取的总数',
    `used_count`         BIGINT COMMENT '已使用数量，当前已被使用的总数',
    `start_time`         DATETIME COMMENT '可领取开始时间，优惠券可开始领取的时间',
    `end_time`           DATETIME COMMENT '可领取结束时间，优惠券领取截止时间',
    `valid_days`         BIGINT COMMENT '有效天数，领取后有效的天数',
    `status`             TINYINT COMMENT '状态，1:有效 0:无效',
    `sku_id_list`        ARRAY<STRING> COMMENT '适用商品ID列表，优惠券可用于的商品范围',
    `category_id_list`   ARRAY<STRING> COMMENT '适用品类ID列表，优惠券可用于的品类范围'
)
    ENGINE=OLAP
UNIQUE KEY(`id`,`k1`) -- 使用优惠券ID和日期作为联合主键，确保单一日期内一个优惠券只有一条记录
COMMENT '优惠券维度表 - 整合优惠券相关维度信息'
PARTITION BY RANGE(`k1`) () -- 按日期范围分区，便于管理历史数据
DISTRIBUTED BY HASH(`id`) -- 按优惠券ID哈希分布，确保相关查询高效
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
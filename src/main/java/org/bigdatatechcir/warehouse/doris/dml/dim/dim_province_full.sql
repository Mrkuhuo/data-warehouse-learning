-- ===============================================================================
-- 地区维度表(dim_province_full)
-- 功能描述：构建标准化的地区维度信息，支持地域相关的分析和报表
-- 更新策略：全量快照，低频更新
-- 主键策略：省份ID
-- 分区策略：不分区(数据量小)
-- 应用场景：支持地域销售分析、区域市场评估、物流配送分析等业务场景
-- ===============================================================================

-- DROP TABLE IF EXISTS dim.dim_province_full;
CREATE TABLE dim.dim_province_full
(
    `id`            VARCHAR(255) COMMENT '省份ID，省份唯一标识',
    `province_name` STRING COMMENT '省份名称，如"北京市"、"广东省"',
    `area_code`     STRING COMMENT '地区编码，行政区划代码',
    `iso_code`      STRING COMMENT '旧版ISO-3166-2编码，供可视化地图使用',
    `iso_3166_2`    STRING COMMENT '新版ISO-3166-2编码，国际标准化组织省级行政区划编码',
    `region_id`     STRING COMMENT '地区ID，关联所属地区，如华北、华东等',
    `region_name`   STRING COMMENT '地区名称，如"华北"、"华东"、"华南"等'
)
ENGINE=OLAP
UNIQUE KEY(`id`) -- 使用省份ID作为主键
COMMENT '地区维度表 - 提供地域相关分析维度'
DISTRIBUTED BY HASH(`id`) -- 按省份ID哈希分布
PROPERTIES
(
    "replication_allocation" = "tag.location.default: 1", -- 默认副本分配策略
    "is_being_synced" = "false", -- 是否正在同步
    "storage_format" = "V2", -- 使用V2存储格式，提升性能
    "light_schema_change" = "true", -- 允许轻量级的schema变更
    "disable_auto_compaction" = "false", -- 不禁用自动压缩
    "enable_single_replica_compaction" = "false" -- 不启用单副本压缩
);
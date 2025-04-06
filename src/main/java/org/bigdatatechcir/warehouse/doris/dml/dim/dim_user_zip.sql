-- ===============================================================================
-- 用户维度表(dim_user_zip)
-- 功能描述：使用拉链表设计存储用户维度信息，记录用户属性随时间的变化
-- 更新策略：拉链表，增量更新
-- 主键策略：用户ID + 日期分区
-- 分区策略：按日期范围分区
-- 应用场景：支持用户分析、用户分群、用户画像等业务场景的维度关联
-- 特殊说明：该表为SCD Type 2缓慢变化维度，通过start_date和end_date记录版本有效期
-- ===============================================================================

-- DROP TABLE IF EXISTS dim.dim_user_zip;
CREATE TABLE dim.dim_user_zip
(
    `id`           VARCHAR(64) COMMENT '用户ID，用户唯一标识',
    `k1`           DATE NOT NULL COMMENT '分区字段，数据日期',
    `login_name`   STRING COMMENT '用户登录名，账号名称',
    `nick_name`    STRING COMMENT '用户昵称，用户自定义展示名',
    `name`         STRING COMMENT '用户真实姓名，已加密',
    `phone_num`    STRING COMMENT '手机号码，已加密',
    `email`        STRING COMMENT '电子邮箱，已加密',
    `user_level`   STRING COMMENT '用户等级，如"青铜"、"白银"、"黄金"等',
    `birthday`     STRING COMMENT '用户生日，格式YYYY-MM-DD',
    `gender`       STRING COMMENT '用户性别，M-男性，F-女性，U-未知',
    `create_time`  STRING COMMENT '创建时间，用户首次注册时间',
    `operate_time` STRING COMMENT '操作时间，用户信息最后修改时间',
    `start_date`   STRING COMMENT '开始日期，当前版本生效开始日期',
    `end_date`     STRING COMMENT '结束日期，当前版本失效日期，9999-12-31表示当前有效版本'
)
    ENGINE=OLAP
UNIQUE KEY(`id`,`k1`) -- 使用用户ID和日期作为联合主键，确保单一日期内一个用户只有一条记录
COMMENT '用户维度表 - 拉链表结构，记录用户属性变化历史'
PARTITION BY RANGE(`k1`) () -- 按日期范围分区，便于管理历史数据
DISTRIBUTED BY HASH(`id`) -- 按用户ID哈希分布，确保相关查询高效
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
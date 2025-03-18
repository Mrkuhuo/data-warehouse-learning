-- DROP TABLE IF EXISTS ods.ods_ware_order_task_detail_full;
CREATE TABLE ods.ods_ware_order_task_detail_full
(
    `id`           VARCHAR(255) COMMENT '编号',
    `sku_id`       STRING COMMENT 'sku_id',
    `sku_name`     STRING COMMENT 'sku名称',
    `sku_num`      BIGINT COMMENT '购买个数',
    `task_id`      STRING COMMENT '工作单id',
    `refund_status`  STRING COMMENT '创建时间'
)
    ENGINE=OLAP  -- 使用Doris的OLAP引擎，适用于高并发分析场景
    UNIQUE KEY(`id`)  -- 唯一键约束，保证(id, k1)组合的唯一性（Doris聚合模型特性）
COMMENT '库存工作单明细表'
DISTRIBUTED BY HASH(`id`)  -- 按id哈希分桶，保证相同id的数据分布在同一节点
PROPERTIES
(
    "replication_allocation" = "tag.location.default: 1",  -- 副本分配策略：默认标签分配1个副本
    "is_being_synced" = "false",          -- 是否处于同步状态（通常保持false）
    "storage_format" = "V2",             -- 存储格式版本（V2支持更高效压缩和索引）
    "light_schema_change" = "true",      -- 启用轻量级schema变更（仅修改元数据，无需数据重写）
    "disable_auto_compaction" = "false", -- 启用自动压缩（合并小文件提升查询性能）
    "enable_single_replica_compaction" = "false", -- 禁用单副本压缩（多副本时保持数据一致性）

    "bloom_filter_columns" = "id,sku_id,task_id",  -- 为高频过滤字段创建布隆过滤器，加速WHERE查询
    "compaction_policy" = "time_series",          -- 按时间序合并策略优化时序数据
    "enable_unique_key_merge_on_write" = "true",  -- 唯一键写时合并（实时更新场景减少读放大）
    "in_memory" = "false"                        -- 关闭全内存存储（仅小表可开启）
); 
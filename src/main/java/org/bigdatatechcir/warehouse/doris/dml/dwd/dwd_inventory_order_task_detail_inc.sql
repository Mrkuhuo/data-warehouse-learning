-- DROP TABLE IF EXISTS dwd.dwd_inventory_order_task_detail_inc;
CREATE TABLE dwd.dwd_inventory_order_task_detail_inc
(
    `id`                    VARCHAR(255) COMMENT '编号',
    `k1`                    DATE NOT NULL COMMENT '分区字段',
    `task_id`               STRING COMMENT '任务ID',
    `order_id`              STRING COMMENT '订单ID',
    `sku_id`                STRING COMMENT '商品ID',
    `sku_name`              STRING COMMENT '商品名称',
    `sku_num`               BIGINT COMMENT '商品数量',
    `sku_price`             DECIMAL(16,2) COMMENT '商品价格',
    `warehouse_id`          STRING COMMENT '仓库ID',
    `warehouse_name`        STRING COMMENT '仓库名称',
    `create_time`           STRING COMMENT '创建时间',
    `source_id`             STRING COMMENT '来源ID',
    `source_type`           STRING COMMENT '来源类型',
    `split_total_amount`    DECIMAL(16,2) COMMENT '分摊总金额',
    `split_activity_amount` DECIMAL(16,2) COMMENT '分摊活动金额',
    `split_coupon_amount`   DECIMAL(16,2) COMMENT '分摊优惠券金额',
    `task_status`           STRING COMMENT '任务状态',
    `tracking_no`           STRING COMMENT '物流单号',
    `user_id`               STRING COMMENT '用户ID'
)
    ENGINE=OLAP  -- 使用Doris的OLAP引擎，适用于高并发分析场景
    UNIQUE KEY(`id`,`k1`)  -- 唯一键约束，保证(id, k1)组合的唯一性（Doris聚合模型特性）
COMMENT '库存域订单任务明细增量表'
PARTITION BY RANGE(`k1`) ()  -- 按日期范围分区（具体分区规则由动态分区配置决定）
DISTRIBUTED BY HASH(`id`)  -- 按id哈希分桶，保证相同id的数据分布在同一节点
PROPERTIES
(
    "replication_allocation" = "tag.location.default: 1",  -- 副本分配策略：默认标签分配1个副本
    "is_being_synced" = "false",          -- 是否处于同步状态（通常保持false）
    "storage_format" = "V2",             -- 存储格式版本（V2支持更高效压缩和索引）
    "light_schema_change" = "true",      -- 启用轻量级schema变更（仅修改元数据，无需数据重写）
    "disable_auto_compaction" = "false", -- 启用自动压缩（合并小文件提升查询性能）
    "enable_single_replica_compaction" = "false", -- 禁用单副本压缩（多副本时保持数据一致性）

    "dynamic_partition.enable" = "true",            -- 启用动态分区
    "dynamic_partition.time_unit" = "DAY",          -- 按天创建分区
    "dynamic_partition.start" = "-60",             -- 保留最近60天的历史分区
    "dynamic_partition.end" = "3",                 -- 预先创建未来3天的分区
    "dynamic_partition.prefix" = "p",              -- 分区名前缀（如p20240101）
    "dynamic_partition.buckets" = "32",            -- 每个分区的分桶数（影响并行度）
    "dynamic_partition.create_history_partition" = "true", -- 自动创建缺失的历史分区

    "bloom_filter_columns" = "id,task_id,order_id,sku_id,warehouse_id,user_id",  -- 为高频过滤字段创建布隆过滤器，加速WHERE查询
    "compaction_policy" = "time_series",          -- 按时间序合并策略优化时序数据
    "enable_unique_key_merge_on_write" = "true",  -- 唯一键写时合并（实时更新场景减少读放大）
    "in_memory" = "false"                        -- 关闭全内存存储（仅小表可开启）
); 
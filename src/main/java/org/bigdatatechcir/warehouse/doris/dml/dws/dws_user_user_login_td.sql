/*
 * 表名: dws_user_user_login_td
 * 说明: 用户域用户粒度登录历史至今汇总事实表
 * 数据粒度: 用户 + 日期
 * 存储策略: 分区存储，按日期分区
 * 刷新周期: 每日刷新
 * 应用场景:
 *   - 用户活跃度分析：评估用户的登录频次和最近活跃情况
 *   - 用户留存研究：分析用户的长期留存情况和活跃周期
 *   - 用户生命周期管理：基于登录历史构建用户生命周期模型
 *   - 用户活跃度分层：根据累计登录次数进行用户价值分层
 *   - 用户流失预警：基于末次登录时间识别潜在流失用户
 */
-- DROP TABLE IF EXISTS dws.dws_user_user_login_td;
CREATE TABLE dws.dws_user_user_login_td
(
    /* 维度字段 */
    `user_id`        VARCHAR(255) COMMENT '用户ID - 用于唯一标识注册用户',
    `k1`             DATE NOT NULL COMMENT '数据日期 - 分区字段，记录数据所属日期',
    
    /* 度量值字段 - 登录统计 */
    `login_date_last` VARCHAR(255) COMMENT '末次登录日期 - 用户最近一次登录的日期',
    `login_count_td`  BIGINT COMMENT '累计登录次数 - 用户历史至今的总登录次数'
)
ENGINE=OLAP
UNIQUE KEY(`user_id`, `k1`)
COMMENT '用户域用户粒度登录历史至今汇总事实表'
PARTITION BY RANGE(`k1`) ()
DISTRIBUTED BY HASH(`user_id`) BUCKETS 32
PROPERTIES
(
    "replication_allocation" = "tag.location.default: 1",
    "is_being_synced" = "false",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false",
    "dynamic_partition.enable" = "true",           /* 启用动态分区功能 */
    "dynamic_partition.time_unit" = "DAY",         /* 按天进行分区 */
    "dynamic_partition.start" = "-60",             /* 保留60天历史数据 */
    "dynamic_partition.end" = "3",                 /* 预创建未来3天分区 */
    "dynamic_partition.prefix" = "p",              /* 分区名称前缀 */
    "dynamic_partition.buckets" = "32",            /* 每个分区的分桶数 */
    "dynamic_partition.create_history_partition" = "true" /* 创建历史分区 */
);

/*
 * 表设计说明：
 *
 * 1. 主键与分布设计：
 *    - 主键：使用user_id + k1复合主键，支持特定用户在不同日期的登录累计数据分析
 *    - 分桶：按用户ID分桶(32桶)，优化用户级查询性能，适合用户行为分析场景
 *    - 分区：使用日期（k1）分区，支持历史数据生命周期管理，提升时间范围查询效率
 *
 * 2. 数据组织：
 *    - 用户维度：记录用户唯一标识，支持用户级活跃度分析
 *    - 时间维度：包含数据日期和末次登录日期，支持时间序列分析和近期活跃度评估
 *    - 累计指标：记录历史至今的登录总次数，体现用户的长期活跃度和参与度
 * 
 * 3. 存储管理：
 *    - 动态分区：自动管理历史数据，保留最近60天数据，预创建3天分区
 *    - 存储优化：使用zstd压缩算法和V2存储格式，平衡查询性能和存储效率
 *    - 分桶策略：根据用户ID分桶，优化针对特定用户的查询性能
 *
 * 4. 典型应用场景：
 *    - 用户活跃度评估：基于登录次数和末次登录日期，评估用户活跃状态
 *    - 用户分层分析：根据登录频次将用户分为高频、中频、低频群体
 *    - 用户流失预警：识别长期未登录的用户，实施召回策略
 *    - 用户价值评估：将登录频次作为用户参与度的指标之一，评估用户价值
 *    - 用户生命周期研究：分析用户从首次登录到最近登录的行为模式和变化
 *
 * 5. 优化建议：
 *    - 增加首次登录日期：记录用户的首次登录时间，便于分析用户生命周期完整区间
 *    - 添加登录间隔指标：计算平均登录间隔，评估用户的登录频率稳定性
 *    - 增加登录时段统计：记录用户常用登录时段，分析用户使用习惯
 *    - 添加登录渠道维度：记录用户登录的渠道来源，评估不同渠道的用户活跃度
 *    - 扩展为近期登录指标：增加近7日、近30日登录次数，评估近期活跃度变化
 */
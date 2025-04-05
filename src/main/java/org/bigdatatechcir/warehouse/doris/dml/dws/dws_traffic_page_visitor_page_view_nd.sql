/*
 * 表名: dws_traffic_page_visitor_page_view_nd
 * 说明: 流量域访客页面粒度页面浏览最近n日汇总事实表
 * 数据粒度: 访客 + 页面 + 日期
 * 存储策略: 分区存储，按日期分区
 * 刷新周期: 每日刷新
 * 应用场景:
 *   - 页面中长期访问趋势分析：通过7日和30日数据，评估页面持续吸引力
 *   - 用户粘性与忠诚度研究：分析用户在不同时间跨度的页面访问行为变化
 *   - 设备使用趋势研究：分析中长期内不同设备的访问模式变化
 *   - 内容策略优化：通过中长期数据评估内容对用户的持续吸引力
 *   - 用户行为变化监控：识别用户访问习惯和偏好的变化趋势
 */
-- DROP TABLE IF EXISTS dws.dws_traffic_page_visitor_page_view_nd;
CREATE TABLE dws.dws_traffic_page_visitor_page_view_nd
(
    /* 维度字段 */
    `mid_id`         VARCHAR(255) COMMENT '访客ID - 用于唯一标识一个访问用户',
    `k1`             DATE NOT NULL COMMENT '数据日期 - 分区字段，记录数据所属日期',
    `page_id`        VARCHAR(255) COMMENT '页面ID - 标识被访问的具体页面',
    
    /* 设备维度 - 用于分析不同设备的访问特征 */
    `brand`          VARCHAR(255) COMMENT '手机品牌 - 访问设备的品牌名称',
    `model`          VARCHAR(255) COMMENT '手机型号 - 访问设备的具体型号',
    `operate_system` VARCHAR(255) COMMENT '操作系统 - 访问设备的操作系统类型',
    
    /* 度量值字段 - 7日访问统计 */
    `during_time_7d` BIGINT COMMENT '最近7日浏览时长 - 用户在该页面近7天的总停留时间(秒)',
    `view_count_7d`  BIGINT COMMENT '最近7日访问次数 - 用户访问该页面近7天的总次数',
    
    /* 度量值字段 - 30日访问统计 */
    `during_time_30d` BIGINT COMMENT '最近30日浏览时长 - 用户在该页面近30天的总停留时间(秒)',
    `view_count_30d`  BIGINT COMMENT '最近30日访问次数 - 用户访问该页面近30天的总次数'
)
ENGINE=OLAP
UNIQUE KEY(`mid_id`, `k1`, `page_id`)
COMMENT '流量域访客页面粒度页面浏览最近n日汇总事实表'
PARTITION BY RANGE(`k1`) ()
DISTRIBUTED BY HASH(`mid_id`) BUCKETS 32
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
 *    - 主键：使用mid_id + k1 + page_id复合主键，支持访客对特定页面在不同日期的中长期浏览行为分析
 *    - 分桶：按访客ID分桶(32桶)，优化访客级查询性能，适合用户行为分析场景
 *    - 分区：使用日期（k1）分区，支持历史数据生命周期管理，提升时间范围查询效率
 *
 * 2. 数据组织：
 *    - 访客维度：记录用户唯一标识，支持个体行为的中长期分析
 *    - 页面维度：包含页面标识，支持页面级流量的中长期分析
 *    - 设备维度：记录品牌、型号和操作系统信息，支持多终端适配的持续优化
 *    - 时间维度：分别提供7日和30日两个时间窗口的汇总指标，覆盖中短期和中长期分析
 *    - 浏览指标：包含访问次数和浏览时长，反映页面的持续吸引力和用户粘性变化
 * 
 * 3. 存储管理：
 *    - 动态分区：自动管理历史数据，保留最近60天数据，预创建3天分区
 *    - 存储优化：使用zstd压缩算法和V2存储格式，平衡查询性能和存储效率
 *    - 分桶策略：根据访客ID分桶，优化针对特定用户的查询性能
 *
 * 4. 典型应用场景：
 *    - 页面持续吸引力评估：比较7日和30日指标，评估页面对用户的持续吸引力
 *    - 用户活跃度变化分析：通过对比不同时间窗口数据，识别用户活跃度变化趋势
 *    - 内容策略有效性检验：评估内容改版或优化策略在中长期的实际效果
 *    - 季节性访问模式识别：通过不同时间维度数据对比，发现访问行为的季节性特征
 *    - 用户留存分析：结合1日表与N日表，评估页面对用户的留存贡献
 *
 * 5. 优化建议：
 *    - 扩展为更多时间维度：考虑增加90日等更长周期的汇总指标，支持长期趋势分析
 *    - 增加同环比指标：添加环比和同比变化率，直观反映访问趋势变化
 *    - 添加用户分群维度：根据访问频次对用户分群，分析不同用户群体的行为特征
 *    - 增加页面转化指标：结合业务事件数据，分析页面对转化的中长期贡献
 *    - 添加交互深度指标：记录用户与页面的交互深度，全面评估页面体验
 */
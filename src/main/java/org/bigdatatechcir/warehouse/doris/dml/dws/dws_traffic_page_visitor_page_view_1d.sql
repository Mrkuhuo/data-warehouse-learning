/*
 * 表名: dws_traffic_page_visitor_page_view_1d
 * 说明: 流量域访客页面粒度页面浏览最近1日汇总事实表
 * 数据粒度: 访客 + 页面 + 日期
 * 存储策略: 分区存储，按日期分区
 * 刷新周期: 每日刷新
 * 应用场景:
 *   - 页面访问热度分析：识别站内最受欢迎的页面和访问路径
 *   - 用户设备偏好研究：分析不同设备对网站浏览行为的影响
 *   - 页面停留时长优化：评估用户在各页面的停留时长，发现可优化的页面
 *   - 用户访问路径分析：结合页面访问数据构建用户浏览路径模型
 *   - 操作系统与终端偏好：了解用户使用的设备特征，优化多终端体验
 */
-- DROP TABLE IF EXISTS dws.dws_traffic_page_visitor_page_view_1d;
CREATE TABLE dws.dws_traffic_page_visitor_page_view_1d
(
    /* 维度字段 */
    `mid_id`         VARCHAR(255) COMMENT '访客ID - 用于唯一标识一个访问用户',
    `k1`             DATE NOT NULL COMMENT '数据日期 - 分区字段，记录数据所属日期',
    `page_id`        VARCHAR(255) COMMENT '页面ID - 标识被访问的具体页面',
    
    /* 设备维度 - 用于分析不同设备的访问特征 */
    `brand`          STRING COMMENT '手机品牌 - 访问设备的品牌名称',
    `model`          STRING COMMENT '手机型号 - 访问设备的具体型号',
    `operate_system` STRING COMMENT '操作系统 - 访问设备的操作系统类型',
    
    /* 度量值字段 - 访问统计 */
    `during_time_1d` BIGINT COMMENT '最近1日浏览时长 - 用户在该页面的总停留时间(秒)',
    `view_count_1d`  BIGINT COMMENT '最近1日访问次数 - 用户访问该页面的总次数'
)
ENGINE=OLAP
UNIQUE KEY(`mid_id`, `k1`, `page_id`)
COMMENT '流量域访客页面粒度页面浏览最近1日汇总事实表'
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
 *    - 主键：使用mid_id + k1 + page_id复合主键，支持访客对特定页面在不同日期的浏览行为分析
 *    - 分桶：按访客ID分桶(32桶)，优化访客级查询性能，适合用户行为分析场景
 *    - 分区：使用日期（k1）分区，支持历史数据生命周期管理，提升时间范围查询效率
 *
 * 2. 数据组织：
 *    - 访客维度：记录用户唯一标识，支持个体行为分析
 *    - 页面维度：包含页面标识，支持页面级流量分析
 *    - 设备维度：记录品牌、型号和操作系统信息，支持多终端适配分析
 *    - 浏览指标：包含访问次数和浏览时长，反映页面吸引力和用户粘性
 * 
 * 3. 存储管理：
 *    - 动态分区：自动管理历史数据，保留最近60天数据，预创建3天分区
 *    - 存储优化：使用zstd压缩算法和V2存储格式，平衡查询性能和存储效率
 *    - 分桶策略：根据访客ID分桶，优化针对特定用户的查询性能
 *
 * 4. 典型应用场景：
 *    - 热门页面分析：找出访问量最高、停留时间最长的页面，指导内容优化
 *    - 用户行为研究：分析用户在不同页面的浏览模式，了解用户兴趣
 *    - 设备兼容性评估：分析不同设备和操作系统的访问特征，优化页面适配
 *    - 页面性能监控：结合访问时长，评估页面加载和交互性能
 *    - 转化路径分析：与转化事件数据结合，分析页面对转化的贡献
 *
 * 5. 优化建议：
 *    - 扩展为N日汇总表：增加7日、30日汇总指标，支持中长期趋势分析
 *    - 增加页面分类维度：添加页面类型字段，便于按功能模块分析页面表现
 *    - 添加会话维度：增加会话ID和会话内序号，支持用户完整访问路径分析
 *    - 增加地理位置维度：添加访问地区信息，支持地域分布分析
 *    - 添加页面加载指标：记录页面加载时间，监控页面性能
 */
/*
 * 表名: dws_traffic_session_page_view_1d
 * 说明: 流量域会话粒度页面浏览最近1日汇总事实表
 * 数据粒度: 会话 + 访客 + 日期
 * 存储策略: 分区存储，按日期分区
 * 刷新周期: 每日刷新
 * 应用场景:
 *   - 会话行为分析：评估单次访问会话的访问深度和停留时长
 *   - 渠道质量评估：分析不同渠道来源会话的页面访问特征
 *   - 应用版本体验对比：比较不同应用版本的会话访问行为差异
 *   - 用户访问路径研究：基于会话分析用户完整浏览路径和访问流程
 *   - 设备兼容性监控：评估不同设备和系统下会话体验的差异
 */
-- DROP TABLE IF EXISTS dws.dws_traffic_session_page_view_1d;
CREATE TABLE dws.dws_traffic_session_page_view_1d
(
    /* 维度字段 */
    `session_id`     VARCHAR(255) COMMENT '会话ID - 用于唯一标识一次访问会话',
    `mid_id`         VARCHAR(255) COMMENT '设备ID - 标识用户使用的设备',
    `k1`             DATE NOT NULL COMMENT '数据日期 - 分区字段，记录数据所属日期',
    
    /* 设备与渠道维度 - 用于分析不同来源和设备的会话特征 */
    `brand`          VARCHAR(255) COMMENT '手机品牌 - 访问设备的品牌名称',
    `model`          VARCHAR(255) COMMENT '手机型号 - 访问设备的具体型号',
    `operate_system` VARCHAR(255) COMMENT '操作系统 - 访问设备的操作系统类型',
    `version_code`   VARCHAR(255) COMMENT 'APP版本号 - 应用的版本标识',
    `channel`        VARCHAR(255) COMMENT '渠道 - 用户访问来源渠道',
    
    /* 度量值字段 - 会话访问统计 */
    `during_time_1d` BIGINT COMMENT '最近1日访问时长 - 会话总停留时间(秒)',
    `page_count_1d`  BIGINT COMMENT '最近1日访问页面数 - 会话浏览的页面总数'
)
ENGINE=OLAP
UNIQUE KEY(`session_id`, `mid_id`, `k1`)
COMMENT '流量域会话粒度页面浏览最近1日汇总事实表'
PARTITION BY RANGE(`k1`) ()
DISTRIBUTED BY HASH(`session_id`) BUCKETS 32
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
 *    - 主键：使用session_id + mid_id + k1复合主键，支持对特定会话在特定设备和日期的访问行为分析
 *    - 分桶：按会话ID分桶(32桶)，优化会话级查询性能，适合会话行为分析场景
 *    - 分区：使用日期（k1）分区，支持历史数据生命周期管理，提升时间范围查询效率
 *
 * 2. 数据组织：
 *    - 会话维度：记录会话唯一标识，支持单次访问行为分析
 *    - 设备维度：记录设备ID、品牌、型号和操作系统信息，支持设备兼容性分析
 *    - 应用维度：包含APP版本号和渠道信息，支持版本体验和渠道质量分析
 *    - 会话指标：包含会话停留时长和访问页面数，反映会话深度和体验质量
 * 
 * 3. 存储管理：
 *    - 动态分区：自动管理历史数据，保留最近60天数据，预创建3天分区
 *    - 存储优化：使用zstd压缩算法和V2存储格式，平衡查询性能和存储效率
 *    - 分桶策略：根据会话ID分桶，优化针对特定会话的查询性能
 *
 * 4. 典型应用场景：
 *    - 会话质量评估：分析会话的停留时长和页面访问数，评估交互深度
 *    - 版本迭代分析：比较不同APP版本的会话访问行为，评估版本更新效果
 *    - 渠道效果分析：研究不同渠道来源会话的访问特征，优化渠道投放
 *    - 用户体验优化：结合设备信息分析会话行为，发现潜在的兼容性问题
 *    - 访问路径分析：作为用户路径研究的基础数据，支持漏斗分析
 *
 * 5. 优化建议：
 *    - 扩展为N日汇总表：增加7日、30日汇总指标，支持会话特征的中长期分析
 *    - 增加页面序列信息：记录会话内页面访问顺序，支持更深入的路径分析
 *    - 添加会话来源属性：记录会话的入口页面和来源类型，分析访问入口效果
 *    - 增加地理位置维度：添加访问地区信息，支持地域分布分析
 *    - 添加会话结果标记：标记会话是否达成转化目标，评估会话有效性
 */
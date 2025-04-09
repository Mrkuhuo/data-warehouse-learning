-- ===============================================================================
-- 渠道流量统计报表(ads_traffic_stats_by_channel)
-- 功能描述：统计各渠道的流量指标，包括访客数、会话数、停留时长等
-- 数据来源：dws_traffic_session_page_view_1d、dws_traffic_page_visitor_page_view_nd
-- 刷新策略：每日刷新
-- 应用场景：渠道质量分析、营销效果评估、流量运营决策
-- ===============================================================================

-- DROP TABLE IF EXISTS ads.ads_traffic_stats_by_channel;
CREATE  TABLE ads.ads_traffic_stats_by_channel
(
    `dt`               VARCHAR(255) COMMENT '统计日期',
    `recent_days`      BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `channel`          STRING COMMENT '渠道',
    `uv_count`         BIGINT COMMENT '访客人数',
    `avg_duration_sec` BIGINT COMMENT '会话平均停留时长，单位为秒',
    `avg_page_count`   BIGINT COMMENT '会话平均浏览页面数',
    `sv_count`         BIGINT COMMENT '会话数',
    `bounce_rate`      DECIMAL(16, 2) COMMENT '跳出率'
)
    ENGINE=OLAP
DUPLICATE KEY(`dt`)
COMMENT '各渠道流量统计'
DISTRIBUTED BY HASH(`dt`)
PROPERTIES
(
    "replication_allocation" = "tag.location.default: 1",
    "is_being_synced" = "false",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false"
);

/*
 * 表设计说明：
 *
 * 1. 表用途与价值：
 *    本表是渠道流量分析的核心报表，统计各来源渠道的流量质量指标，
 *    帮助营销和运营团队评估各渠道的流量价值，优化营销资源分配和渠道策略。
 *
 * 2. 数据组成与维度：
 *    - 时间维度：统计日期和时间周期(1天/7天/30天)，支持不同时间粒度的渠道分析
 *    - 渠道维度：记录流量来源渠道，如搜索引擎、社交媒体、直接访问等
 *    - 流量指标：包含流量规模(UV/SV)和质量指标(停留时长、页面数、跳出率)
 *
 * 3. 关键指标解读：
 *    - 访客人数(UV)：渠道带来的独立访客数，反映渠道的流量规模
 *    - 会话数(SV)：用户从进入到离开网站的访问过程，一个用户可能有多个会话
 *    - 平均停留时长：会话平均持续时间，反映用户对内容的关注度和兴趣度
 *    - 平均页面浏览数：会话内平均浏览的页面数量，反映内容对用户的吸引力
 *    - 跳出率：只浏览一个页面就离开的会话比例，反映渠道精准度和内容相关性
 *    - 人均会话数：可通过计算 sv_count/uv_count 得出，反映用户访问频次
 *
 * 4. 典型应用场景：
 *    - 渠道质量评估：比较不同渠道流量的质量指标，如停留时长、跳出率等
 *    - 营销投入决策：基于渠道流量质量，优化营销资源分配
 *    - 渠道异常监控：监控渠道流量的异常波动，及时调整策略
 *    - 内容匹配度分析：通过跳出率等指标，评估渠道流量与网站内容的匹配度
 *    - 用户路径优化：基于不同渠道用户的行为特征，优化登陆页和用户路径
 *    - 渠道转化分析：结合订单数据，评估不同渠道的流量转化能力
 *
 * 5. 查询示例：
 *    - 按流量规模排序渠道：
 *      SELECT channel, uv_count, sv_count
 *      FROM ads.ads_traffic_stats_by_channel
 *      WHERE dt = '${yesterday}' AND recent_days = 30
 *      ORDER BY uv_count DESC;
 *    
 *    - 分析渠道流量质量指标：
 *      SELECT channel, 
 *             avg_duration_sec/60 AS avg_minutes, 
 *             avg_page_count,
 *             bounce_rate
 *      FROM ads.ads_traffic_stats_by_channel
 *      WHERE dt = '${yesterday}' AND recent_days = 7
 *      ORDER BY avg_duration_sec DESC;
 *
 *    - 计算各渠道的流量价值综合评分：
 *      SELECT channel, 
 *             uv_count,
 *             (avg_duration_sec/60 * 0.3 + avg_page_count * 0.3 + (100-bounce_rate) * 0.4) AS quality_score
 *      FROM ads.ads_traffic_stats_by_channel
 *      WHERE dt = '${yesterday}' AND recent_days = 30
 *      AND uv_count > 100  -- 过滤小流量渠道
 *      ORDER BY quality_score DESC;
 *
 * 6. 建议扩展：
 *    - 添加转化指标：增加渠道带来的下单转化率、GMV等指标
 *    - 添加成本指标：增加渠道获客成本，计算ROI
 *    - 添加用户价值指标：增加渠道用户的生命周期价值、复购率等
 *    - 添加设备维度：区分PC端和移动端的渠道表现
 *    - 添加对比指标：增加环比、同比变化率，评估渠道增长趋势
 */
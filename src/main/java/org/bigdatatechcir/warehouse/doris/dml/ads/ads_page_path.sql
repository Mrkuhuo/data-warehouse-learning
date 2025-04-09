-- ===============================================================================
-- 页面浏览路径分析报表(ads_page_path)
-- 功能描述：分析用户在站内的页面浏览路径和跳转次数
-- 数据来源：dws_traffic_page_visitor_page_view_nd
-- 刷新策略：每日刷新
-- 应用场景：用户行为路径分析、页面流量分析、用户体验优化
-- ===============================================================================

-- DROP TABLE IF EXISTS ads.ads_page_path;
CREATE  TABLE ads.ads_page_path
(
    `dt`          VARCHAR(255) COMMENT '统计日期',
    `recent_days` BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `source`      STRING COMMENT '跳转起始页面ID',
    `target`      STRING COMMENT '跳转终到页面ID',
    `path_count`  BIGINT COMMENT '跳转次数'
)
    ENGINE=OLAP
DUPLICATE KEY(`dt`)
COMMENT '页面浏览路径分析'
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
 *    本表是用户页面浏览路径分析的核心报表，记录用户在站内的页面跳转行为和频次，
 *    帮助产品和运营团队了解用户的浏览路径偏好，发现页面间的关联关系，优化网站结构和用户体验。
 *
 * 2. 数据组成与维度：
 *    - 时间维度：统计日期和时间周期(1天/7天/30天)，支持不同时间范围的路径分析
 *    - 路径维度：包含起始页面和目标页面，构成用户浏览的有向路径
 *    - 频次维度：记录路径的跳转次数，反映路径的热度和重要性
 *
 * 3. 关键指标解读：
 *    - 跳转次数：特定路径的发生频次，反映该路径的流量大小
 *    - 页面出入流量：可通过聚合计算特定页面的总入流量和出流量
 *    - 转化路径效率：可分析从入口页到目标页的不同路径效率
 *
 * 4. 典型应用场景：
 *    - 热门浏览路径分析：发现用户最常使用的浏览路径，优化这些路径的体验
 *    - 页面流量瓶颈分析：发现流量异常流失的页面，改进其设计或功能
 *    - 导航结构优化：基于实际用户行为优化网站导航和页面间的链接关系
 *    - 转化漏斗路径分析：分析从首页到下单页的不同路径效率，优化转化路径
 *    - 新旧版本对比：比较网站改版前后的用户浏览路径变化，评估改版效果
 *
 * 5. 查询示例：
 *    - 查询热门跳转路径：
 *      SELECT source, target, path_count
 *      FROM ads.ads_page_path
 *      WHERE dt = '${yesterday}' AND recent_days = 7
 *      ORDER BY path_count DESC
 *      LIMIT 20;
 *    
 *    - 分析特定页面的去向分布：
 *      SELECT target, path_count, 
 *             path_count/SUM(path_count) OVER(PARTITION BY source) AS path_rate
 *      FROM ads.ads_page_path
 *      WHERE dt = '${yesterday}' AND recent_days = 30
 *      AND source = '${page_id}'
 *      ORDER BY path_count DESC;
 *
 *    - 构建从首页到支付页的主要路径：
 *      WITH RECURSIVE path_analysis AS (
 *          -- 起始页为首页的路径
 *          SELECT source, target, path_count, 1 AS level, ARRAY[source, target] AS path
 *          FROM ads.ads_page_path
 *          WHERE dt = '${yesterday}' AND recent_days = 30
 *          AND source = 'home_page'
 *          
 *          UNION ALL
 *          
 *          -- 递归连接下一步路径
 *          SELECT p.source, p.target, p.path_count, pa.level + 1, 
 *                 ARRAY_APPEND(pa.path, p.target) AS path
 *          FROM ads.ads_page_path p
 *          JOIN path_analysis pa ON p.source = pa.target
 *          WHERE p.dt = '${yesterday}' AND p.recent_days = 30
 *          AND pa.level < 5  -- 限制路径深度
 *          AND NOT ARRAY_CONTAINS(pa.path, p.target)  -- 避免循环
 *      )
 *      SELECT path, SUM(path_count) AS total_count
 *      FROM path_analysis
 *      WHERE ARRAY_CONTAINS(path, 'payment_page')  -- 包含支付页的路径
 *      GROUP BY path
 *      ORDER BY total_count DESC
 *      LIMIT 10;
 *
 * 6. 建议扩展：
 *    - 添加页面名称：增加页面ID对应的页面名称，提高可读性
 *    - 添加会话维度：记录路径在会话中的位置，区分会话初始路径和深度浏览路径
 *    - 添加用户维度：增加用户类型或用户标签，分析不同用户群体的路径偏好
 *    - 添加时间指标：记录页面停留时间，评估路径的时间效率
 *    - 扩展为多步路径：记录3步或更长的典型路径，支持更复杂的路径分析
 */
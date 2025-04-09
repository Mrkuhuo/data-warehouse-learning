-- ===============================================================================
-- 用户留存率报表(ads_user_retention)
-- 功能描述：统计不同日期新增用户的留存情况
-- 数据来源：dws_user_user_login_td
-- 刷新策略：每日刷新
-- 应用场景：用户留存分析、产品质量评估、用户生命周期分析
-- ===============================================================================

-- DROP TABLE IF EXISTS ads.ads_user_retention;
CREATE  TABLE ads.ads_user_retention
(
    `dt`               VARCHAR(255) COMMENT '统计日期',
    `create_date`     STRING COMMENT '用户新增日期',
    `retention_day`   INT COMMENT '截至当前日期留存天数',
    `retention_count` BIGINT COMMENT '留存用户数量',
    `new_user_count`  BIGINT COMMENT '新增用户数量',
    `retention_rate`  DECIMAL(16, 2) COMMENT '留存率'
)
    ENGINE=OLAP
DUPLICATE KEY(`dt`)
COMMENT '用户留存率'
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
 *    本表是用户留存分析的核心报表，统计各批次新增用户在不同时间点的留存情况，
 *    帮助产品和运营团队评估产品粘性和用户价值，优化用户体验和运营策略，提升长期留存率。
 *
 * 2. 数据组成与维度：
 *    - 时间维度：包含统计日期、用户新增日期和留存天数，支持多角度的留存分析
 *    - 用户规模指标：包含新增用户数和留存用户数，用于计算留存率
 *    - 留存指标：直接提供计算好的留存率，便于分析
 *
 * 3. 关键指标解读：
 *    - 新增用户数：特定日期新增的用户总数，是留存率计算的基数
 *    - 留存用户数：特定批次用户在n天后仍然活跃的用户数量
 *    - 留存率：留存用户数/新增用户数，反映用户留存情况
 *    - 留存衰减曲线：可通过查询同一批次用户在不同留存天数的留存率，绘制衰减曲线
 *
 * 4. 典型应用场景：
 *    - 产品质量评估：通过留存率评估产品的用户体验和核心价值
 *    - 版本迭代分析：比较不同版本上线前后的用户留存变化，评估迭代效果
 *    - 用户生命周期管理：基于留存曲线特征，制定不同阶段的用户运营策略
 *    - 活动效果评估：分析活动期间获取的用户留存情况，评估活动质量
 *    - 渠道质量分析：结合渠道数据，比较不同渠道用户的留存表现
 *    - 用户价值预测：基于早期留存率预测用户的长期价值和生命周期
 *
 * 5. 查询示例：
 *    - 查询特定批次用户的留存曲线：
 *      SELECT retention_day, retention_rate
 *      FROM ads.ads_user_retention
 *      WHERE dt = '${yesterday}' 
 *      AND create_date = '${batch_date}'
 *      ORDER BY retention_day;
 *    
 *    - 比较不同批次用户的次日留存率：
 *      SELECT create_date, new_user_count, retention_rate
 *      FROM ads.ads_user_retention
 *      WHERE dt BETWEEN DATE_ADD(create_date, 1) AND DATE_ADD(create_date, 1)
 *      AND create_date BETWEEN DATE_SUB('${yesterday}', 30) AND DATE_SUB('${yesterday}', 1)
 *      ORDER BY create_date;
 *
 *    - 计算最近30天的平均留存率：
 *      SELECT retention_day, AVG(retention_rate) AS avg_retention_rate
 *      FROM ads.ads_user_retention
 *      WHERE create_date BETWEEN DATE_SUB('${yesterday}', 60) AND DATE_SUB('${yesterday}', 30)
 *      GROUP BY retention_day
 *      ORDER BY retention_day;
 *
 *    - 分析周末与工作日新增用户的留存差异：
 *      SELECT 
 *        CASE WHEN DAYOFWEEK(create_date) IN (1,7) THEN '周末' ELSE '工作日' END AS day_type,
 *        retention_day,
 *        AVG(retention_rate) AS avg_retention_rate
 *      FROM ads.ads_user_retention
 *      WHERE create_date BETWEEN DATE_SUB('${yesterday}', 90) AND DATE_SUB('${yesterday}', 1)
 *      GROUP BY day_type, retention_day
 *      ORDER BY day_type, retention_day;
 *
 * 6. 建议扩展：
 *    - 添加用户分层：增加不同价值层级用户的留存情况
 *    - 添加渠道维度：增加不同获客渠道的用户留存分析
 *    - 添加用户属性：增加不同属性用户的留存特征
 *    - 添加周留存和月留存：增加更长周期的留存分析
 *    - 添加留存预测：基于历史留存曲线，预测未来留存情况
 */
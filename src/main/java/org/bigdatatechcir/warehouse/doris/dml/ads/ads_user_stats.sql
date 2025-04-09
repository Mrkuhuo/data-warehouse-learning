-- ===============================================================================
-- 用户新增活跃统计报表(ads_user_stats)
-- 功能描述：统计平台用户的新增和活跃情况
-- 数据来源：dws_user_user_login_td
-- 刷新策略：每日刷新
-- 应用场景：用户增长分析、活跃度监控、用户运营决策
-- ===============================================================================

-- DROP TABLE IF EXISTS ads.ads_user_stats;
CREATE  TABLE ads.ads_user_stats
(
    `dt`               VARCHAR(255) COMMENT '统计日期',
    `recent_days`       BIGINT COMMENT '最近n日,1:最近1日,7:最近7日,30:最近30日',
    `new_user_count`    BIGINT COMMENT '新增用户数',
    `active_user_count` BIGINT COMMENT '活跃用户数'
)
    ENGINE=OLAP
DUPLICATE KEY(`dt`)
COMMENT '用户新增活跃统计'
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
 *    本表是用户增长分析的基础报表，统计平台用户的新增和活跃情况，
 *    帮助业务团队监控用户规模变化，评估用户增长健康度，指导用户运营策略调整。
 *
 * 2. 数据组成与维度：
 *    - 时间维度：统计日期和时间周期(1日/7日/30日)，支持不同时间粒度的用户分析
 *    - 用户规模指标：包含新增用户数和活跃用户数两个核心指标
 *
 * 3. 关键指标解读：
 *    - 新增用户数：特定时间段内首次使用产品的用户数量，反映获客能力
 *    - 活跃用户数：特定时间段内有活跃行为的用户数量，反映用户活跃度
 *    - 日活/月活比：可通过计算不同周期的活跃用户比例得出，反映用户黏性
 *    - 新用户占比：可通过计算 new_user_count/active_user_count 得出，反映用户构成
 *
 * 4. 典型应用场景：
 *    - 用户增长监控：监控日常用户新增和活跃趋势，评估业务健康度
 *    - 活动效果评估：分析活动期间用户新增和活跃变化，评估活动拉新和促活效果
 *    - 用户构成分析：分析活跃用户中新用户占比，评估新老用户结构
 *    - 季节性分析：分析不同时期的用户增长和活跃模式，把握用户行为规律
 *    - 增长瓶颈诊断：结合留存和流失数据，诊断用户增长瓶颈
 *    - 产品迭代评估：对比产品迭代前后的用户活跃变化，评估迭代效果
 *
 * 5. 查询示例：
 *    - 分析最近30天的用户增长趋势：
 *      SELECT dt, new_user_count, active_user_count
 *      FROM ads.ads_user_stats
 *      WHERE recent_days = 1
 *      AND dt BETWEEN DATE_SUB('${yesterday}', 29) AND '${yesterday}'
 *      ORDER BY dt;
 *    
 *    - 计算不同周期的用户活跃指标：
 *      SELECT recent_days, 
 *             new_user_count, 
 *             active_user_count,
 *             new_user_count/active_user_count AS new_user_ratio
 *      FROM ads.ads_user_stats
 *      WHERE dt = '${yesterday}'
 *      ORDER BY recent_days;
 *
 *    - 计算周度用户增长汇总：
 *      SELECT DATE_FORMAT(dt, '%Y-%u') AS week,
 *             SUM(new_user_count) AS weekly_new_users,
 *             AVG(active_user_count) AS avg_dau
 *      FROM ads.ads_user_stats
 *      WHERE recent_days = 1
 *      AND dt BETWEEN DATE_SUB('${yesterday}', 89) AND '${yesterday}'
 *      GROUP BY DATE_FORMAT(dt, '%Y-%u')
 *      ORDER BY week;
 *
 * 6. 建议扩展：
 *    - 添加留存维度：增加次日/7日留存用户数，直接评估用户质量
 *    - 添加渠道维度：增加不同渠道的用户新增数据，评估渠道效果
 *    - 添加用户属性：增加用户属性分布，如地域、设备类型等
 *    - 添加行为质量：增加人均访问时长、访问页面数等活跃质量指标
 *    - 添加同环比指标：增加环比和同比增长率，更直观地反映变化趋势
 */
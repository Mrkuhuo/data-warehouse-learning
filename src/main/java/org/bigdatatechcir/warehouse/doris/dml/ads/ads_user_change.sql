-- ===============================================================================
-- 用户变动统计报表(ads_user_change)
-- 功能描述：统计用户流失和回流情况
-- 数据来源：dws_user_user_login_td
-- 刷新策略：每日刷新
-- 应用场景：用户生命周期管理、用户流失预警、回流用户分析
-- ===============================================================================

-- DROP TABLE IF EXISTS ads.ads_user_change;
CREATE  TABLE ads.ads_user_change
(
    `dt`               VARCHAR(255) COMMENT '统计日期',
    `user_churn_count` BIGINT COMMENT '流失用户数',
    `user_back_count`  BIGINT COMMENT '回流用户数'
)
    ENGINE=OLAP
DUPLICATE KEY(`dt`)
COMMENT '用户变动统计'
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
 *    本表是用户生命周期管理的核心报表，统计用户流失和回流情况，
 *    帮助运营团队监控用户变动趋势，及时发现流失风险，评估回流策略效果，优化用户留存。
 *
 * 2. 数据组成与维度：
 *    - 时间维度：统计日期，可用于分析日常用户流失和回流趋势
 *    - 用户变动指标：包含流失用户数和回流用户数两个核心指标
 *
 * 3. 关键指标解读：
 *    - 流失用户数：当日被判定为流失的用户数量，通常指一段时间(如30天)未活跃的用户
 *    - 回流用户数：当日重新活跃的曾经流失用户数量
 *    - 净流失用户数：可通过计算 user_churn_count - user_back_count 得出，反映用户规模净变化
 *    - 回流率：可通过计算 user_back_count / (之前累计流失用户总数) 得出，反映回流策略有效性
 *
 * 4. 典型应用场景：
 *    - 用户流失监控：跟踪日常用户流失趋势，发现异常流失情况
 *    - 用户回流分析：评估召回策略效果，分析哪些措施更有效地带回流失用户
 *    - 用户生命周期管理：结合获客、活跃和留存数据，构建完整的用户生命周期管理体系
 *    - 季节性分析：分析不同时期的用户流失和回流规律，制定相应的用户维护策略
 *    - 活动效果评估：分析特定召回活动前后的用户回流变化，评估活动效果
 *    - 用户资产评估：评估平台的用户资产变化情况，为业务决策提供依据
 *
 * 5. 查询示例：
 *    - 分析最近30天的用户流失和回流趋势：
 *      SELECT dt, 
 *             user_churn_count, 
 *             user_back_count,
 *             user_churn_count - user_back_count AS net_loss
 *      FROM ads.ads_user_change
 *      WHERE dt BETWEEN DATE_SUB('${yesterday}', 29) AND '${yesterday}'
 *      ORDER BY dt;
 *    
 *    - 计算周度用户流失和回流汇总：
 *      SELECT DATE_FORMAT(dt, '%Y-%u') AS week,
 *             SUM(user_churn_count) AS weekly_churn,
 *             SUM(user_back_count) AS weekly_back,
 *             SUM(user_back_count)/SUM(user_churn_count) AS back_ratio
 *      FROM ads.ads_user_change
 *      WHERE dt BETWEEN DATE_SUB('${yesterday}', 89) AND '${yesterday}'
 *      GROUP BY DATE_FORMAT(dt, '%Y-%u')
 *      ORDER BY week;
 *
 * 6. 建议扩展：
 *    - 添加用户分层：增加不同价值层级用户的流失和回流情况
 *    - 添加流失原因：增加流失用户的最后行为和可能的流失原因分类
 *    - 添加回流渠道：增加回流用户的召回渠道，评估不同召回策略效果
 *    - 添加用户属性：增加用户的属性特征，分析不同类型用户的流失风险
 *    - 添加预测指标：基于用户行为模式预测可能流失的用户数量
 */
-- ===============================================================================
-- 商品关联分析报表(ads_sku_association_analysis)
-- 功能描述：分析用户购买行为中的商品关联关系，发现频繁组合购买的商品集合
-- 数据来源：dws_trade_user_sku_order_nd等
-- 刷新策略：每日刷新
-- 应用场景：商品推荐、搭配销售策略、商品陈列优化、营销活动设计
-- ===============================================================================

-- DROP TABLE IF EXISTS ads.ads_sku_association_analysis;
CREATE TABLE ads.ads_sku_association_analysis
(
    `dt`                        VARCHAR(255) COMMENT '统计日期',
    `recent_days`               BIGINT COMMENT '最近天数,7:最近7天,30:最近30天,90:最近90天',
    `source_sku_id`             VARCHAR(255) COMMENT '来源商品ID',
    `source_sku_name`           STRING COMMENT '来源商品名称',
    `source_category1_id`       STRING COMMENT '来源商品一级类目ID',
    `source_category1_name`     STRING COMMENT '来源商品一级类目名称',
    `target_sku_id`             VARCHAR(255) COMMENT '目标商品ID',
    `target_sku_name`           STRING COMMENT '目标商品名称',
    `target_category1_id`       STRING COMMENT '目标商品一级类目ID',
    `target_category1_name`     STRING COMMENT '目标商品一级类目名称',
    `co_purchase_count`         BIGINT COMMENT '共同购买次数',
    `co_purchase_user_count`    BIGINT COMMENT '共同购买用户数',
    `support`                   DECIMAL(10, 4) COMMENT '支持度 - 共同购买次数/总订单数',
    `confidence`                DECIMAL(10, 4) COMMENT '置信度 - 购买来源商品后购买目标商品的概率',
    `lift`                      DECIMAL(10, 4) COMMENT '提升度 - 规则的有效性度量',
    `sequence_pattern`          STRING COMMENT '购买序列模式(同时/先后)',
    `time_interval_avg`         DECIMAL(16, 2) COMMENT '平均购买时间间隔(小时)',
    `association_strength`      STRING COMMENT '关联强度(强/中/弱)',
    `recommendation_score`      DECIMAL(10, 2) COMMENT '推荐分数'
)
ENGINE=OLAP
DUPLICATE KEY(`dt`, `recent_days`, `source_sku_id`)
COMMENT '商品关联分析报表'
DISTRIBUTED BY HASH(`dt`, `recent_days`, `source_sku_id`)
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
 *    本表是商品关联关系分析的核心报表，通过分析用户购买行为中的商品组合模式，
 *    发现商品间的关联规则和搭配关系，为商品推荐系统、搭配销售策略和商品陈列优化提供数据支持。
 *
 * 2. 分析原理与方法：
 *    - 数据基础：基于用户的历史购买行为，分析同一订单或同一用户不同订单中的商品组合
 *    - 关联规则：应用购物篮分析(Market Basket Analysis)和关联规则挖掘算法(如Apriori算法)
 *    - 评估指标：使用支持度(Support)、置信度(Confidence)和提升度(Lift)评估关联规则强度
 *    - 时序分析：区分同时购买和先后购买的序列模式，分析购买时间间隔
 *    - 综合评分：通过多维度指标综合计算推荐分数，用于商品推荐排序
 *
 * 3. 关键指标解读：
 *    - 支持度(Support)：反映规则覆盖面，计算公式为共同购买次数/总订单数
 *    - 置信度(Confidence)：反映规则准确性，计算公式为共同购买次数/来源商品购买次数
 *    - 提升度(Lift)：反映规则有效性，计算公式为置信度/目标商品单独购买概率
 *      * Lift > 1：正相关，两商品互相促进购买
 *      * Lift = 1：独立无关
 *      * Lift < 1：负相关，两商品互相排斥
 *    - 关联强度：综合支持度、置信度和提升度的分类结果
 *    - 推荐分数：综合各指标的加权计算结果，用于最终排序
 *
 * 4. 典型应用场景：
 *    - 商品推荐系统：支持"购买了这个还购买了"、"经常一起购买"等推荐场景
 *    - 搭配销售策略：发现互补性强的商品组合，设计捆绑销售方案
 *    - 商品陈列优化：指导线上商品详情页推荐位和线下商品陈列布局
 *    - 促销活动设计：为满减、满赠、套餐等促销活动提供商品组合建议
 *    - 库存管理优化：基于关联购买预测商品库存需求
 *    - 交叉销售策略：利用商品关联关系设计交叉销售和向上销售策略
 *
 * 5. 查询示例：
 *    - 查询某商品的最佳推荐组合：
 *      SELECT target_sku_name, confidence, lift, recommendation_score
 *      FROM ads.ads_sku_association_analysis
 *      WHERE dt = '${yesterday}' AND recent_days = 30
 *      AND source_sku_id = '${sku_id}'
 *      ORDER BY recommendation_score DESC
 *      LIMIT 10;
 *    
 *    - 查找跨品类强关联商品：
 *      SELECT source_category1_name, target_category1_name, 
 *             COUNT(*) AS pair_count, AVG(lift) AS avg_lift
 *      FROM ads.ads_sku_association_analysis
 *      WHERE dt = '${yesterday}' AND recent_days = 90
 *      AND source_category1_id != target_category1_id
 *      AND lift > 2
 *      GROUP BY source_category1_name, target_category1_name
 *      ORDER BY avg_lift DESC;
 *
 *    - 查找同时购买vs.先后购买的模式差异：
 *      SELECT sequence_pattern, 
 *             COUNT(*) AS rule_count,
 *             AVG(confidence) AS avg_confidence,
 *             AVG(lift) AS avg_lift
 *      FROM ads.ads_sku_association_analysis
 *      WHERE dt = '${yesterday}' AND recent_days = 30
 *      GROUP BY sequence_pattern;
 *
 * 6. 实现说明：
 *    - 数据来源：主要基于用户SKU粒度的订单数据计算
 *    - 计算周期：支持7天、30天、90天三种时间窗口，反映不同周期的关联模式
 *    - 计算方法：使用SparkSQL或Doris内置的窗口函数实现关联规则挖掘
 *    - 性能考虑：对于高流量商品，可能产生大量关联记录，因此设置最低支持度阈值过滤
 *    - 更新策略：每日全量刷新，历史数据可通过dt字段访问
 *
 * 7. 商品推荐算法补充说明：
 *    - 基于此表的推荐算法是协同过滤(Collaborative Filtering)的一种变体
 *    - 相比传统的基于用户的协同过滤，此方法不需要计算用户相似度，实现更简单高效
 *    - 推荐分数计算公式：a*confidence + b*lift + c*recency_factor，其中a,b,c为权重参数
 *    - 为避免推荐结果中出现过多相似商品，实际应用中会结合商品属性进行多样性优化
 */ 
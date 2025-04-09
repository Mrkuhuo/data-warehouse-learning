-- ===============================================================================
-- 销售趋势预测报表(ads_sales_trend_forecast)
-- 功能描述：基于历史销售数据，对未来一段时间的销售趋势进行预测分析
-- 数据来源：dws_trade_sku_order_1d、dws_trade_user_order_nd等
-- 刷新策略：每日刷新
-- 应用场景：销售预测、库存规划、促销活动规划、资源调配决策
-- ===============================================================================

-- DROP TABLE IF EXISTS ads.ads_sales_trend_forecast;
CREATE TABLE ads.ads_sales_trend_forecast
(
    `dt`                        VARCHAR(255) COMMENT '统计日期',
    `forecast_date`             VARCHAR(255) COMMENT '预测日期',
    `forecast_type`             STRING COMMENT '预测类型(整体/品类/品牌/区域)',
    `dimension_id`              STRING COMMENT '维度ID(品类ID/品牌ID/区域ID等)',
    `dimension_name`            STRING COMMENT '维度名称',
    `forecast_order_count`      BIGINT COMMENT '预测订单量',
    `forecast_order_amount`     DECIMAL(16, 2) COMMENT '预测销售额',
    `forecast_user_count`       BIGINT COMMENT '预测购买用户数',
    `forecast_interval_lower`   DECIMAL(16, 2) COMMENT '预测区间下限',
    `forecast_interval_upper`   DECIMAL(16, 2) COMMENT '预测区间上限',
    `confidence_level`          DECIMAL(10, 2) COMMENT '预测置信度',
    `historical_avg_amount`     DECIMAL(16, 2) COMMENT '历史平均销售额',
    `seasonal_index`            DECIMAL(10, 2) COMMENT '季节性指数',
    `trend_coefficient`         DECIMAL(10, 2) COMMENT '趋势系数',
    `prediction_model`          STRING COMMENT '预测模型(ARIMA/Prophet/LSTM等)',
    `anomaly_flag`              BOOLEAN COMMENT '异常标记',
    `expected_growth_rate`      DECIMAL(10, 2) COMMENT '预期增长率'
)
ENGINE=OLAP
DUPLICATE KEY(`dt`, `forecast_date`)
COMMENT '销售趋势预测报表'
DISTRIBUTED BY HASH(`dt`, `forecast_date`)
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
 *    本表是面向未来的预测性分析报表，基于历史销售数据应用时间序列分析和机器学习算法，
 *    对未来一段时间（通常为未来7天、30天、90天）的销售趋势进行预测，为业务决策提供前瞻性指导。
 *
 * 2. 预测机制与维度：
 *    - 多维度预测：支持整体、品类、品牌、区域等多个维度的销售预测
 *    - 多指标预测：预测订单量、销售额、用户数等核心业务指标
 *    - 时间粒度：通常按日预测，但也可聚合为周度、月度预测
 *    - 预测区间：同时给出点预测值和置信区间，全面反映预测的确定性
 *    - 模型多样化：根据数据特性选择不同的预测模型，如ARIMA、Prophet、LSTM等
 *
 * 3. 关键字段说明：
 *    - 预测结果字段：包括forecast_order_count、forecast_order_amount等预测指标
 *    - 预测区间字段：forecast_interval_lower和forecast_interval_upper定义预测的可能范围
 *    - 预测质量字段：confidence_level反映预测的可信度
 *    - 历史参考字段：historical_avg_amount提供历史基准
 *    - 趋势分解字段：seasonal_index和trend_coefficient分解时间序列特征
 *    - 模型信息字段：prediction_model标识使用的预测模型
 *    - 异常标记字段：anomaly_flag标识预测是否存在异常
 *
 * 4. 典型应用场景：
 *    - 销售预测与目标设定：基于预测制定合理的销售目标和KPI
 *    - 库存管理优化：根据销售预测调整库存水平，减少缺货和积压
 *    - 促销活动规划：针对预测销售低谷期进行促销活动规划
 *    - 人力资源调配：基于销售预测优化客服、物流等人力资源配置
 *    - 现金流管理：通过预测销售额辅助财务部门进行现金流规划
 *    - 异常预警：发现与预期显著偏离的销售趋势，提前干预
 *
 * 5. 查询示例：
 *    - 查询未来7天的整体销售预测：
 *      SELECT forecast_date, forecast_order_count, forecast_order_amount,
 *             forecast_interval_lower, forecast_interval_upper
 *      FROM ads.ads_sales_trend_forecast
 *      WHERE dt = '${yesterday}' AND forecast_type = '整体'
 *      AND forecast_date BETWEEN '${tomorrow}' AND DATE_ADD('${tomorrow}', 6)
 *      ORDER BY forecast_date;
 *    
 *    - 查询品类销售异常预警：
 *      SELECT dimension_name, forecast_date, forecast_order_amount, 
 *             historical_avg_amount, expected_growth_rate
 *      FROM ads.ads_sales_trend_forecast
 *      WHERE dt = '${yesterday}' AND forecast_type = '品类'
 *      AND anomaly_flag = true
 *      ORDER BY forecast_date, dimension_name;
 *
 *    - 比较不同维度的预测增长趋势：
 *      SELECT forecast_type, dimension_name, 
 *             AVG(expected_growth_rate) AS avg_growth_rate
 *      FROM ads.ads_sales_trend_forecast
 *      WHERE dt = '${yesterday}' 
 *      AND forecast_date BETWEEN '${nextWeekStart}' AND '${nextWeekEnd}'
 *      GROUP BY forecast_type, dimension_name
 *      ORDER BY avg_growth_rate DESC;
 *
 * 6. 预测模型说明：
 *    - 时间序列分解：将销售数据分解为趋势、季节性和残差三个组件
 *    - 季节性指数：反映周期性波动的强度，例如1.2表示该时间点通常比平均高20%
 *    - 趋势系数：反映长期增长或下降趋势的斜率
 *    - 模型选择逻辑：系统会根据历史数据的特性自动选择最适合的预测模型
 *    - 置信区间：基于历史预测准确度和数据波动性自动计算
 *    - 异常判定：当预测值与历史同期数据偏差超过阈值时标记为异常
 */ 
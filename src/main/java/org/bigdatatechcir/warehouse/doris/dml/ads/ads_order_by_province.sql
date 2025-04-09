-- ===============================================================================
-- 各地区订单统计报表(ads_order_by_province)
-- 功能描述：统计各省份订单量和订单金额
-- 数据来源：dws_trade_province_order_nd
-- 刷新策略：每日刷新
-- 应用场景：区域销售分析、销售地域分布、区域市场洞察
-- ===============================================================================

-- DROP TABLE IF EXISTS ads.ads_order_by_province;
CREATE  TABLE ads.ads_order_by_province
(
    `dt`          VARCHAR(255) COMMENT '统计日期',
    `recent_days`        BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `province_id`        STRING COMMENT '省份ID',
    `province_name`      STRING COMMENT '省份名称',
    `area_code`          STRING COMMENT '地区编码',
    `iso_code`           STRING COMMENT '国际标准地区编码',
    `iso_code_3166_2`    STRING COMMENT '国际标准地区编码',
    `order_count`        BIGINT COMMENT '订单数',
    `order_total_amount` DECIMAL(16, 2) COMMENT '订单金额'
)
    ENGINE=OLAP
DUPLICATE KEY(`dt`)
COMMENT '各地区订单统计'
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
 *    本表是区域销售分析的核心报表，统计各省份的订单数量和销售金额，
 *    帮助业务团队了解销售的地域分布特点，发现区域市场机会和问题，支持区域营销策略制定。
 *
 * 2. 数据组成与维度：
 *    - 时间维度：统计日期和时间周期(1天/7天/30天)，支持不同时间粒度的地域分析
 *    - 地理维度：包含省份ID、名称和多种地区编码，支持多种地理标准的分析和可视化
 *    - 业务指标：包含订单数和订单金额两个核心指标，反映区域业务规模
 *
 * 3. 关键指标解读：
 *    - 订单数：区域内产生的订单总数，反映区域交易活跃度
 *    - 订单金额：区域内产生的订单总金额，反映区域销售贡献
 *    - 平均客单价：可通过计算 order_total_amount/order_count 得出，反映区域消费能力
 *
 * 4. 典型应用场景：
 *    - 区域销售排名：发现销售表现最好和最差的地区，指导资源分配
 *    - 销售地图可视化：结合GIS工具创建销售热力图，直观展示销售地域分布
 *    - 区域市场洞察：结合人口、GDP等外部数据，分析区域市场渗透率和潜力
 *    - 区域销售趋势：分析不同地区销售的增长或下降趋势，预测未来发展
 *    - 区域营销策略：针对不同区域特点，制定差异化的营销和推广策略
 *
 * 5. 查询示例：
 *    - 查询销售额前十的省份：
 *      SELECT province_name, order_count, order_total_amount
 *      FROM ads.ads_order_by_province
 *      WHERE dt = '${yesterday}' AND recent_days = 30
 *      ORDER BY order_total_amount DESC
 *      LIMIT 10;
 *    
 *    - 计算各省份平均客单价并排名：
 *      SELECT province_name, 
 *             order_count, 
 *             order_total_amount,
 *             order_total_amount/order_count AS avg_order_amount
 *      FROM ads.ads_order_by_province
 *      WHERE dt = '${yesterday}' AND recent_days = 30
 *      ORDER BY avg_order_amount DESC;
 *
 * 6. 建议扩展：
 *    - 添加用户维度：增加下单用户数，计算用户渗透率
 *    - 添加同环比指标：增加环比和同比增长率，反映区域销售趋势
 *    - 添加订单属性：增加平均订单商品数、退单率等维度
 *    - 细化地理粒度：增加城市维度的销售统计
 *    - 添加人口归一化指标：增加人均消费等指标，消除人口基数影响
 */
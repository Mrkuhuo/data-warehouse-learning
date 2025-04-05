/*
 * 表名: dws_trade_province_order_1d
 * 说明: 交易域省份粒度订单最近1日汇总事实表
 * 数据粒度: 省份 + 日期
 * 存储策略: 分区存储，按日期分区
 * 刷新周期: 每日刷新
 * 应用场景:
 *   - 区域销售分析：按省份统计销售业绩
 *   - 区域市场洞察：发现各地区销售特点和趋势
 *   - 营销策略制定：针对不同地区制定差异化营销策略
 *   - 区域绩效评估：评估不同省份的销售表现
 */

-- DROP TABLE IF EXISTS dws.dws_trade_province_order_1d;
CREATE  TABLE dws.dws_trade_province_order_1d
(
    /* 维度字段 */
    `province_id`               VARCHAR(255) COMMENT '省份ID - 唯一标识特定省份',
    `k1`                        DATE NOT NULL   COMMENT '分区字段 - 数据日期，用于分区管理',
    
    /* 冗余维度 - 用于提高分析效率，避免关联查询 */
    `province_name`             STRING COMMENT '省份名称 - 省份的中文名称',
    `area_code`                 STRING COMMENT '地区编码 - 省份电话区号',
    `iso_code`                  STRING COMMENT '旧版ISO-3166-2编码 - 国际标准化组织定义的地理编码',
    `iso_3166_2`                STRING COMMENT '新版ISO-3166-2编码 - 更新的国际标准地理编码',
    
    /* 度量值字段 - 1日汇总 */
    `order_count_1d`            BIGINT COMMENT '最近1日下单次数 - 该省份一天内的订单总数',
    `order_original_amount_1d`  DECIMAL(16, 2) COMMENT '最近1日下单原始金额 - 该省份一天内订单的原始总金额',
    `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT '最近1日下单活动优惠金额 - 该省份一天内活动带来的优惠总金额',
    `coupon_reduce_amount_1d`   DECIMAL(16, 2) COMMENT '最近1日下单优惠券优惠金额 - 该省份一天内优惠券带来的优惠总金额',
    `order_total_amount_1d`     DECIMAL(16, 2) COMMENT '最近1日下单最终金额 - 该省份一天内优惠后的实际支付总金额'
)
    ENGINE=OLAP
UNIQUE KEY(`province_id`,`k1`)
COMMENT '交易域省份粒度订单最近1日汇总事实表'
PARTITION BY RANGE(`k1`) ()
DISTRIBUTED BY HASH(`province_id`)
PROPERTIES
(
    "replication_allocation" = "tag.location.default: 1",
    "is_being_synced" = "false",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false",
    
    /* 动态分区配置 - 自动管理历史数据 */
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-60",    /* 保留60天历史数据 */
    "dynamic_partition.end" = "3",        /* 预创建未来3天分区 */
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "32",
    "dynamic_partition.create_history_partition" = "true"
);

/*
 * 表设计说明：
 * 
 * 1. 主键设计：
 *    - 使用省份ID + 日期作为联合主键，确保每个省份每天只有一条汇总记录
 *    - 这种设计支持按天查询特定省份的销售表现，也支持按天比较不同省份
 *
 * 2. 分区和分桶策略：
 *    - 按日期分区，支持高效的历史数据管理和按时间范围的查询
 *    - 使用省份ID作为分桶键，优化并行查询性能，特别适合全国销售分析场景
 *
 * 3. 字段选择考量：
 *    - 包含省份的基本属性和标准编码，便于与外部系统数据对接
 *    - 汇总字段设计为1日周期，适合日常销售分析和监控
 *    - 包含多种金额指标（原始金额、优惠金额、最终金额），支持全面的销售分析
 *
 * 4. 典型使用场景：
 *    - 省份销售排名：基于订单金额或订单数对省份进行排名
 *    - 区域优惠分析：分析不同地区的优惠效果差异
 *    - 销售地图可视化：结合地图展示全国销售热力分布
 *    - 区域销售同比环比分析：比较不同时期各省份的销售变化
 *
 * 5. 扩展建议：
 *    - 考虑添加下单用户数指标，了解各省份的用户活跃度
 *    - 可细分为城市粒度，提供更精细的地域分析
 *    - 结合人口数据，计算人均消费指标，更准确评估区域潜力
 */
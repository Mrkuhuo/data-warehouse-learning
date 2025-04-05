/*
 * 表名: dws_trade_province_order_nd
 * 说明: 交易域省份粒度订单最近n日汇总事实表
 * 数据粒度: 省份 + 日期
 * 存储策略: 分区存储，按日期分区
 * 刷新周期: 每日刷新
 * 应用场景:
 *   - 区域销售趋势分析：按省份统计中长期销售趋势
 *   - 区域营销效果评估：评估省份层面的营销活动效果
 *   - 区域业务健康度监控：监控各省份销售稳定性和增长性
 *   - 区域销售预测：基于历史数据进行区域销售预测
 */

-- DROP TABLE IF EXISTS dws.dws_trade_province_order_nd;
CREATE  TABLE dws.dws_trade_province_order_nd
(
    /* 维度字段 */
    `province_id`               VARCHAR(255) COMMENT '省份ID - 唯一标识特定省份',
    `k1`                        DATE NOT NULL   COMMENT '分区字段 - 数据日期，用于分区管理',
    
    /* 冗余维度 - 用于提高分析效率，避免关联查询 */
    `province_name`              STRING COMMENT '省份名称 - 省份的中文名称',
    `area_code`                  STRING COMMENT '地区编码 - 省份电话区号',
    `iso_code`                   STRING COMMENT '旧版ISO-3166-2编码 - 国际标准化组织定义的地理编码',
    `iso_3166_2`                 STRING COMMENT '新版ISO-3166-2编码 - 更新的国际标准地理编码',
    
    /* 度量值字段 - 7日汇总 */
    `order_count_7d`             BIGINT COMMENT '最近7日下单次数 - 该省份7天内的订单总数',
    `order_original_amount_7d`   DECIMAL(16, 2) COMMENT '最近7日下单原始金额 - 该省份7天内订单的原始总金额',
    `activity_reduce_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日下单活动优惠金额 - 该省份7天内活动带来的优惠总金额',
    `coupon_reduce_amount_7d`    DECIMAL(16, 2) COMMENT '最近7日下单优惠券优惠金额 - 该省份7天内优惠券带来的优惠总金额',
    `order_total_amount_7d`      DECIMAL(16, 2) COMMENT '最近7日下单最终金额 - 该省份7天内优惠后的实际支付总金额',
    
    /* 度量值字段 - 30日汇总 */
    `order_count_30d`            BIGINT COMMENT '最近30日下单次数 - 该省份30天内的订单总数',
    `order_original_amount_30d`  DECIMAL(16, 2) COMMENT '最近30日下单原始金额 - 该省份30天内订单的原始总金额',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '最近30日下单活动优惠金额 - 该省份30天内活动带来的优惠总金额',
    `coupon_reduce_amount_30d`   DECIMAL(16, 2) COMMENT '最近30日下单优惠券优惠金额 - 该省份30天内优惠券带来的优惠总金额',
    `order_total_amount_30d`     DECIMAL(16, 2) COMMENT '最近30日下单最终金额 - 该省份30天内优惠后的实际支付总金额'
)
    ENGINE=OLAP
UNIQUE KEY(`province_id`,`k1`)
COMMENT '交易域省份粒度订单最近n日汇总事实表'
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
 *    - 这种设计支持按天查询特定省份的历史销售表现，也支持跨省份比较
 *
 * 2. 分区和分桶策略：
 *    - 按日期分区，支持高效的历史数据管理和按时间范围的查询
 *    - 使用省份ID作为分桶键，优化并行查询性能，适合全国范围的销售分析场景
 *
 * 3. 字段选择考量：
 *    - 包含省份的基本属性和标准编码，便于与外部系统数据对接
 *    - 设计7日和30日两个时间窗口的汇总指标，分别用于短期和中期趋势分析
 *    - 包含多种金额指标（原始金额、优惠金额、最终金额），支持全面的销售分析
 *
 * 4. 与1日汇总表的关系：
 *    - 本表基于dws_trade_province_order_1d表聚合而来，提供更长时间跨度的汇总数据
 *    - 1日表适合日常监控和细粒度分析，本表适合周期性分析和趋势研究
 *    - 两表结合使用，可以提供从日度到月度的全面销售视图
 *
 * 5. 典型使用场景：
 *    - 省份销售趋势分析：观察不同省份销售的稳定性和周期性
 *    - 跨区域营销效果对比：比较不同地区对营销活动的响应差异
 *    - 省份销售预测：基于7日和30日数据预测未来销售走势
 *    - 区域销售热力地图：直观展示全国各省销售热度分布
 *
 * 6. 扩展建议：
 *    - 添加环比和同比指标，便于趋势分析
 *    - 考虑增加下单用户数指标，了解各省份的用户活跃度变化
 *    - 可增加90日指标，支持季度分析
 */
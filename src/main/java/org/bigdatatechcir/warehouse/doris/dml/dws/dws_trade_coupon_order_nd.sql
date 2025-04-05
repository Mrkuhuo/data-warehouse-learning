/*
 * 表名: dws_trade_coupon_order_nd
 * 说明: 交易域优惠券粒度订单最近n日汇总事实表
 * 数据粒度: 优惠券 + 日期
 * 存储策略: 分区存储，按日期分区
 * 刷新周期: 每日刷新
 * 应用场景:
 *   - 优惠券效果分析：评估不同优惠券的销售带动效果
 *   - 优惠策略评估：计算优惠券带来的折扣和实际效益
 *   - 优惠券类型对比：对比不同类型优惠券的转化效果
 *   - 优惠券使用监控：跟踪优惠券使用率和带来的销售额
 */

-- DROP TABLE IF EXISTS dws.dws_trade_coupon_order_nd;
CREATE  TABLE dws.dws_trade_coupon_order_nd
(
    /* 维度字段 */
    `coupon_id`                VARCHAR(255) COMMENT '优惠券ID - 唯一标识特定优惠券',
    `k1`                        DATE NOT NULL   COMMENT '分区字段 - 数据日期，用于分区管理',
    
    /* 冗余维度 - 用于提高分析效率，避免关联查询 */
    `coupon_name`              STRING COMMENT '优惠券名称 - 优惠券的业务描述名称',
    `coupon_type_code`         STRING COMMENT '优惠券类型编码 - 例如满减券、折扣券、现金券等类型的编码',
    `coupon_type_name`         STRING COMMENT '优惠券类型名称 - 优惠券类型的文字描述',
    `coupon_rule`              STRING COMMENT '优惠券规则 - 描述优惠券使用条件和优惠方式的规则文本',
    `start_date`               STRING COMMENT '发布日期 - 优惠券开始生效的日期',
    
    /* 度量值字段 - 30日汇总 */
    `original_amount_30d`      DECIMAL(16, 2) COMMENT '使用下单原始金额 - 30天内使用该优惠券订单的原始总金额',
    `coupon_reduce_amount_30d` DECIMAL(16, 2) COMMENT '使用下单优惠金额 - 30天内该优惠券提供的优惠总金额'
)
    ENGINE=OLAP
UNIQUE KEY(`coupon_id`,`k1`)
COMMENT '交易域优惠券粒度订单最近n日汇总事实表'
PARTITION BY RANGE(`k1`) ()
DISTRIBUTED BY HASH(`coupon_id`)
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
 *    - 使用优惠券ID + 日期作为联合主键，确保每个优惠券每天只有一条汇总记录
 *    - 这种设计支持按天查询特定优惠券的表现，也支持按天比较不同优惠券
 *
 * 2. 分区和分桶策略：
 *    - 按日期分区，支持高效的历史数据管理和按时间范围的查询
 *    - 使用优惠券ID作为分桶键，优化并行查询性能
 *
 * 3. 字段选择考量：
 *    - 包含优惠券的核心属性，如名称、类型和规则，便于分析
 *    - 汇总字段包括原始金额和优惠金额，用于计算优惠券的使用效果和ROI
 *    - 当前仅保留30天汇总数据，可根据需要扩展1天、7天等周期
 *
 * 4. 典型使用场景：
 *    - 计算优惠券转化率：优惠金额 / 原始金额
 *    - 按优惠券类型分析效果差异
 *    - 评估优惠券策略对销售的拉动作用
 *    - 监控优惠券使用的时间趋势
 *
 * 5. 建议扩展指标：
 *    - 优惠券使用次数：记录优惠券被使用的总次数
 *    - 优惠券使用人数：记录使用该优惠券的独立用户数
 *    - 平均客单价：使用优惠券后的实际支付金额/订单数
 */
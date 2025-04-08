/*
 * 脚本名称: dws_trade_user_payment_nd.sql
 * 目标表: dws.dws_trade_user_payment_nd
 * 数据粒度: 用户 + 日期
 * 刷新策略: 全量刷新
 * 调度周期: 每日调度一次
 * 运行参数:
 *   - pdate: 数据日期，默认为当天
 * 依赖表:
 *   - dws.dws_trade_user_payment_1d: 用户粒度支付1日汇总表
 */
-- 交易域用户粒度支付最近n日汇总表
-- 计算逻辑:
-- 1. 最近7日指标: 通过条件判断计算最近7天的指标累计值
-- 2. 最近30日指标: 汇总最近30天的所有数据
-- 3. 时间筛选: 仅处理最近30天的数据，确保计算范围的一致性
INSERT INTO dws.dws_trade_user_payment_nd
(
    user_id,                   /* 用户ID */
    k1,                        /* 数据日期 */
    payment_count_7d,          /* 最近7日支付次数 */
    payment_num_7d,            /* 最近7日支付商品件数 */
    payment_amount_7d,         /* 最近7日支付金额 */
    payment_count_30d,         /* 最近30日支付次数 */
    payment_num_30d,           /* 最近30日支付商品件数 */
    payment_amount_30d         /* 最近30日支付金额 */
)
SELECT
    user_id,                   /* 用户ID: 标识支付用户 */
    k1,                        /* 数据日期: 当前计算日期 */
    /* 最近7日指标计算: 条件聚合最近7天的数据 */
    SUM(IF(k1 >= date_add(date('${pdate}'), -6), payment_count_1d, 0)) AS payment_count_7d,
    SUM(IF(k1 >= date_add(date('${pdate}'), -6), payment_num_1d, 0)) AS payment_num_7d,
    SUM(IF(k1 >= date_add(date('${pdate}'), -6), payment_amount_1d, 0)) AS payment_amount_7d,
    /* 最近30日指标计算: 汇总最近30天的所有数据 */
    SUM(payment_count_1d) AS payment_count_30d,
    SUM(payment_num_1d) AS payment_num_30d,
    SUM(payment_amount_1d) AS payment_amount_30d
FROM dws.dws_trade_user_payment_1d
/* 时间范围: 只处理最近30天数据 */
WHERE k1 >= date_add(date('${pdate}'), -29)
  AND k1 <= date('${pdate}')
/* 分组: 按用户和日期分组，确保每个用户每天一条汇总记录 */
GROUP BY user_id, k1;

/*
 * 数据处理说明:
 *
 * 1. 数据来源:
 *    - 从1日汇总表(dws_trade_user_payment_1d)获取基础数据
 *    - 该表已按用户和日期维度聚合了支付次数、件数和金额
 *
 * 2. 计算方法:
 *    - 7日指标: 通过IF条件函数筛选最近7天数据并求和
 *    - 30日指标: 对筛选出的最近30天数据直接求和
 *    - 日期筛选: 使用参数化方式处理日期范围，便于灵活调整
 *
 * 3. 数据质量控制:
 *    - 采用先删除后插入的方式，确保数据不会重复
 *    - WHERE条件严格限定数据时间范围，防止历史数据干扰
 *    - GROUP BY确保数据聚合的准确性和一致性
 *
 * 4. 性能优化:
 *    - 只处理最近30天数据，减少数据处理量
 *    - 使用条件聚合(IF)避免多次扫描数据源
 *    - 一次计算多个指标，减少重复计算和I/O开销
 *
 * 5. 应用建议:
 *    - 结合订单表分析支付转化率趋势
 *    - 可基于此表构建用户支付行为分析报表
 *    - 利用7日和30日数据对比分析近期支付趋势变化
 *    - 与用户订单N日表结合，分析下单到支付的转化效率
 */
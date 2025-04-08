/*
 * 脚本名称: dws_trade_user_payment_td_first.sql
 * 目标表: dws.dws_trade_user_payment_td
 * 数据粒度: 用户 + 日期
 * 刷新策略: 全量加载历史数据
 * 调度周期: 一次性执行
 * 依赖表:
 *   - dws.dws_trade_user_payment_1d: 用户粒度支付1日汇总表
 */

-- 交易域用户粒度支付历史至今汇总表
-- 计算逻辑: 
-- 1. 一次性加载所有历史数据
-- 2. 按用户和日期分组聚合支付数据
-- 3. 计算用户首次和末次支付日期，以及累计支付统计
INSERT INTO dws.dws_trade_user_payment_td
(
    user_id,                 /* 用户ID */
    k1,                      /* 数据日期 */
    payment_date_first,      /* 首次支付日期 */
    payment_date_last,       /* 末次支付日期 */
    payment_count_td,        /* 累计支付次数 */
    payment_num_td,          /* 累计支付商品件数 */
    payment_amount_td        /* 累计支付金额 */
)
SELECT
    user_id,                             /* 用户ID: 标识支付用户 */
    k1,                                  /* 数据日期: 当前计算日期 */
    MIN(k1) AS payment_date_first,       /* 首次支付日期: 用户最早的支付日期 */
    MAX(k1) AS payment_date_last,        /* 末次支付日期: 用户最近的支付日期 */
    SUM(payment_count_1d) AS payment_count_td,  /* 累计支付次数: 汇总用户所有支付次数 */
    SUM(payment_num_1d) AS payment_num_td,      /* 累计支付商品件数: 汇总用户所有支付商品件数 */
    SUM(payment_amount_1d) AS payment_amount_td /* 累计支付金额: 汇总用户所有支付金额 */
FROM dws.dws_trade_user_payment_1d
/* 分组: 按用户和日期分组，确保每个用户每天一条汇总记录 */
GROUP BY user_id, k1;

/*
 * 数据处理说明:
 *
 * 1. 执行场景:
 *    - 首次构建数据仓库时执行
 *    - 数据修复或重建时执行
 *    - 全量加载所有历史支付数据
 *
 * 2. 数据来源:
 *    - 从1日汇总表(dws_trade_user_payment_1d)获取所有历史数据
 *    - 该表已按用户和日期维度聚合了支付次数、件数和金额
 *
 * 3. 计算方法:
 *    - 首次支付日期: 用户所有支付记录中最早的日期(MIN函数)
 *    - 末次支付日期: 用户所有支付记录中最晚的日期(MAX函数)
 *    - 累计支付指标: 对用户所有日期的支付指标进行求和(SUM函数)
 *
 * 4. 执行建议:
 *    - 确保目标表为空，避免数据重复
 *    - 根据数据量大小，可能需要调整执行资源配置
 *    - 执行完成后，建议验证数据一致性
 *    - 首次加载后，日常维护应使用每日增量加载脚本(dws_trade_user_payment_td_per_day.sql)
 */
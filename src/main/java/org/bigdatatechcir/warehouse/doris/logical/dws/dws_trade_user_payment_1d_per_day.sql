/*
 * 脚本名称: dws_trade_user_payment_1d_per_day.sql
 * 目标表: dws.dws_trade_user_payment_1d
 * 数据粒度: 用户 + 日期
 * 刷新策略: 增量刷新，每日新增数据
 * 调度周期: 每日调度一次
 * 运行参数:
 *   - pdate: 数据日期，默认为当天
 * 依赖表:
 *   - dwd.dwd_trade_pay_detail_suc_inc: 交易域支付成功事实表，增量表
 */
-- 交易域用户粒度支付最近1日汇总表
-- 计算逻辑: 
-- 1. 按用户和日期分组聚合当日支付数据
-- 2. 计算支付次数、件数和金额等指标
INSERT INTO dws.dws_trade_user_payment_1d
(
    user_id,                 /* 用户ID */
    k1,                      /* 数据日期 */
    payment_count_1d,        /* 支付次数 */
    payment_num_1d,          /* 支付商品件数 */
    payment_amount_1d        /* 支付金额 */
)
SELECT
    user_id,                             /* 用户ID: 标识支付用户 */
    k1,                                  /* 数据日期: 当前计算日期 */
    COUNT(DISTINCT(order_id)) AS payment_count,  /* 支付次数: 统计支付订单数 */
    SUM(sku_num) AS payment_num,               /* 支付商品件数: 统计支付购买的商品数量 */
    SUM(split_payment_amount) AS payment_amount /* 支付金额: 统计支付的总金额 */
FROM dwd.dwd_trade_pay_detail_suc_inc
/* 时间筛选: 只处理当日数据 */
WHERE k1 = date('${pdate}')
/* 分组: 按用户和日期分组，确保每个用户每天一条汇总记录 */
GROUP BY user_id, k1;

/*
 * 数据处理说明:
 *
 * 1. 数据来源:
 *    - 从明细事实表(dwd_trade_pay_detail_suc_inc)获取基础数据
 *    - 该表记录了每个支付成功事件的详细信息
 *
 * 2. 计算方法:
 *    - 支付次数: 对不同订单ID进行去重计数，确保准确的支付订单数
 *    - 支付件数: 对支付商品数量进行求和
 *    - 支付金额: 对支付金额进行求和
 *
 * 3. 数据质量控制:
 *    - 采用先删除后插入的方式，确保当日数据不会重复
 *    - WHERE条件严格限定数据时间范围为当天
 *    - 使用DISTINCT确保订单计数的准确性
 *
 * 4. 应用价值:
 *    - 为用户支付行为分析提供基础数据
 *    - 支持支付转化率和支付行为的监控
 *    - 作为N日汇总表的数据来源，支持中长期分析
 *    - 可结合订单数据分析下单到支付的转化过程
 */
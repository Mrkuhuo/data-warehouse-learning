/*
 * 脚本名称: dws_trade_user_payment_1d_first.sql
 * 目标表: dws.dws_trade_user_payment_1d
 * 数据粒度: 用户 + 日期
 * 刷新策略: 全量加载历史数据
 * 调度周期: 一次性执行
 * 依赖表:
 *   - dwd.dwd_trade_pay_detail_suc_inc: 交易域支付成功事实表，增量表
 */

-- 交易域用户粒度支付最近1日汇总表
-- 计算逻辑: 
-- 1. 一次性加载所有历史数据
-- 2. 按用户和日期分组聚合支付数据
-- 3. 计算每日支付次数、件数和金额指标
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
    k1,                                  /* 数据日期: 支付发生日期 */
    COUNT(DISTINCT(order_id)) AS payment_count,  /* 支付次数: 统计支付订单数 */
    SUM(sku_num) AS payment_num,               /* 支付商品件数: 统计支付购买的商品数量 */
    SUM(split_payment_amount) AS payment_amount /* 支付金额: 统计支付的总金额 */
FROM dwd.dwd_trade_pay_detail_suc_inc
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
 *    - 从明细事实表(dwd_trade_pay_detail_suc_inc)获取所有历史数据
 *    - 该表记录了每个支付成功事件的详细信息
 *    - 不做时间范围限制，处理全部可用数据
 *
 * 3. 计算方法:
 *    - 支付次数: 对不同订单ID进行去重计数，确保准确的支付订单数
 *    - 支付件数: 对支付商品数量进行求和
 *    - 支付金额: 对支付金额进行求和
 *
 * 4. 执行建议:
 *    - 确保目标表为空，避免数据重复
 *    - 根据数据量大小，可能需要调整执行资源配置
 *    - 执行完成后，建议验证数据一致性
 *    - 首次加载后，日常维护应使用每日增量加载脚本
 */
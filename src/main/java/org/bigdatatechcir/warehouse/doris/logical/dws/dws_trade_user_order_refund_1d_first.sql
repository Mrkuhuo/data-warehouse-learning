/*
 * 脚本名称: dws_trade_user_order_refund_1d_first.sql
 * 目标表: dws.dws_trade_user_order_refund_1d
 * 数据粒度: 用户 + 日期
 * 刷新策略: 全量加载历史数据
 * 调度周期: 一次性执行
 * 依赖表:
 *   - dwd.dwd_trade_order_refund_inc: 交易域退单事实表，增量表
 */

-- 交易域用户粒度退单最近1日汇总表
-- 计算逻辑: 
-- 1. 一次性加载所有历史数据
-- 2. 按用户和日期分组聚合退单数据
-- 3. 计算每日退单次数、件数和金额指标
INSERT INTO dws.dws_trade_user_order_refund_1d
(
    user_id,                 /* 用户ID */
    k1,                      /* 数据日期 */
    order_refund_count_1d,   /* 退单次数 */
    order_refund_num_1d,     /* 退单商品件数 */
    order_refund_amount_1d   /* 退单金额 */
)
SELECT
    user_id,                            /* 用户ID: 标识退单行为的用户 */
    k1,                                 /* 数据日期: 退单发生日期 */
    COUNT(*) AS order_refund_count,     /* 退单次数: 统计退单记录数 */
    SUM(refund_num) AS order_refund_num,     /* 退单商品件数: 统计退回的商品数量 */
    SUM(refund_amount) AS order_refund_amount /* 退单金额: 统计退单涉及的总金额 */
FROM dwd.dwd_trade_order_refund_inc
/* 分组: 按用户和日期分组，确保每个用户每天一条汇总记录 */
GROUP BY user_id, k1;

/*
 * 数据处理说明:
 *
 * 1. 执行场景:
 *    - 首次构建数据仓库时执行
 *    - 数据修复或重建时执行
 *    - 全量加载所有历史退单数据
 *
 * 2. 数据来源:
 *    - 从明细事实表(dwd_trade_order_refund_inc)获取所有历史数据
 *    - 不做时间范围限制，处理全部可用数据
 *
 * 3. 计算方法:
 *    - 退单次数: 对退单事件记录进行计数统计
 *    - 退单件数: 对退单商品数量进行求和
 *    - 退单金额: 对退单金额进行求和
 *
 * 4. 执行建议:
 *    - 确保目标表为空，避免数据重复
 *    - 根据数据量大小，可能需要调整执行资源配置
 *    - 执行完成后，建议验证数据一致性
 *    - 首次加载后，日常维护应使用每日增量加载脚本
 */
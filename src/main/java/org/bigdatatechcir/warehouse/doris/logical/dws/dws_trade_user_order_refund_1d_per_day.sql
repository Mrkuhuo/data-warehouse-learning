/*
 * 脚本名称: dws_trade_user_order_refund_1d_per_day.sql
 * 目标表: dws.dws_trade_user_order_refund_1d
 * 数据粒度: 用户 + 日期
 * 刷新策略: 增量刷新，每日新增数据
 * 调度周期: 每日调度一次
 * 运行参数:
 *   - pdate: 数据日期，默认为当天
 * 依赖表:
 *   - dwd.dwd_trade_order_refund_inc: 交易域退单事实表，增量表
 */

-- 交易域用户粒度退单最近1日汇总表
-- 计算逻辑: 
-- 1. 按用户和日期分组聚合当日退单数据
-- 2. 计算退单次数、件数和金额等指标
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
    k1,                                 /* 数据日期: 当前计算日期 */
    COUNT(*) AS order_refund_count,     /* 退单次数: 统计退单记录数 */
    SUM(refund_num) AS order_refund_num,     /* 退单商品件数: 统计退回的商品数量 */
    SUM(refund_amount) AS order_refund_amount /* 退单金额: 统计退单涉及的总金额 */
FROM dwd.dwd_trade_order_refund_inc
/* 时间筛选: 只处理当日数据 */
WHERE k1 = date('${pdate}')
/* 分组: 按用户和日期分组，确保每个用户每天一条汇总记录 */
GROUP BY user_id, k1;

/*
 * 数据处理说明:
 *
 * 1. 数据来源:
 *    - 从明细事实表(dwd_trade_order_refund_inc)获取基础数据
 *    - 该表记录了每个退单事件的详细信息，包括退单商品数量和金额
 *
 * 2. 计算方法:
 *    - 退单次数: 对退单事件记录进行计数统计
 *    - 退单件数: 对退单商品数量进行求和
 *    - 退单金额: 对退单金额进行求和
 *
 * 3. 数据质量控制:
 *    - 采用先删除后插入的方式，确保当日数据不会重复
 *    - WHERE条件严格限定数据时间范围为当天
 *    - GROUP BY确保数据聚合的准确性和一致性
 *
 * 4. 应用价值:
 *    - 为用户退单行为分析提供基础数据
 *    - 支持商品质量和用户满意度监控
 *    - 作为N日汇总表的数据来源，支持中长期分析
 */
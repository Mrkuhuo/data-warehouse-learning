/*
 * 脚本名称: dws_trade_user_order_td_per_day.sql
 * 目标表: dws.dws_trade_user_order_td
 * 数据粒度: 用户 + 日期
 * 刷新策略: 增量更新，每日累加
 * 调度周期: 每日调度一次
 * 运行参数:
 *   - pdate: 数据日期，默认为当天
 * 依赖表:
 *   - dws.dws_trade_user_order_td: 用户粒度订单历史至今汇总表(前一天)
 *   - dws.dws_trade_user_order_1d: 用户粒度订单1日汇总表(当天)
 */

-- 交易域用户粒度订单历史至今汇总表
-- 计算逻辑: 
-- 1. 关联前一天的历史汇总数据和当天的新增数据
-- 2. 对新用户设置首次下单日期为当天
-- 3. 对有订单的用户更新末次下单日期为当天
-- 4. 累加计算各项订单指标
INSERT INTO dws.dws_trade_user_order_td
(
    user_id,                    /* 用户ID */
    k1,                         /* 数据日期 */
    order_date_first,           /* 首次下单日期 */
    order_date_last,            /* 末次下单日期 */
    order_count_td,             /* 累计下单次数 */
    order_num_td,               /* 累计购买商品件数 */
    original_amount_td,         /* 累计下单原始金额 */
    activity_reduce_amount_td,  /* 累计活动优惠金额 */
    coupon_reduce_amount_td,    /* 累计优惠券优惠金额 */
    total_amount_td             /* 累计下单最终金额 */
)
    SELECT
        NVL(old.user_id, new.user_id),      /* 用户ID: 历史或新增用户ID */
        date('${pdate}') AS k1,              /* 数据日期: 当前处理日期 */

        /* 首次下单日期: 新用户设为当天，老用户保持不变 */
        IF(new.user_id IS NOT NULL AND old.user_id IS NULL,
           date('${pdate}'),
           old.order_date_first) AS order_date_first,

        /* 末次下单日期: 有新订单则更新为当天 */
        IF(new.user_id IS NOT NULL,
           date('${pdate}'),
           old.order_date_last) AS order_date_last,

        /* 累计各项订单指标: 前一天历史累计值 + 当天新增值 */
        NVL(old.order_count_td, 0) + NVL(new.order_count_1d, 0) AS order_count_td,
        NVL(old.order_num_td, 0) + NVL(new.order_num_1d, 0) AS order_num_td,
        NVL(old.original_amount_td, 0) + NVL(new.order_original_amount_1d, 0) AS original_amount_td,
        NVL(old.activity_reduce_amount_td, 0) + NVL(new.activity_reduce_amount_1d, 0) AS activity_reduce_amount_td,
        NVL(old.coupon_reduce_amount_td, 0) + NVL(new.coupon_reduce_amount_1d, 0) AS coupon_reduce_amount_td,
        NVL(old.total_amount_td, 0) + NVL(new.order_total_amount_1d, 0) AS total_amount_td
    FROM
        (
            /* 前一天的历史累计数据 */
            SELECT
                user_id,
                order_date_first,
                order_date_last,
                order_count_td,
                order_num_td,
                original_amount_td,
                activity_reduce_amount_td,
                coupon_reduce_amount_td,
                total_amount_td
            FROM dws.dws_trade_user_order_td
            WHERE k1 = date_add(date('${pdate}'), -1)
        ) AS old
        FULL OUTER JOIN
    (
        /* 当天的新增订单数据 */
        SELECT
            user_id,
            order_count_1d,
            order_num_1d,
            order_original_amount_1d,
            activity_reduce_amount_1d,
            coupon_reduce_amount_1d,
            order_total_amount_1d
        FROM dws.dws_trade_user_order_1d
        WHERE k1 = date('${pdate}')
    ) AS new
ON old.user_id = new.user_id;

/*
 * 数据处理说明:
 *
 * 1. 更新策略:
 *    - 采用增量累加方式，将当天订单数据累加到历史汇总中
 *    - 通过FULL OUTER JOIN确保能处理老用户和新用户的数据
 *    - 新用户的首次下单日期设为当天，末次下单日期也为当天
 *    - 老用户保持首次下单日期不变，但如有新订单更新末次下单日期
 *
 * 2. 数据来源:
 *    - 历史累计数据: 从前一天的TD表获取
 *    - 当日新增数据: 从当天的1D表获取
 *
 * 3. 特殊处理:
 *    - NVL函数: 处理NULL值，确保计算准确性
 *    - IF条件: 根据用户类型(新用户/老用户)设置不同的首末次下单日期
 *    - 日期参数: 使用参数化的日期处理，确保脚本通用性
 *
 * 4. 性能优化:
 *    - 仅关联前一天的历史数据，而非全量历史数据
 *    - 仅处理当天的增量订单数据
 *    - 使用DELETE+INSERT模式而非UPDATE，提高写入效率
 *
 * 5. 应用价值:
 *    - 提供用户生命周期内的完整消费视图
 *    - 支持用户价值评估和分层分析
 *    - 为用户留存和活跃度分析提供基础数据
 */
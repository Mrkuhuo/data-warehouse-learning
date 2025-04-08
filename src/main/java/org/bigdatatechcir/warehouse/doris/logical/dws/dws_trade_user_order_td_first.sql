/*
 * 脚本名称: dws_trade_user_order_td_first.sql
 * 目标表: dws.dws_trade_user_order_td
 * 数据粒度: 用户 + 日期
 * 刷新策略: 全量加载历史数据
 * 调度周期: 一次性执行
 * 依赖表:
 *   - dws.dws_trade_user_order_1d: 用户粒度订单1日汇总表
 */

-- 交易域用户粒度订单历史至今汇总表
-- 计算逻辑: 
-- 1. 一次性汇总所有历史订单数据
-- 2. 计算用户首次和末次下单时间
-- 3. 累计用户的所有历史订单指标
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
    user_id,                            /* 用户ID: 标识下单用户 */
    k1,                                 /* 数据日期: 分区字段 */
    MIN(k1) AS login_date_first,        /* 首次下单日期: 用户最早的下单日期 */
    MAX(k1) AS login_date_last,         /* 末次下单日期: 用户最近的下单日期 */
    SUM(order_count_1d) AS order_count, /* 累计下单次数: 历史总订单数 */
    SUM(order_num_1d) AS order_num,     /* 累计购买商品件数: 历史购买总数量 */
    SUM(order_original_amount_1d) AS original_amount,       /* 累计原始金额: 历史原始总金额 */
    SUM(activity_reduce_amount_1d) AS activity_reduce_amount, /* 累计活动优惠: 历史活动优惠总额 */
    SUM(coupon_reduce_amount_1d) AS coupon_reduce_amount,   /* 累计优惠券优惠: 历史优惠券总额 */
    SUM(order_total_amount_1d) AS total_amount              /* 累计最终金额: 历史实付总金额 */
FROM dws.dws_trade_user_order_1d
/* 分组: 按用户和日期分组，每个用户每天一条汇总记录 */
GROUP BY user_id, k1;

/*
 * 数据处理说明:
 *
 * 1. 执行场景:
 *    - 首次构建数据仓库时执行
 *    - 数据修复或重建时执行
 *    - 全量计算所有用户的历史消费数据
 *
 * 2. 数据来源:
 *    - 从1日汇总表(dws_trade_user_order_1d)获取所有历史数据
 *    - 该表已按用户和日期维度聚合了订单基础指标
 *
 * 3. 计算逻辑:
 *    - 首次下单: 通过MIN函数获取最早的订单日期
 *    - 末次下单: 通过MAX函数获取最近的订单日期
 *    - 累计指标: 通过SUM函数汇总所有历史指标
 *
 * 4. 执行建议:
 *    - 确保目标表为空，避免数据重复
 *    - 数据量较大时，注意调整计算资源配置
 *    - 执行完成后验证用户消费总额的准确性
 *    - 首次加载后，日常维护应使用每日增量加载脚本
 */
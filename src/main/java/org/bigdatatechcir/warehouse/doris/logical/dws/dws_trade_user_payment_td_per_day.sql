/*
 * 脚本名称: dws_trade_user_payment_td_per_day.sql
 * 目标表: dws.dws_trade_user_payment_td
 * 数据粒度: 用户 + 日期
 * 刷新策略: 增量刷新，每日累积更新
 * 调度周期: 每日调度一次
 * 运行参数:
 *   - pdate: 数据日期，默认为当天
 * 依赖表:
 *   - dws.dws_trade_user_payment_td: 历史至今汇总表（前一天）
 *   - dws.dws_trade_user_payment_1d: 用户粒度支付1日汇总表
 */

-- 交易域用户粒度支付历史至今汇总表
-- 计算逻辑: 
-- 1. 获取前一天的历史累计数据
-- 2. 获取当天的新增支付数据
-- 3. 通过全外连接处理新老用户，更新首末次支付日期和累计指标
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
    /* 使用COALESCE(NVL)确保新老用户都能被处理 */
    NVL(old.user_id, new.user_id) AS user_id,                /* 用户ID: 新老用户ID */
    date('${pdate}') AS k1,                                  /* 数据日期: 当前计算日期 */
    
    /* 首次支付日期: 对新用户使用当前日期，老用户保持原值 */
    IF(old.user_id IS NULL AND new.user_id IS NOT NULL, 
       date('${pdate}'), 
       old.payment_date_first) AS payment_date_first,
    
    /* 末次支付日期: 对有新支付的用户更新为当前日期 */
    IF(new.user_id IS NOT NULL, 
       date('${pdate}'), 
       old.payment_date_last) AS payment_date_last,
    
    /* 累计支付次数: 前一天累计值 + 当天新增值 */
    NVL(old.payment_count_td, 0) + NVL(new.payment_count_1d, 0) AS payment_count_td,
    
    /* 累计支付商品件数: 前一天累计值 + 当天新增值 */
    NVL(old.payment_num_td, 0) + NVL(new.payment_num_1d, 0) AS payment_num_td,
    
    /* 累计支付金额: 前一天累计值 + 当天新增值 */
    NVL(old.payment_amount_td, 0) + NVL(new.payment_amount_1d, 0) AS payment_amount_td
FROM
    (
        /* 子查询: 获取前一天的历史累计数据 */
        SELECT
            user_id,
            k1,
            payment_date_first,
            payment_date_last,
            payment_count_td,
            payment_num_td,
            payment_amount_td
        FROM dws.dws_trade_user_payment_td
        WHERE k1 = date_add(date('${pdate}'), -1)  /* 筛选前一天的数据 */
    ) old
    FULL OUTER JOIN  /* 全外连接: 确保新老用户都能被处理 */
    (
        /* 子查询: 获取当天的新增支付数据 */
        SELECT
            user_id,
            payment_count_1d,
            payment_num_1d,
            payment_amount_1d
        FROM dws.dws_trade_user_payment_1d
        WHERE k1 = date('${pdate}')  /* 筛选当天的数据 */
    ) new
    ON old.user_id = new.user_id;  /* 连接条件: 用户ID */

/*
 * 数据处理说明:
 *
 * 1. 增量处理模式:
 *    - 获取前一天的历史累计数据作为基础
 *    - 获取当天的新增支付数据
 *    - 通过全外连接整合新老数据，实现增量累积更新
 *
 * 2. 用户类型处理:
 *    - 老用户(存在于前一天): 累加其当天新增支付数据
 *    - 新用户(仅存在于当天): 设置首末次支付日期为当天，初始化累计指标
 *    - 无当天支付(仅存在于前一天): 保持历史数据不变，末次支付日期不更新
 *
 * 3. 指标计算逻辑:
 *    - 首次支付日期: 新用户为当天，老用户保持不变
 *    - 末次支付日期: 有当天支付的用户更新为当天日期
 *    - 累计指标: 历史累计值 + 当天新增值，确保正确累加
 *
 * 4. 性能优化:
 *    - 只处理前一天的历史数据和当天的新增数据，减少数据处理量
 *    - 使用NVL函数处理空值，确保计算准确性
 *    - 根据分区字段筛选数据，充分利用分区优势
 *
 * 5. 应用建议:
 *    - 配合首次加载脚本(dws_trade_user_payment_td_first.sql)使用
 *    - 每日执行一次，保持历史至今指标的持续累积
 *    - 可用于分析用户支付生命周期和长期价值评估
 *    - 与其他历史至今表结合，构建完整的用户价值评估体系
 */
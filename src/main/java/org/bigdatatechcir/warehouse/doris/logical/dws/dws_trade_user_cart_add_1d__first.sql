/*
 * 文件名: dws_trade_user_cart_add_1d__first.sql
 * 功能描述: 交易域用户粒度加购最近1日汇总表加载脚本
 * 数据粒度: 用户ID + 日期
 * 刷新策略: 每日全量刷新
 * 调度周期: 每日执行
 * 调度依赖:
 *   - dwd层加购事实表数据已准备完毕
 * 数据来源:
 *   - dwd.dwd_trade_cart_add_full: 加购事实表
 * 目标表: dws.dws_trade_user_cart_add_1d
 */

-- 交易域用户粒度加购最近1日汇总表
INSERT INTO dws.dws_trade_user_cart_add_1d(
    user_id,            -- 用户ID
    k1,                -- 数据日期
    cart_add_count_1d, -- 最近1日加购次数
    cart_add_num_1d    -- 最近1日加购商品件数
)
SELECT
    user_id,            -- 用户ID
    k1,                -- 数据日期
    COUNT(*),          -- 统计用户最近1日的加购次数
    SUM(sku_num)       -- 统计用户最近1日的加购商品总件数
FROM 
    dwd.dwd_trade_cart_add_inc  -- 从加购事实表获取数据
GROUP BY 
    user_id, k1;  -- 按用户ID和日期分组统计

/*
 * 脚本设计说明:
 * 1. 数据处理逻辑:
 *    - 从加购事实表获取用户加购数据
 *    - 按用户ID和日期分组统计加购次数和商品件数
 *    - 使用COUNT(*)统计加购行为次数
 *    - 使用SUM统计加购商品总件数
 *
 * 2. 指标计算方法:
 *    - 加购次数：使用COUNT(*)统计加购行为记录数
 *    - 加购商品件数：使用SUM统计所有加购商品数量
 *
 * 3. 数据质量保障:
 *    - 使用全量数据确保统计的完整性
 *    - 通过GROUP BY确保数据不重复统计
 *
 * 4. 优化考虑:
 *    - 全量处理：确保历史数据准确性
 *    - 分组统计：使用GROUP BY提高查询效率
 *    - 简单聚合：使用基础聚合函数提高性能
 */
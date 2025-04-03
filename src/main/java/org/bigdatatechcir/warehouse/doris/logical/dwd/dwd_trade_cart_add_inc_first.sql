/*
 * 文件名: dwd_trade_cart_add_inc_first.sql
 * 功能描述: 交易域加购事务事实表(首次加载) - 记录用户商品加购行为
 * 数据粒度: 加购事件
 * 刷新策略: 首次增量加载
 * 调度周期: 一次性执行
 * 依赖表: 
 *   - ods.ods_cart_info_full: 购物车信息全量数据
 *   - ods.ods_base_dic_full: 数据字典表，用于转换来源类型编码
 * 目标表: dwd.dwd_trade_cart_add_inc
 * 主要功能: 
 *   1. 加载历史全量加购数据
 *   2. 转换来源类型编码为可读名称
 *   3. 生成日期ID字段，便于按日期分析
 *   4. 为后续每日增量加载奠定数据基础
 */

-- 交易域加购事务事实表(首次加载)
INSERT INTO dwd.dwd_trade_cart_add_inc(
    id,               -- 加购记录ID
    k1,               -- 数据日期分区
    user_id,          -- 用户ID
    sku_id,           -- 商品SKU ID
    date_id,          -- 日期ID(yyyy-MM-dd格式)
    create_time,      -- 加购时间
    source_id,        -- 来源ID
    source_type,      -- 来源类型编码
    source_type_name, -- 来源类型名称
    sku_num           -- 加购商品数量
)
select
    id,                                              -- 加购记录ID
    current_date() as k1,                            -- 使用当前日期作为分区
    user_id,                                         -- 用户ID
    sku_id,                                          -- 商品SKU ID
    date_format(create_time,'yyyy-MM-dd') date_id,   -- 将时间戳转换为日期ID
    create_time,                                     -- 加购时间
    source_id,                                       -- 来源ID
    source_type,                                     -- 来源类型编码
    dic.dic_name,                                    -- 来源类型名称(关联自字典表)
    sku_num                                          -- 加购商品数量
from
    (
        -- 购物车信息子查询：获取加购数据
        select
            id,                                      -- 加购记录ID
            user_id,                                 -- 用户ID
            sku_id,                                  -- 商品SKU ID
            create_time,                             -- 加购时间
            source_id,                               -- 来源ID
            source_type,                             -- 来源类型编码
            sku_num                                  -- 加购商品数量
        from ods.ods_cart_info_full
        -- 注：首次加载不使用k1过滤，加载所有历史数据
    )ci
    left join
    (
        -- 数据字典子查询：获取来源类型名称
        select
            dic_code,                                -- 字典编码
            dic_name                                 -- 字典名称
        from ods.ods_base_dic_full
        where parent_code='24'                       -- 筛选订单来源类型的字典项
    )dic
    on ci.source_type=dic.dic_code;                  -- 通过来源类型编码关联字典表

/*
 * 设计说明:
 * 1. 首次加载特点:
 *    - 不使用时间过滤条件，加载购物车表中所有历史加购记录
 *    - 使用current_date()作为k1值，表示数据加载的日期，而非数据本身的日期
 *    - 与每日增量加载脚本配合使用，形成完整的数据加载策略
 *    
 * 2. 字典解析设计:
 *    - 使用左连接(left join)关联数据字典表
 *    - 将原始的来源类型编码转换为可读的来源类型名称
 *    - 同时保留原始编码，便于程序处理和字典项变更
 *
 * 3. 时间处理:
 *    - 保留原始create_time字段，便于精确时间分析
 *    - 同时生成date_id字段(yyyy-MM-dd格式)，便于按天汇总分析
 *
 * 4. 数据应用场景:
 *    - 用户购物意向分析：了解用户对哪些商品有购买意向
 *    - 加购转化分析：结合订单数据分析加购到下单的转化率
 *    - 商品受欢迎度：评估各商品被加入购物车的频率
 *    - 渠道效果分析：分析不同来源渠道的用户加购行为差异
 */
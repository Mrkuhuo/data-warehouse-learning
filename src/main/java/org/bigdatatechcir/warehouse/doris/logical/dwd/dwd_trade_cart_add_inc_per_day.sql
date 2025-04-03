/*
 * 文件名: dwd_trade_cart_add_inc_per_day.sql
 * 功能描述: 交易域加购事务事实表(每日增量) - 记录用户每日新增加购行为
 * 数据粒度: 加购事件
 * 刷新策略: 每日增量加载
 * 调度周期: 每日
 * 依赖表: 
 *   - ods.ods_cart_info_full: 购物车信息全量数据
 *   - ods.ods_base_dic_full: 数据字典表，用于转换来源类型编码
 * 目标表: dwd.dwd_trade_cart_add_inc
 * 主要功能: 
 *   1. 提取每日新增加购数据
 *   2. 转换来源类型编码为可读名称
 *   3. 生成日期ID字段，便于按日期分析
 *   4. 为用户购物意向分析提供数据支持
 */

-- 交易域加购事务事实表(每日增量)
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
        -- 购物车信息子查询：获取当日加购数据
        select
            id,                                      -- 加购记录ID
            user_id,                                 -- 用户ID
            sku_id,                                  -- 商品SKU ID
            create_time,                             -- 加购时间
            source_id,                               -- 来源ID
            source_type,                             -- 来源类型编码
            sku_num                                  -- 加购商品数量
        from ods.ods_cart_info_full
        where k1=date('${pdate}')                    -- 按分区日期过滤，只处理当天数据
    )ci
    left join
    (
        -- 数据字典子查询：获取来源类型名称
        select
            dic_code,                                -- 字典编码
            dic_name                                 -- 字典名称
        from ods.ods_base_dic_full
        where k1=date('${pdate}')                    -- 按分区日期过滤，使用当天最新的字典数据
          and parent_code='24'                       -- 筛选订单来源类型的字典项
    )dic
    on ci.source_type=dic.dic_code;                  -- 通过来源类型编码关联字典表

/*
 * 设计说明:
 * 1. 每日增量加载策略:
 *    - 使用k1=date('${pdate}')条件过滤购物车表和字典表
 *    - 只处理特定日期的新增加购记录
 *    - 与首次加载脚本(dwd_trade_cart_add_inc_first.sql)配合使用
 *    
 * 2. 分区字段处理:
 *    - 使用current_date()作为k1值，表示数据加载的日期
 *    - 这与过滤条件中的date('${pdate}')在语义上保持一致
 *
 * 3. 字典表处理:
 *    - 使用与主表相同的日期条件过滤字典表
 *    - 确保使用当天最新的字典数据进行编码转换
 *    - 使用左连接(left join)确保即使没有匹配的字典项也不会丢失加购记录
 *
 * 4. 数据应用场景:
 *    - 每日加购行为监控：实时跟踪用户加购趋势
 *    - 商品热度分析：识别当日热门加购商品
 *    - 用户兴趣追踪：分析用户近期购物意向变化
 *    - 营销活动评估：结合促销活动分析对加购行为的影响
 */
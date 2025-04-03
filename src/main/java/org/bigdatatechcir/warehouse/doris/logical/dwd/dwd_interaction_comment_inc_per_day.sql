/*
 * 文件名: dwd_interaction_comment_inc_per_day.sql
 * 功能描述: 互动域评价事务事实表(每日增量) - 记录用户每日新增的商品评价
 * 数据粒度: 评价事件
 * 刷新策略: 每日增量
 * 调度周期: 每日
 * 依赖表: 
 *   - ods.ods_comment_info_inc: 评价信息增量数据
 *   - ods.ods_base_dic_full: 数据字典表，用于转换评价代码为评价名称
 * 目标表: dwd.dwd_interaction_comment_inc
 * 主要功能: 
 *   1. 提取每日新增的评价数据
 *   2. 将评价代码转换为可读的评价名称
 *   3. 生成日期ID字段，便于按日期分析
 *   4. 为用户反馈和商品质量分析提供数据支持
 */

-- 互动域评价事务事实表(每日增量)
INSERT INTO dwd.dwd_interaction_comment_inc(
    id,           -- 评价记录唯一标识
    k1,           -- 数据日期，用于分区
    user_id,      -- 用户ID
    sku_id,       -- 商品SKU ID
    order_id,     -- 订单ID
    date_id,      -- 日期ID，格式yyyy-MM-dd
    create_time,  -- 评价时间
    appraise_code,-- 评价代码
    appraise_name -- 评价名称(好评/中评/差评)
)
select 
    ci.id,                                      -- 评价记录ID
    ci.k1,                                      -- 分区日期
    ci.user_id,                                 -- 用户ID，关联用户维度
    ci.sku_id,                                  -- 商品SKU ID，关联商品维度
    ci.order_id,                                -- 订单ID，关联订单维度
    date_format(ci.create_time,'yyyy-MM-dd') as date_id, -- 将时间戳转换为日期ID格式
    ci.create_time,                             -- 评价创建时间
    ci.appraise as appraise_code,               -- 评价代码(1/2/3)
    dic.dic_name as appraise_name               -- 评价名称(好评/中评/差评)
from ods.ods_comment_info_inc ci
left join ods.ods_base_dic_full dic             -- 关联字典表转换评价代码
on ci.appraise = dic.dic_code
where ci.k1=date('${pdate}');                   -- 按分区日期过滤，只处理当天数据

/*
 * 设计说明:
 * 1. 增量加载策略:
 *    - 此脚本用于每日例行数据加载，只处理新增评价
 *    - 使用k1=date('${pdate}')过滤条件确保只加载特定日期的数据
 *    - 与首次加载脚本(dwd_interaction_comment_inc_first.sql)配合使用
 *    
 * 2. 字典解析设计:
 *    - 使用左连接(left join)关联数据字典表
 *    - 将原始的评价代码(1/2/3)转换为可读的评价名称(好评/中评/差评)
 *    - 使用左连接确保即使字典中没有匹配项，评价记录也不会丢失
 *
 * 3. 增量数据源选择:
 *    - 使用增量表(ods_comment_info_inc)作为数据源
 *    - 相比全量表，增量表体积更小，处理更高效
 *    - 确保数据不会重复加载到目标表中
 *
 * 4. 数据应用场景:
 *    - 每日评价监控：实时跟踪商品评价趋势变化
 *    - 评价异常预警：监控突发的差评情况
 *    - 商品满意度追踪：分析评价随时间的变化趋势
 *    - 用户行为分析：结合购买时间分析用户从购买到评价的时间间隔
 */
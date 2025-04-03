/*
 * 文件名: dwd_interaction_comment_inc_first.sql
 * 功能描述: 互动域评价事务事实表(首次加载) - 记录用户对商品的评价信息
 * 数据粒度: 评价事件
 * 刷新策略: 首次增量加载
 * 调度周期: 一次性执行
 * 依赖表: 
 *   - ods.ods_comment_info_full: 评价信息全量数据
 *   - ods.ods_base_dic_full: 数据字典表，用于转换评价代码为评价名称
 * 目标表: dwd.dwd_interaction_comment_inc
 * 主要功能: 
 *   1. 首次加载全量历史评价数据
 *   2. 将评价代码转换为可读的评价名称
 *   3. 生成日期ID字段，便于按日期分析
 *   4. 为后续每日增量加载奠定数据基础
 */

-- 互动域评价事务事实表(首次加载)
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
from ods.ods_comment_info_full ci
left join ods.ods_base_dic_full dic             -- 关联字典表转换评价代码
on ci.appraise = dic.dic_code
-- 注：首次加载不使用k1过滤，加载全量历史数据

/*
 * 设计说明:
 * 1. 首次加载特点:
 *    - 此脚本用于DWD层初始化时一次性执行
 *    - 不使用时间过滤条件，加载所有历史评价记录
 *    - 后续日常加载应使用dwd_interaction_comment_inc_per_day.sql
 *    
 * 2. 字典解析设计:
 *    - 使用左连接(left join)关联数据字典表
 *    - 将原始的评价代码(1/2/3)转换为可读的评价名称(好评/中评/差评)
 *    - 同时保留原始代码，便于程序处理和未来字典变更
 *
 * 3. 多维度关联:
 *    - 保留用户、商品和订单ID，建立多维关联
 *    - 可用于分析特定用户、特定商品或特定订单的评价情况
 *
 * 4. 数据应用场景:
 *    - 商品质量评估：分析商品的好评率和差评率
 *    - 用户满意度分析：评估用户购物体验和满意程度
 *    - 订单履约质量：结合订单数据分析物流、客服等服务质量
 *    - 商品改进方向：根据评价内容识别产品需要改进的方面
 */
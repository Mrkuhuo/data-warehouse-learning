/*
 * 文件名: dwd_interaction_favor_add_inc_per_day.sql
 * 功能描述: 互动域收藏商品事务事实表(每日增量) - 记录用户收藏商品行为
 * 数据粒度: 收藏商品事件
 * 刷新策略: 每日增量
 * 调度周期: 每日
 * 依赖表: 
 *   - ods.ods_favor_info_inc: 商品收藏增量数据
 * 目标表: dwd.dwd_interaction_favor_add_inc
 * 主要功能: 
 *   1. 提取用户收藏商品的行为数据
 *   2. 生成日期ID字段，便于按日期分析
 *   3. 为用户兴趣分析和商品受欢迎度分析提供数据支持
 */

-- 互动域收藏商品事务事实表
INSERT INTO dwd.dwd_interaction_favor_add_inc(
    id,          -- 收藏记录唯一标识
    k1,          -- 数据日期，用于分区
    user_id,     -- 用户ID
    sku_id,      -- 商品SKU ID
    date_id,     -- 日期ID，格式yyyy-MM-dd
    create_time  -- 收藏时间
)
select
    id,                                        -- 收藏记录ID
    k1,                                        -- 分区日期
    user_id,                                   -- 用户ID，关联用户维度
    sku_id,                                    -- 商品SKU ID，关联商品维度
    date_format(create_time,'yyyy-MM-dd') date_id, -- 将时间戳转换为日期ID格式
    create_time                                -- 收藏创建时间
from ods.ods_favor_info_inc
where k1=date('${pdate}')                     -- 按分区日期过滤，只处理当天数据

/*
 * 设计说明:
 * 1. 增量加载策略:
 *    - 此表采用每日增量加载模式，只处理当天新增的收藏记录
 *    - 使用k1=date('${pdate}')过滤条件确保只加载特定日期的数据
 *    
 * 2. 时间处理:
 *    - 保留原始create_time字段，保留完整时间信息
 *    - 同时生成date_id字段(格式yyyy-MM-dd)，便于按天汇总分析
 *
 * 3. 数据应用场景:
 *    - 用户兴趣分析：了解用户收藏的商品类型和特征
 *    - 商品受欢迎度评估：统计各商品被收藏次数
 *    - 商品推荐：基于收藏行为的个性化推荐
 *
 * 4. 业务价值:
 *    - 收藏行为是用户兴趣的强信号，比浏览行为更有价值
 *    - 可用于分析用户从收藏到购买的转化路径和转化率
 *    - 帮助识别高关注度但低转化率的商品，指导产品优化
 */
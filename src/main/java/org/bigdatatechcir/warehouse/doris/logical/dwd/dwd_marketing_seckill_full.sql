/*
 * 文件名: dwd_marketing_seckill_full.sql
 * 功能描述: 营销域秒杀全量表 - 整合秒杀商品信息与商品基础信息
 * 数据粒度: 秒杀商品
 * 刷新策略: 全量刷新
 * 调度周期: 每日
 * 依赖表: 
 *   - ods.ods_seckill_goods_full: 秒杀商品表
 *   - ods.ods_sku_info_full: 商品SKU信息表
 * 目标表: dwd.dwd_marketing_seckill_full
 * 主要功能: 
 *   1. 整合秒杀商品信息与商品基础信息
 *   2. 补充商品名称、默认图片等展示信息
 *   3. 标准化秒杀商品的价格、库存等关键指标
 *   4. 为秒杀活动分析和决策提供数据支持
 */

-- 营销域秒杀全量表
INSERT INTO dwd.dwd_marketing_seckill_full(
    id,                -- 记录唯一标识
    k1,                -- 数据日期分区
    seckill_id,        -- 秒杀ID
    sku_id,            -- 商品SKU ID
    sku_name,          -- 商品名称
    sku_default_img,   -- 商品默认图片
    original_price,    -- 原始价格
    seckill_price,     -- 秒杀价格
    create_time,       -- 创建时间
    check_time,        -- 审核时间
    status,            -- 秒杀状态
    start_time,        -- 开始时间
    end_time,          -- 结束时间
    total_num,         -- 秒杀总数量
    stock_count,       -- 剩余库存
    sku_desc,          -- 商品描述
    spu_id             -- 商品SPU ID
)
select
    sg.id,                          -- 使用秒杀商品ID作为记录ID
    sg.k1,                          -- 分区字段
    sg.id as seckill_id,            -- 秒杀ID (与id字段相同)
    sg.sku_id,                      -- 商品SKU ID
    si.sku_name,                    -- 商品名称(关联自商品表)
    si.sku_default_img,             -- 商品默认图片(关联自商品表)
    sg.price as original_price,     -- 商品原价
    sg.cost_price as seckill_price, -- 秒杀价格
    sg.create_time,                 -- 创建时间
    sg.check_time,                  -- 审核时间
    sg.status,                      -- 秒杀状态
    sg.start_time,                  -- 开始时间
    sg.end_time,                    -- 结束时间
    sg.num as total_num,            -- 秒杀商品总数量
    sg.stock_count,                 -- 剩余库存数量
    sg.sku_desc,                    -- 商品描述
    sg.spu_id                       -- 商品SPU ID
from
    (
        -- 秒杀商品子查询：获取秒杀基础信息
        select
            id,                     -- 秒杀ID
            k1,                     -- 分区字段
            sku_id,                 -- 商品SKU ID
            spu_id,                 -- 商品SPU ID
            price,                  -- 原始价格
            cost_price,             -- 秒杀价格(成本价)
            create_time,            -- 创建时间
            check_time,             -- 审核时间
            status,                 -- 秒杀状态
            start_time,             -- 开始时间
            end_time,               -- 结束时间
            num,                    -- 秒杀总数量
            stock_count,            -- 剩余库存
            sku_desc                -- 商品描述
        from ods.ods_seckill_goods_full
        where k1=date('${pdate}')   -- 按分区日期过滤，只处理当天数据
    ) sg
    left join
    (
        -- 商品信息子查询：获取商品展示信息
        select
            id,                     -- 商品SKU ID
            sku_name,               -- 商品名称
            sku_default_img         -- 商品默认图片
        from ods.ods_sku_info_full
        where k1=date('${pdate}')   -- 按分区日期过滤，只处理当天数据
    ) si
    on sg.sku_id = si.id;           -- 通过商品SKU ID关联商品信息

/*
 * 设计说明:
 * 1. 数据整合设计:
 *    - 以秒杀商品表为主表，关联商品SKU表获取商品展示信息
 *    - 通过SKU ID建立两个表之间的关联，确保秒杀信息与商品信息一致
 *    - 使用left join确保即使商品信息表中缺少对应记录，也能保留秒杀商品信息
 *    
 * 2. 字段命名标准化:
 *    - 对原始字段进行重命名和标准化，使字段名称更加直观
 *    - 如将price重命名为original_price，cost_price重命名为seckill_price
 *    - 将num重命名为total_num，使其含义更清晰
 *
 * 3. 数据过滤机制:
 *    - 两个子查询都按照相同的日期过滤条件处理数据
 *    - 这确保了秒杀信息和商品信息保持时间一致性
 *    - 对于跨天的秒杀活动，每天都会生成最新状态的快照
 *
 * 4. 价格与库存数据:
 *    - 保留原始价格和秒杀价格，便于计算折扣率和促销效果
 *    - 同时保留总数量和剩余库存，便于计算销售转化率
 *    - 这些指标对于评估秒杀活动效果至关重要
 *
 * 5. 数据应用场景:
 *    - 秒杀活动分析：评估不同商品在秒杀活动中的表现
 *    - 价格敏感度分析：分析不同折扣力度对销售的影响
 *    - 库存预测：根据历史秒杀数据预测未来活动的库存需求
 *    - 活动效果监控：实时监控秒杀活动的进行情况和销售转化
 */ 
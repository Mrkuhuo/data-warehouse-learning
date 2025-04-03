/*
 * 文件名: dwd_trade_order_detail_inc_per_day.sql
 * 功能描述: 交易域下单事务事实表(每日增量) - 整合订单明细与相关维度数据
 * 数据粒度: 订单明细项(一个订单中的一个商品)
 * 刷新策略: 每日增量加载
 * 调度周期: 每日
 * 依赖表: 
 *   - ods.ods_order_detail_full: 订单明细数据
 *   - ods.ods_order_info_full: 订单主表数据
 *   - ods.ods_order_detail_activity_full: 订单明细参与活动关联表
 *   - ods.ods_order_detail_coupon_full: 订单明细使用优惠券关联表
 *   - ods.ods_base_dic_full: 数据字典表(用于解析来源类型)
 * 目标表: dwd.dwd_trade_order_detail_inc
 * 主要功能: 
 *   1. 提取每日新增订单明细数据
 *   2. 关联活动和优惠券信息
 *   3. 计算拆分后的各项金额
 *   4. 转换订单来源类型编码为可读名称
 */

-- 交易域下单事务事实表(每日增量)
INSERT INTO dwd.dwd_trade_order_detail_inc(
    id,                     -- 订单明细ID
    k1,                     -- 数据日期分区
    order_id,               -- 订单ID
    user_id,                -- 用户ID
    sku_id,                 -- 商品SKU ID
    province_id,            -- 省份ID
    activity_id,            -- 活动ID
    activity_rule_id,       -- 活动规则ID
    coupon_id,              -- 优惠券ID
    date_id,                -- 日期ID(年月日)
    create_time,            -- 创建时间
    source_id,              -- 来源ID
    source_type,            -- 来源类型
    source_type_name,       -- 来源类型名称
    sku_num,                -- 商品数量
    split_original_amount,  -- 拆分原始金额(商品原价×数量)
    split_activity_amount,  -- 拆分活动优惠金额
    split_coupon_amount,    -- 拆分优惠券优惠金额
    split_total_amount      -- 拆分最终金额
)
select
    od.id,                                           -- 订单明细ID
    k1,                                              -- 分区日期
    order_id,                                        -- 订单ID
    user_id,                                         -- 用户ID(关联自订单主表)
    sku_id,                                          -- 商品SKU ID
    province_id,                                     -- 省份ID(关联自订单主表)
    activity_id,                                     -- 活动ID(关联自活动关联表)
    activity_rule_id,                                -- 活动规则ID(关联自活动关联表)
    coupon_id,                                       -- 优惠券ID(关联自优惠券关联表)
    date_format(create_time, 'yyyy-MM-dd') date_id,  -- 将时间戳转换为日期ID
    create_time,                                     -- 创建时间
    source_id,                                       -- 来源ID
    source_type,                                     -- 来源类型编码
    dic_name,                                        -- 来源类型名称(关联自字典表)
    sku_num,                                         -- 商品数量
    split_original_amount,                           -- 拆分原始金额
    split_activity_amount,                           -- 拆分活动优惠金额
    split_coupon_amount,                             -- 拆分优惠券优惠金额
    split_total_amount                               -- 拆分最终金额
from
    (
        -- 订单明细子查询：获取基础订单明细数据并计算金额
        select
            id,                                        -- 订单明细ID
            k1,                                        -- 分区日期
            order_id,                                  -- 订单ID
            sku_id,                                    -- 商品SKU ID
            create_time,                               -- 创建时间
            source_id,                                 -- 订单来源ID
            source_type,                               -- 订单来源类型
            sku_num,                                   -- 商品数量
            sku_num * order_price split_original_amount, -- 计算拆分原始金额(数量×单价)
            split_total_amount,                        -- 拆分后最终金额
            split_activity_amount,                     -- 拆分活动优惠金额
            split_coupon_amount                        -- 拆分优惠券优惠金额
        from ods.ods_order_detail_full
        where k1=date('${pdate}')                      -- 按分区日期过滤，只处理当天数据
    ) od
    left join
    (
        -- 订单主表子查询：获取用户和省份信息
        select
            id,                                        -- 订单ID
            user_id,                                   -- 用户ID
            province_id                                -- 省份ID
        from ods.ods_order_info_full
        where k1=date('${pdate}')                      -- 按分区日期过滤，只处理当天数据
    ) oi
    on od.order_id = oi.id                             -- 通过订单ID关联订单主表
    left join
    (
        -- 订单活动关联子查询：获取活动信息
        select
            order_detail_id,                           -- 订单明细ID
            activity_id,                               -- 活动ID
            activity_rule_id                           -- 活动规则ID
        from ods.ods_order_detail_activity_full
        where k1=date('${pdate}')                      -- 按分区日期过滤，只处理当天数据
    ) act
    on od.id = act.order_detail_id                     -- 通过订单明细ID关联活动表
    left join
    (
        -- 订单优惠券关联子查询：获取优惠券信息
        select
            order_detail_id,                           -- 订单明细ID
            coupon_id                                  -- 优惠券ID
        from ods.ods_order_detail_coupon_full
        where k1=date('${pdate}')                      -- 按分区日期过滤，只处理当天数据
    ) cou
    on od.id = cou.order_detail_id                     -- 通过订单明细ID关联优惠券表
    left join
    (
        -- 数据字典子查询：获取来源类型名称
        select
            dic_code,                                  -- 字典编码
            dic_name                                   -- 字典名称
        from ods.ods_base_dic_full
        where parent_code='24'                         -- 筛选订单来源类型的字典项
          and k1=date('${pdate}')                      -- 按分区日期过滤，使用当天最新的字典数据
    )dic
    on od.source_type=dic.dic_code;                    -- 通过来源类型编码关联字典表

/*
 * 设计说明:
 * 1. 每日增量加载策略:
 *    - 使用k1=date('${pdate}')过滤条件，确保只处理特定日期的新增数据
 *    - 所有关联表也使用相同的时间过滤，保持数据一致性
 *    - 与首次加载脚本(dwd_trade_order_detail_inc_first.sql)配合使用
 *    
 * 2. 多表关联设计:
 *    - 使用订单明细表(ods_order_detail_full)作为主表
 *    - 关联订单主表获取用户和地区维度信息
 *    - 关联活动和优惠券表获取促销维度信息
 *    - 关联字典表解析订单来源类型编码
 *
 * 3. 金额计算逻辑:
 *    - split_original_amount：原始金额，通过商品数量乘以单价计算
 *    - split_activity_amount：活动优惠金额，直接从订单明细表获取
 *    - split_coupon_amount：优惠券优惠金额，直接从订单明细表获取
 *    - split_total_amount：最终金额，直接从订单明细表获取
 *    - 这些拆分金额反映了订单明细在整个订单中的金额占比
 *
 * 4. 数据质量保障:
 *    - 使用left join确保不会因为维度关联失败而丢失订单明细记录
 *    - 各维度表和字典表使用当日数据，确保数据的时间一致性
 *
 * 5. 数据应用场景:
 *    - 订单分析：分析订单来源渠道、地域分布、商品构成等
 *    - 促销效果评估：分析活动和优惠券对销售的影响
 *    - 用户购买行为：结合用户维度分析购买偏好和模式
 *    - 商品销售情况：评估各商品的销售表现和受欢迎程度
 */
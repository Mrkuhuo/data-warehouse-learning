/*
 * 文件名: dwd_trade_order_detail_inc_first.sql
 * 功能描述: 交易域下单事务事实表(首次加载) - 整合订单明细与相关维度数据
 * 数据粒度: 订单明细项(一个订单中的一个商品)
 * 刷新策略: 首次增量加载(历史数据首次导入)
 * 调度周期: 一次性执行
 * 依赖表: 
 *   - ods.ods_order_detail_full: 订单明细数据
 *   - ods.ods_order_info_full: 订单主表数据
 *   - ods.ods_order_detail_activity_full: 订单明细参与活动关联表
 *   - ods.ods_order_detail_coupon_full: 订单明细使用优惠券关联表
 *   - ods.ods_base_dic_full: 数据字典表(用于解析来源类型)
 * 目标表: dwd.dwd_trade_order_detail_inc
 * 主要功能: 
 *   1. 整合订单明细与订单主表数据
 *   2. 关联活动和优惠券信息
 *   3. 计算拆分后的各项金额
 *   4. 转换订单来源类型编码为可读名称
 */

-- 交易域下单事务事实表(首次加载)
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
        -- 注：首次加载不使用k1过滤，加载所有历史数据
    ) od
    left join
    (
        -- 订单主表子查询：获取用户和省份信息
        select
            id,           -- 订单ID
            user_id,      -- 用户ID
            province_id   -- 省份ID
        from ods.ods_order_info_full
        -- 注：关联订单主表获取用户ID和省份ID
    ) oi
    on od.order_id = oi.id
    left join
    (
        -- 订单活动关联子查询：获取活动信息
        select
            order_detail_id,  -- 订单明细ID
            activity_id,      -- 活动ID
            activity_rule_id  -- 活动规则ID
        from ods.ods_order_detail_activity_full
        -- 注：关联活动表获取订单明细参与的活动信息
    ) act
    on od.id = act.order_detail_id
    left join
    (
        -- 订单优惠券关联子查询：获取优惠券信息
        select
            order_detail_id,  -- 订单明细ID
            coupon_id         -- 优惠券ID
        from ods.ods_order_detail_coupon_full
        -- 注：关联优惠券表获取订单明细使用的优惠券信息
    ) cou
    on od.id = cou.order_detail_id
    left join
    (
        -- 数据字典子查询：获取来源类型名称
        select
            dic_code,     -- 字典编码
            dic_name      -- 字典名称
        from ods.ods_base_dic_full
        where parent_code='24'  -- 筛选订单来源类型的字典项
        -- 注：关联字典表将来源类型编码转换为可读名称
    ) dic
    on od.source_type=dic.dic_code;

/*
 * 设计说明:
 * 1. 首次增量加载特点:
 *    - 不使用时间过滤条件，加载所有历史数据
 *    - 与常规增量加载的区别在于数据范围，加载机制相同
 *    - 一般在数仓初始化阶段执行一次，后续使用常规增量加载
 *    
 * 2. 多表关联设计:
 *    - 使用订单明细表(ods_order_detail_full)作为主表
 *    - 关联订单主表(ods_order_info_full)获取用户和地区信息
 *    - 关联活动关联表和优惠券关联表获取促销信息
 *    - 关联字典表解析订单来源类型编码
 *
 * 3. 金额计算逻辑:
 *    - split_original_amount：原始金额，通过商品数量乘以单价计算
 *    - split_activity_amount：活动优惠金额，从订单明细表直接获取
 *    - split_coupon_amount：优惠券优惠金额，从订单明细表直接获取
 *    - split_total_amount：最终金额，从订单明细表直接获取
 *    - 分拆金额反映了一个订单明细在订单总价中的占比
 *
 * 4. 数据质量策略:
 *    - 使用left join确保不会因为维度关联失败而丢失订单明细记录
 *    - 对于缺失的维度信息(如活动、优惠券等)，相关字段将为NULL
 *
 * 5. 字典解析:
 *    - 使用字典表(parent_code='24')将来源类型编码转换为可读名称
 *    - 提高数据可理解性，便于分析和展示
 *
 * 6. 时间处理:
 *    - 保留原始create_time字段，便于精确时间分析
 *    - 同时生成date_id字段(yyyy-MM-dd格式)，便于按天汇总分析
 */
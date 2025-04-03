/*
 * 文件名: dwd_trade_pay_detail_suc_inc_per_day.sql
 * 功能描述: 交易域支付成功事务事实表(每日增量) - 记录订单支付成功的明细信息
 * 数据粒度: 订单明细支付事件
 * 刷新策略: 每日增量加载
 * 调度周期: 每日
 * 依赖表: 
 *   - ods.ods_order_detail_full: 订单明细表
 *   - ods.ods_payment_info_full: 支付信息表
 *   - ods.ods_order_info_full: 订单主表
 *   - ods.ods_order_detail_activity_full: 订单明细活动关联表
 *   - ods.ods_order_detail_coupon_full: 订单明细优惠券关联表
 *   - ods.ods_base_dic_full: 数据字典表
 * 目标表: dwd.dwd_trade_pay_detail_suc_inc
 * 主要功能: 
 *   1. 提取每日新增的支付成功订单明细数据
 *   2. 整合订单明细、支付、活动、优惠券等维度信息
 *   3. 计算每个支付明细的原始金额、活动优惠、优惠券减免等
 *   4. 为支付分析提供明细数据
 */

-- 交易域支付成功事务事实表
INSERT INTO dwd.dwd_trade_pay_detail_suc_inc(
    id,                     -- 订单明细ID
    k1,                     -- 数据日期分区
    order_id,               -- 订单ID
    user_id,                -- 用户ID
    sku_id,                 -- 商品SKU_ID
    province_id,            -- 省份ID
    activity_id,            -- 活动ID
    activity_rule_id,       -- 活动规则ID
    coupon_id,              -- 优惠券ID
    payment_type_code,      -- 支付类型编码
    payment_type_name,      -- 支付类型名称
    date_id,                -- 日期ID，格式yyyy-MM-dd
    callback_time,          -- 支付回调时间
    source_id,              -- 来源编号
    source_type,            -- 来源类型编码
    source_type_name,       -- 来源类型名称
    sku_num,                -- 商品数量
    split_original_amount,  -- 原始金额
    split_activity_amount,  -- 活动优惠金额
    split_coupon_amount,    -- 优惠券优惠金额
    split_payment_amount    -- 实际支付金额
)
select
    od.id,                                          -- 订单明细ID
    k1,                                             -- 分区字段
    od.order_id,                                    -- 订单ID
    user_id,                                        -- 用户ID
    sku_id,                                         -- 商品SKU_ID
    province_id,                                    -- 省份ID
    activity_id,                                    -- 活动ID
    activity_rule_id,                               -- 活动规则ID
    coupon_id,                                      -- 优惠券ID
    payment_type,                                   -- 支付类型编码
    pay_dic.dic_name,                               -- 支付类型名称
    date_format(callback_time,'yyyy-MM-dd') date_id, -- 将回调时间转换为日期ID格式
    callback_time,                                  -- 支付回调时间
    source_id,                                      -- 订单来源编号
    source_type,                                    -- 订单来源类型编码
    src_dic.dic_name,                               -- 订单来源类型名称
    sku_num,                                        -- 商品购买数量
    split_original_amount,                          -- 订单明细原始金额
    split_activity_amount,                          -- 订单明细活动优惠金额
    split_coupon_amount,                            -- 订单明细优惠券优惠金额
    split_total_amount                              -- 订单明细最终支付金额
from
    (
        select
            id,                                         -- 明细ID
            k1,                                         -- 分区字段
            order_id,                                   -- 订单ID
            sku_id,                                     -- 商品SKU_ID
            source_id,                                  -- 来源编号
            source_type,                                -- 来源类型编码
            sku_num,                                    -- 商品数量
            sku_num * order_price split_original_amount, -- 计算订单明细原始金额（数量*单价）
            split_total_amount,                         -- 明细最终金额
            split_activity_amount,                      -- 明细活动优惠金额
            split_coupon_amount                         -- 明细优惠券优惠金额
        from ods.ods_order_detail_full
        where k1=date('${pdate}')                       -- 只处理当日分区数据
    ) od
        join
    (
        select
            user_id,                                    -- 用户ID
            order_id,                                   -- 订单ID
            payment_type,                               -- 支付类型编码
            callback_time                               -- 支付回调时间
        from ods.ods_payment_info_full
        where payment_status='1602'                     -- 筛选支付状态为"支付成功"的记录
          and k1=date('${pdate}')                       -- 只处理当日分区数据
    ) pi
    on od.order_id=pi.order_id                          -- 关联订单明细与支付信息
        left join
    (
        select
            id,                                         -- 订单ID
            province_id                                 -- 省份ID
        from ods.ods_order_info_full
        where k1=date('${pdate}')                       -- 只处理当日分区数据
    ) oi
    on od.order_id = oi.id                              -- 关联订单明细与订单主表
        left join
    (
        select
            order_detail_id,                            -- 订单明细ID
            activity_id,                                -- 活动ID
            activity_rule_id                            -- 活动规则ID
        from ods.ods_order_detail_activity_full
        where k1=date('${pdate}')                       -- 只处理当日分区数据
    ) act
    on od.id = act.order_detail_id                      -- 关联订单明细与活动信息
        left join
    (
        select
            order_detail_id,                            -- 订单明细ID
            coupon_id                                   -- 优惠券ID
        from ods.ods_order_detail_coupon_full
        where k1=date('${pdate}')                       -- 只处理当日分区数据
    ) cou
    on od.id = cou.order_detail_id                      -- 关联订单明细与优惠券信息
        left join
    (
        select
            dic_code,                                   -- 字典编码
            dic_name                                    -- 字典名称
        from ods.ods_base_dic_full
        where parent_code='11'                          -- 筛选支付类型字典数据(父编码为11)
          and k1=date('${pdate}')                       -- 只处理当日分区数据
    ) pay_dic
    on pi.payment_type=pay_dic.dic_code                 -- 关联支付类型编码与名称
        left join
    (
        select
            dic_code,                                   -- 字典编码
            dic_name                                    -- 字典名称
        from ods.ods_base_dic_full
        where parent_code='24'                          -- 筛选来源类型字典数据(父编码为24)
          and k1=date('${pdate}')                       -- 只处理当日分区数据
    )src_dic
    on od.source_type=src_dic.dic_code;                 -- 关联来源类型编码与名称

/*
 * 设计说明:
 * 1. 数据增量加载逻辑:
 *    - 使用k1=date('${pdate}')进行分区过滤，确保只处理当天新增的支付数据
 *    - 数据字典表也使用分区过滤，保证使用最新的编码映射关系
 *    
 * 2. 多表关联设计:
 *    - 通过INNER JOIN关联订单明细表与支付表，确保只处理支付成功的订单
 *    - 使用LEFT JOIN关联订单主表、活动表和优惠券表，丰富维度信息
 *    - 通过LEFT JOIN关联数据字典表，将来源类型和支付类型代码转换为可读的名称
 *
 * 3. 支付成功的识别:
 *    - 支付表中payment_status='1602'表示支付成功
 *    - 使用callback_time作为支付成功时间，记录支付平台回调的时间点
 *
 * 4. 金额计算逻辑:
 *    - split_original_amount通过sku_num * order_price计算原始金额
 *    - split_activity_amount和split_coupon_amount记录优惠金额
 *    - split_total_amount作为split_payment_amount字段的值，表示最终实付金额
 *
 * 5. 执行策略:
 *    - 此脚本设计为每日执行一次，增量加载前一天的数据
 *    - 脚本参数${pdate}通常由调度系统设置为当前日期的前一天
 *    - 执行后将新数据追加到目标表，实现数据的持续积累
 *
 * 6. 数据应用场景:
 *    - 支付方式分析：了解不同支付方式的使用比例和趋势
 *    - 用户消费行为分析：分析用户的购买金额、频次和偏好
 *    - 营销活动效果评估：评估活动对成交的影响
 *    - 区域销售分析：基于省份维度分析销售情况
 *    - 渠道分析：评估不同来源渠道的贡献度
 *    - 日环比分析：比较不同日期的支付情况变化趋势
 */
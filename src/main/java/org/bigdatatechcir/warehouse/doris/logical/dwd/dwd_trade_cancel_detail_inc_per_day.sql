/*
 * 文件名: dwd_trade_cancel_detail_inc_per_day.sql
 * 功能描述: 交易域取消订单明细事务事实表(每日增量) - 记录用户取消订单的明细信息
 * 数据粒度: 订单明细取消事件
 * 刷新策略: 每日增量加载
 * 调度周期: 每日
 * 依赖表: 
 *   - ods.ods_order_detail_full: 订单明细表
 *   - ods.ods_order_info_full: 订单主表
 *   - ods.ods_order_detail_activity_full: 订单明细活动关联表
 *   - ods.ods_order_detail_coupon_full: 订单明细优惠券关联表
 *   - ods.ods_base_dic_full: 数据字典表
 * 目标表: dwd.dwd_trade_cancel_detail_inc
 * 主要功能: 
 *   1. 提取每日新增的订单取消明细数据
 *   2. 整合订单明细、活动、优惠券等维度信息
 *   3. 计算每个取消订单明细的原始金额、活动优惠、优惠券减免等
 *   4. 为订单取消分析提供明细数据
 */

-- 交易域取消订单事务事实表
INSERT INTO dwd.dwd_trade_cancel_detail_inc(
    id,                     -- 订单明细ID
    k1,                     -- 数据日期分区
    order_id,               -- 订单ID
    user_id,                -- 用户ID
    sku_id,                 -- 商品SKU_ID
    province_id,            -- 省份ID
    activity_id,            -- 活动ID
    activity_rule_id,       -- 活动规则ID
    coupon_id,              -- 优惠券ID
    date_id,                -- 日期ID，格式yyyy-MM-dd
    cancel_time,            -- 取消时间
    source_id,              -- 来源编号
    source_type,            -- 来源类型编码
    source_type_name,       -- 来源类型名称
    sku_num,                -- 商品数量
    split_original_amount,  -- 原始金额
    split_activity_amount,  -- 活动优惠金额
    split_coupon_amount,    -- 优惠券优惠金额
    split_total_amount      -- 最终金额
)
select
    od.id,                                          -- 订单明细ID
    k1,                                             -- 分区字段
    order_id,                                       -- 订单ID
    user_id,                                        -- 用户ID
    sku_id,                                         -- 商品SKU_ID
    province_id,                                    -- 省份ID
    activity_id,                                    -- 活动ID
    activity_rule_id,                               -- 活动规则ID
    coupon_id,                                      -- 优惠券ID
    date_format(cancel_time,'yyyy-MM-dd') date_id,  -- 将取消时间转换为日期ID格式
    cancel_time,                                    -- 订单取消时间
    source_id,                                      -- 订单来源编号
    source_type,                                    -- 订单来源类型编码
    dic_name as source_type_name,                   -- 订单来源类型名称（从字典表获取）
    sku_num,                                        -- 商品购买数量
    split_original_amount,                          -- 订单明细原始金额
    split_activity_amount,                          -- 订单明细活动优惠金额
    split_coupon_amount,                            -- 订单明细优惠券优惠金额
    split_total_amount                              -- 订单明细最终金额
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
            id,                                         -- 订单ID
            user_id,                                    -- 用户ID
            province_id,                                -- 省份ID
            operate_time cancel_time                    -- 操作时间作为取消时间
        from ods.ods_order_info_full
        where order_status='1003'                       -- 筛选订单状态为"已取消"的记录
          and k1=date('${pdate}')                       -- 只处理当日分区数据
    ) oi
    on od.order_id = oi.id                              -- 关联订单明细与订单主表
        left join
    (
        select
            order_detail_id,                            -- 订单明细ID
            activity_id,                                -- 活动ID
            activity_rule_id                            -- 活动规则ID
        from ods.ods_order_detail_activity_full         -- 订单明细活动关联表
        where k1=date('${pdate}')                       -- 只处理当日分区数据
    ) act
    on od.id = act.order_detail_id                      -- 关联订单明细与活动信息
        left join
    (
        select
            order_detail_id,                            -- 订单明细ID
            coupon_id                                   -- 优惠券ID
        from ods.ods_order_detail_coupon_full           -- 订单明细优惠券关联表
        where k1=date('${pdate}')                       -- 只处理当日分区数据
    ) cou
    on od.id = cou.order_detail_id                      -- 关联订单明细与优惠券信息
        left join
    (
        select
            dic_code,                                   -- 字典编码
            dic_name                                    -- 字典名称
        from ods.ods_base_dic_full                      -- 数据字典表
        where parent_code='24'                          -- 筛选来源类型字典数据(父编码为24)
    )dic
    on od.source_type=dic.dic_code;                     -- 关联来源类型编码与名称

/*
 * 设计说明:
 * 1. 数据增量加载逻辑:
 *    - 使用k1=date('${pdate}')进行分区过滤，确保只处理当天新增或变更的数据
 *    - 订单取消可能在订单创建后的任何时间点发生，此脚本捕获当天发生的取消事件
 *    
 * 2. 多表关联设计:
 *    - 通过INNER JOIN关联订单明细表与订单主表，确保只处理已取消的订单
 *    - 使用LEFT JOIN关联活动和优惠券表，处理可能存在的促销信息
 *    - 通过LEFT JOIN关联数据字典表，将来源类型代码转换为可读的名称
 *
 * 3. 取消订单的识别:
 *    - 订单主表中order_status='1003'表示订单已取消
 *    - 使用operate_time作为取消时间，记录实际取消操作的时间点
 *
 * 4. 金额计算逻辑:
 *    - split_original_amount通过sku_num * order_price计算原始金额
 *    - split_activity_amount和split_coupon_amount记录优惠金额
 *    - split_total_amount为最终应付金额
 *    - 这些金额分拆可以精确分析每个SKU的价格构成
 *
 * 5. 执行策略:
 *    - 此脚本设计为每日执行一次，增量加载前一天的数据
 *    - 脚本参数${pdate}通常由调度系统设置为当前日期的前一天
 *    - 执行后将新数据追加到目标表，实现数据的持续积累
 *
 * 6. 数据应用场景:
 *    - 订单取消原因分析：结合其他维度了解用户取消订单的可能原因
 *    - 商品退出率分析：统计不同商品被加入购物车后最终被取消的比例
 *    - 营销活动有效性评估：分析参与活动的订单取消率
 *    - 用户行为分析：识别频繁取消订单的用户群体特征
 *    - 销售预测调整：根据取消订单情况调整销售预测模型
 *    - 日环比分析：比较不同日期的订单取消率变化趋势
 */
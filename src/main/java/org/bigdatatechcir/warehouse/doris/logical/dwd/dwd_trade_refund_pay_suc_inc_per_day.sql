/*
 * 文件名: dwd_trade_refund_pay_suc_inc_per_day.sql
 * 功能描述: 交易域退款成功事务事实表(每日增量) - 记录退款成功的明细信息
 * 数据粒度: 退款支付成功事件
 * 刷新策略: 每日增量加载
 * 调度周期: 每日
 * 依赖表: 
 *   - ods.ods_refund_payment_full: 退款支付信息表
 *   - ods.ods_order_info_full: 订单主表
 *   - ods.ods_order_refund_info_full: 订单退款信息表
 *   - ods.ods_base_dic_full: 数据字典表
 * 目标表: dwd.dwd_trade_refund_pay_suc_inc
 * 主要功能: 
 *   1. 提取每日新增的退款成功明细数据
 *   2. 整合退款支付信息、订单信息、退款数量等维度数据
 *   3. 为退款流程分析提供明细数据
 */

-- 交易域退款成功事务事实表
INSERT INTO dwd.dwd_trade_refund_pay_suc_inc(
    id,                -- 退款支付ID
    k1,                -- 数据日期分区
    user_id,           -- 用户ID
    order_id,          -- 订单ID
    sku_id,            -- 商品SKU_ID
    province_id,       -- 省份ID
    payment_type_code, -- 支付类型编码
    payment_type_name, -- 支付类型名称
    date_id,           -- 日期ID，格式yyyy-MM-dd
    callback_time,     -- 退款回调时间
    refund_num,        -- 退款商品数量
    refund_amount      -- 退款金额
)
select
    rp.id,                                          -- 退款支付ID
    k1,                                             -- 分区字段
    user_id,                                        -- 用户ID
    rp.order_id,                                    -- 订单ID
    rp.sku_id,                                      -- 商品SKU_ID
    province_id,                                    -- 省份ID
    payment_type,                                   -- 支付类型编码
    dic_name,                                       -- 支付类型名称
    date_format(callback_time,'yyyy-MM-dd') date_id, -- 将回调时间转换为日期ID格式
    callback_time,                                  -- 退款回调时间
    refund_num,                                     -- 退款商品数量
    total_amount                                    -- 退款金额
from
    (
        select
            id,                                     -- 退款支付ID
            k1,                                     -- 分区字段
            order_id,                               -- 订单ID
            sku_id,                                 -- 商品SKU_ID
            payment_type,                           -- 支付类型编码
            callback_time,                          -- 退款回调时间
            total_amount                            -- 退款金额
        from ods.ods_refund_payment_full
        where refund_status='1602'                  -- 筛选退款状态为"退款成功"的记录
          and k1=date('${pdate}')                   -- 只处理当日分区数据
    )rp
        left join
    (
        select
            id,                                     -- 订单ID
            user_id,                                -- 用户ID
            province_id                             -- 省份ID
        from ods.ods_order_info_full
        where k1=date('${pdate}')                   -- 只处理当日分区数据
    )oi
    on rp.order_id=oi.id                            -- 关联退款支付信息与订单主表
        left join
    (
        select
            order_id,                               -- 订单ID
            sku_id,                                 -- 商品SKU_ID
            refund_num                              -- 退款商品数量
        from ods.ods_order_refund_info_full
        where k1=date('${pdate}')                   -- 只处理当日分区数据
    )ri
    on rp.order_id=ri.order_id                      -- 关联退款支付信息与退款申请信息
        and rp.sku_id=ri.sku_id                     -- 订单ID和SKU_ID同时匹配
        left join
    (
        select
            dic_code,                               -- 字典编码
            dic_name                                -- 字典名称
        from ods.ods_base_dic_full
        where parent_code='11'                      -- 筛选支付类型字典数据(父编码为11)
          and k1=date('${pdate}')                   -- 只处理当日分区数据
    )dic
    on rp.payment_type=dic.dic_code;                -- 关联支付类型编码与名称

/*
 * 设计说明:
 * 1. 数据增量加载逻辑:
 *    - 使用k1=date('${pdate}')进行分区过滤，确保只处理当天新增的退款成功数据
 *    - 数据字典表也使用分区过滤，保证使用最新的编码映射关系
 *    
 * 2. 多表关联设计:
 *    - 通过LEFT JOIN关联订单主表，获取用户ID和省份ID等维度信息
 *    - 通过LEFT JOIN关联退款申请信息表，获取退款商品数量
 *    - 通过LEFT JOIN关联数据字典表，将支付类型代码转换为可读的名称
 *
 * 3. 退款成功的识别:
 *    - 退款支付表中refund_status='1602'表示退款成功
 *    - 使用callback_time作为退款成功时间，记录支付平台回调的时间点
 *
 * 4. 执行策略:
 *    - 此脚本设计为每日执行一次，增量加载前一天的数据
 *    - 脚本参数${pdate}通常由调度系统设置为当前日期的前一天
 *    - 执行后将新数据追加到目标表，实现数据的持续积累
 *
 * 5. 数据应用场景:
 *    - 退款完成率分析：比较退款申请与退款成功的比例
 *    - 退款周期分析：分析从申请退款到退款成功的平均时间
 *    - 支付方式分析：了解不同支付渠道的退款处理效率
 *    - 区域退款分析：基于省份维度分析退款情况
 *    - 日环比分析：比较不同日期的退款情况变化趋势
 */
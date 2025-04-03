/*
 * 文件名: dwd_trade_order_refund_inc_first.sql
 * 功能描述: 交易域退单事务事实表(首次加载) - 记录用户退货退款的明细信息
 * 数据粒度: 退货退款事件
 * 刷新策略: 首次增量加载(历史数据首次导入)
 * 调度周期: 一次性执行
 * 依赖表: 
 *   - ods.ods_order_refund_info_full: 订单退款信息表
 *   - ods.ods_order_info_full: 订单主表
 *   - ods.ods_base_dic_full: 数据字典表
 * 目标表: dwd.dwd_trade_order_refund_inc
 * 主要功能: 
 *   1. 提取历史所有退货退款明细数据
 *   2. 整合订单维度信息和字典表数据
 *   3. 为退款分析提供完整历史数据基础
 */

-- 交易域退单事务事实表
INSERT INTO dwd.dwd_trade_order_refund_inc(
    id,                      -- 退款记录ID
    k1,                      -- 数据日期分区
    user_id,                 -- 用户ID
    order_id,                -- 订单ID
    sku_id,                  -- 商品SKU_ID
    province_id,             -- 省份ID
    date_id,                 -- 日期ID，格式yyyy-MM-dd
    create_time,             -- 退款申请时间
    refund_type_code,        -- 退款类型编码
    refund_type_name,        -- 退款类型名称
    refund_reason_type_code, -- 退款原因类型编码
    refund_reason_type_name, -- 退款原因类型名称
    refund_reason_txt,       -- 退款原因描述
    refund_num,              -- 退款商品数量
    refund_amount            -- 退款金额
)
select
    ri.id,                                         -- 退款记录ID
    k1,                                            -- 分区字段
    user_id,                                       -- 用户ID
    order_id,                                      -- 订单ID
    sku_id,                                        -- 商品SKU_ID
    province_id,                                   -- 省份ID
    date_format(create_time,'yyyy-MM-dd') date_id, -- 将创建时间转换为日期ID格式
    create_time,                                   -- 退款申请时间
    refund_type,                                   -- 退款类型编码
    type_dic.dic_name,                             -- 退款类型名称(从字典表获取)
    refund_reason_type,                            -- 退款原因类型编码
    reason_dic.dic_name,                           -- 退款原因类型名称(从字典表获取)
    refund_reason_txt,                             -- 退款原因描述
    refund_num,                                    -- 退款商品数量
    refund_amount                                  -- 退款金额
from
    (
        select
            id,                                    -- 退款记录ID
            k1,                                    -- 分区字段
            user_id,                               -- 用户ID
            order_id,                              -- 订单ID
            sku_id,                                -- 商品SKU_ID
            refund_type,                           -- 退款类型编码
            refund_num,                            -- 退款商品数量
            refund_amount,                         -- 退款金额
            refund_reason_type,                    -- 退款原因类型编码
            refund_reason_txt,                     -- 退款原因描述
            create_time                            -- 退款申请时间
        from ods.ods_order_refund_info_full
        -- 注：首次加载不使用k1过滤，加载所有历史数据
    )ri
        left join
    (
        select
            id,                                    -- 订单ID
            province_id                            -- 省份ID
        from ods.ods_order_info_full
        -- 注：首次加载不使用k1过滤，获取所有历史订单信息
    )oi
    on ri.order_id=oi.id                           -- 关联订单退款信息与订单主表
        left join
    (
        select
            dic_code,                              -- 字典编码
            dic_name                               -- 字典名称
        from ods.ods_base_dic_full
        where parent_code = '15'                   -- 筛选退款类型字典数据(父编码为15)
        -- 注：首次加载不使用k1过滤，获取所有字典数据
    )type_dic
    on ri.refund_type=type_dic.dic_code            -- 关联退款类型编码与名称
        left join
    (
        select
            dic_code,                              -- 字典编码
            dic_name                               -- 字典名称
        from ods.ods_base_dic_full
        where parent_code = '13'                   -- 筛选退款原因类型字典数据(父编码为13)
        -- 注：首次加载不使用k1过滤，获取所有字典数据
    )reason_dic
    on ri.refund_reason_type=reason_dic.dic_code;  -- 关联退款原因类型编码与名称

/*
 * 设计说明:
 * 1. 首次加载特点:
 *    - 此脚本用于DWD层初始化时一次性执行
 *    - 不使用分区日期(k1)过滤，加载所有历史退款数据
 *    - 后续日常加载应使用dwd_trade_order_refund_inc_per_day.sql
 *    
 * 2. 多表关联设计:
 *    - 通过LEFT JOIN关联订单主表，获取省份ID等维度信息
 *    - 使用LEFT JOIN关联数据字典表，将代码转换为可读的名称
 *      (退款类型和退款原因类型使用不同的父编码进行筛选)
 *
 * 3. 退款信息的解析:
 *    - refund_type对应退款类型（如：仅退款、退货退款等）
 *    - refund_reason_type对应退款原因类型（如：质量问题、尺寸不合适等）
 *    - refund_reason_txt为用户填写的具体退款原因描述
 *
 * 4. 金额计算:
 *    - refund_amount记录实际退款金额
 *    - refund_num记录退款商品数量
 *
 * 5. 执行策略:
 *    - 此脚本应在数仓初始化阶段执行一次
 *    - 执行前应确保目标表为空，避免数据重复
 *    - 执行后应立即切换到每日增量加载模式
 *
 * 6. 数据应用场景:
 *    - 退款原因分析：了解用户退款的主要原因，改进产品和服务
 *    - 商品质量监控：识别退款率高的商品，分析潜在质量问题
 *    - 用户满意度评估：通过退款率和原因分析用户满意度
 *    - 退款类型分析：比较不同退款类型的分布，优化退款流程
 *    - 历史趋势分析：分析不同时期的退款情况变化
 */
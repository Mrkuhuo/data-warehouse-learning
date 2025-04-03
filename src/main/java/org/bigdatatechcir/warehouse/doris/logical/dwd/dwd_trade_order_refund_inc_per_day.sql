/*
 * 文件名: dwd_trade_order_refund_inc_per_day.sql
 * 功能描述: 交易域退单事务事实表(每日增量) - 记录用户退货退款的明细信息
 * 数据粒度: 退货退款事件
 * 刷新策略: 每日增量加载
 * 调度周期: 每日
 * 依赖表: 
 *   - ods.ods_order_refund_info_full: 订单退款信息表
 *   - ods.ods_order_info_full: 订单主表
 *   - ods.ods_base_dic_full: 数据字典表
 * 目标表: dwd.dwd_trade_order_refund_inc
 * 主要功能: 
 *   1. 提取每日新增的退货退款明细数据
 *   2. 整合订单维度信息和字典表数据
 *   3. 为退款分析提供明细数据
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
        where k1=date('${pdate}')                  -- 只处理当日分区数据
    )ri
        left join
    (
        select
            id,                                    -- 订单ID
            province_id                            -- 省份ID
        from ods.ods_order_info_full
        where k1=date('${pdate}')                  -- 只处理当日分区数据
    )oi
    on ri.order_id=oi.id                           -- 关联订单退款信息与订单主表
        left join
    (
        select
            dic_code,                              -- 字典编码
            dic_name                               -- 字典名称
        from ods.ods_base_dic_full
        where parent_code = '15'                   -- 筛选退款类型字典数据(父编码为15)
          and k1=date('${pdate}')                  -- 只处理当日分区数据
    )type_dic
    on ri.refund_type=type_dic.dic_code            -- 关联退款类型编码与名称
        left join
    (
        select
            dic_code,                              -- 字典编码
            dic_name                               -- 字典名称
        from ods.ods_base_dic_full
        where parent_code = '13'                   -- 筛选退款原因类型字典数据(父编码为13)
          and k1=date('${pdate}')                  -- 只处理当日分区数据
    )reason_dic
    on ri.refund_reason_type=reason_dic.dic_code;  -- 关联退款原因类型编码与名称

/*
 * 设计说明:
 * 1. 数据增量加载逻辑:
 *    - 使用k1=date('${pdate}')进行分区过滤，确保只处理当天新增的退款数据
 *    - 数据字典表也使用分区过滤，保证使用最新的编码映射关系
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
 *    - 此脚本设计为每日执行一次，增量加载前一天的数据
 *    - 脚本参数${pdate}通常由调度系统设置为当前日期的前一天
 *    - 执行后将新数据追加到目标表，实现数据的持续积累
 *
 * 6. 数据应用场景:
 *    - 退款原因分析：了解用户退款的主要原因，改进产品和服务
 *    - 商品质量监控：识别退款率高的商品，分析潜在质量问题
 *    - 用户满意度评估：通过退款率和原因分析用户满意度
 *    - 退款类型分析：比较不同退款类型的分布，优化退款流程
 *    - 日环比分析：比较不同日期的退款情况变化趋势
 */
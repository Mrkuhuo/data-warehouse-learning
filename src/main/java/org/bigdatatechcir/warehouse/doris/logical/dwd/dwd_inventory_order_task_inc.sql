/*
 * 文件名: dwd_inventory_order_task_inc.sql
 * 功能描述: 库存域订单任务事实表 - 记录订单对应的仓库任务信息
 * 数据粒度: 订单任务(一个订单在一个仓库的处理任务)
 * 刷新策略: 增量加载
 * 调度周期: 每日
 * 依赖表: 
 *   - ods.ods_ware_order_task_full: 仓库订单任务表
 *   - ods.ods_ware_info_full: 仓库信息表
 *   - ods.ods_ware_order_task_detail_full: 仓库订单任务明细表
 *   - ods.ods_order_info_full: 订单信息表
 * 目标表: dwd.dwd_inventory_order_task_inc
 * 主要功能: 
 *   1. 整合订单任务主表与相关维度信息
 *   2. 聚合任务明细数据，计算商品数量和关联SKU
 *   3. 补充仓库名称和订单地址信息
 *   4. 为物流配送和仓库管理提供数据支持
 */

-- 库存域订单任务事实表
INSERT INTO dwd.dwd_inventory_order_task_inc(
    id,                -- 记录ID
    k1,                -- 数据日期分区
    task_id,           -- 任务ID
    order_id,          -- 订单ID
    consignee,         -- 收货人
    consignee_tel,     -- 收货人电话
    delivery_address,  -- 配送地址
    order_comment,     -- 订单备注
    payment_way,       -- 支付方式
    task_status,       -- 任务状态
    order_body,        -- 订单内容描述
    tracking_no,       -- 物流单号
    warehouse_id,      -- 仓库ID
    warehouse_name,    -- 仓库名称
    create_time,       -- 创建时间
    consign_time,      -- 发货时间
    finish_time,       -- 完成时间
    cancel_time,       -- 取消时间
    cancel_reason,     -- 取消原因
    sku_num,           -- 商品总数量
    sku_ids,           -- 商品SKU ID集合
    province_id,       -- 省份ID
    city_id,           -- 城市ID
    region_id,         -- 区域ID
    user_id            -- 用户ID
)
select
    -- 保留原始ID值，不使用ABS函数，保证数据一致性和跟踪性
    CAST(wot.id AS STRING) as id,                              -- 使用任务ID作为记录ID
    date('${pdate}') as k1,                                    -- 使用参数日期作为k1值
    CAST(wot.id AS STRING) as task_id,                         -- 同ID字段，任务标识
    CAST(COALESCE(wot.order_id, '0') AS STRING) as order_id,   -- 关联的订单ID
    CAST(COALESCE(wot.consignee, '') AS STRING) as consignee,  -- 收货人姓名
    CAST(COALESCE(wot.consignee_tel, '') AS STRING) as consignee_tel, -- 收货人电话
    CAST(COALESCE(wot.delivery_address, '') AS STRING) as delivery_address, -- 配送地址
    CAST(COALESCE(wot.order_comment, '') AS STRING) as order_comment, -- 订单备注
    CAST(COALESCE(wot.payment_way, '0') AS STRING) as payment_way, -- 支付方式编码
    CAST(COALESCE(wot.task_status, '0') AS STRING) as task_status, -- 任务状态编码
    CAST(COALESCE(wot.order_body, '') AS STRING) as order_body, -- 订单内容描述
    CAST(COALESCE(wot.tracking_no, '') AS STRING) as tracking_no, -- 物流单号
    CAST(COALESCE(wot.ware_id, '0') AS STRING) as warehouse_id, -- 仓库ID
    CAST(COALESCE(wi.name, '默认仓库') AS STRING) as warehouse_name, -- 仓库名称
    -- 使用统一的日期格式
    CAST(DATE_FORMAT(COALESCE(wot.create_time, NOW()), '%Y-%m-%d %H:%i:%s') AS STRING) as create_time, -- 任务创建时间
    CAST(null AS STRING) as consign_time,                      -- 需要根据实际情况获取，这里暂时为null
    CAST(null AS STRING) as finish_time,                       -- 需要根据实际情况获取，这里暂时为null  
    CAST(null AS STRING) as cancel_time,                       -- 需要根据实际情况获取，这里暂时为null
    CAST(null AS STRING) as cancel_reason,                     -- 需要根据实际情况获取，这里暂时为null
    CAST(COALESCE(wtd.total_sku_num, 0) AS BIGINT) as sku_num, -- 任务中商品总数量
    CAST(COALESCE(wtd.sku_ids, '') AS STRING) as sku_ids,     -- 逗号分隔的SKU ID列表
    CAST(COALESCE(oi.province_id, '0') AS STRING) as province_id, -- 省份ID
    -- ods_user_address_full表中没有city_id和district_id字段，改为使用默认值
    CAST('0' AS STRING) as city_id,                            -- 城市ID，暂用默认值
    CAST('0' AS STRING) as region_id,                          -- 区域ID，暂用默认值
    CAST(COALESCE(oi.user_id, '0') AS STRING) as user_id       -- 用户ID
from
    (
        -- 订单任务子查询：获取基础任务信息
        select
            id,                                   -- 任务ID
            order_id,                             -- 关联订单ID
            consignee,                            -- 收货人
            consignee_tel,                        -- 收货人电话
            delivery_address,                     -- 配送地址
            order_comment,                        -- 订单备注
            payment_way,                          -- 支付方式
            task_status,                          -- 任务状态
            order_body,                           -- 订单内容描述
            tracking_no,                          -- 物流单号
            ware_id,                              -- 仓库ID
            create_time                           -- 创建时间
        from ods.ods_ware_order_task_full
        where k1=date('${pdate}')                 -- 按分区日期过滤，只处理当天数据
    ) wot
    left join
    (
        -- 仓库信息子查询：获取仓库名称
        select
            id,                                   -- 仓库ID
            name                                  -- 仓库名称
        from ods.ods_ware_info_full
        where k1=date('${pdate}')                 -- 按分区日期过滤，只处理当天数据
    ) wi
    on wot.ware_id = wi.id                        -- 通过仓库ID关联仓库信息
    left join
    (
        -- 任务明细聚合子查询：聚合每个任务的商品数量和SKU列表
        select
            task_id,                              -- 任务ID
            sum(sku_num) as total_sku_num,        -- 计算任务中商品总数量
            GROUP_CONCAT(sku_id, ',') as sku_ids  -- 将该任务下所有SKU ID合并为逗号分隔的字符串
        from ods.ods_ware_order_task_detail_full
        group by task_id                          -- 按任务ID分组聚合
    ) wtd
    on wot.id = wtd.task_id                       -- 通过任务ID关联聚合的任务明细
    left join
    (
        -- 订单信息子查询：获取用户和地址信息
        select
            id,                                   -- 订单ID
            user_id,                              -- 用户ID
            province_id,                          -- 省份ID
            city_id,                              -- 城市ID
            district_id                           -- 区域ID
        from ods.ods_order_info_full
        where k1=date('${pdate}')                 -- 按分区日期过滤，只处理当天数据
    ) oi
    on wot.order_id = oi.id                       -- 通过订单ID关联订单信息
WHERE 
    -- 确保k1在动态分区范围内，与表定义匹配
    date('${pdate}') BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY) AND DATE_ADD(CURRENT_DATE(), INTERVAL 3 DAY)
    -- 增加数据有效性验证
    AND wot.id IS NOT NULL;

/*
 * 设计说明:
 * 1. 数据聚合设计:
 *    - 使用GROUP_CONCAT函数将任务中的所有SKU ID合并成逗号分隔的字符串
 *    - 通过sum函数计算每个任务中的商品总数量
 *    - 这种聚合设计使查询结果更加简洁，但保留了商品级别的信息
 *    
 * 2. 数据类型标准化:
 *    - 一致使用CAST函数转换数据类型，确保与目标表定义一致
 *    - 对于NULL值和空值处理采用COALESCE函数提供默认值
 *    - 字符串类型字段为空时默认为空字符串，数值类型字段为空时默认为0
 *
 * 3. 日期字段处理:
 *    - 对于状态相关的时间字段(consign_time, finish_time, cancel_time)，暂时设置为NULL
 *    - 在实际系统中，应根据task_status的值和具体业务逻辑填充这些字段
 *    - 若需完整实现，应考虑使用事件日志表或变更历史记录
 *
 * 4. 地址信息补充:
 *    - 由于源数据可能缺少city_id和region_id，目前使用默认值'0'
 *    - 后续可考虑通过地址解析服务或关联更丰富的地址维度表进行补充
 *
 * 5. 多表关联设计:
 *    - 以订单任务表为主表，左连接关联其他维度信息
 *    - 采用left join确保主表数据不会因关联失败而丢失
 *    - 通过任务ID和订单ID保持关联链路的完整性
 *
 * 6. 数据应用场景:
 *    - 物流管理：追踪订单从仓库到客户的配送过程
 *    - 仓库运营：分析各仓库的订单处理效率和能力
 *    - 订单履约：评估从订单创建到发货的时间和流程
 *    - 异常分析：识别取消订单的原因和模式
 */
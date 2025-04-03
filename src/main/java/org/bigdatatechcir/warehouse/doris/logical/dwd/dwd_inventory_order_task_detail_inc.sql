/*
 * 文件名: dwd_inventory_order_task_detail_inc.sql
 * 功能描述: 库存域订单任务明细事实表 - 记录订单拆分后的任务明细信息
 * 数据粒度: 订单任务明细项(一个任务中的一个商品)
 * 刷新策略: 增量加载
 * 调度周期: 每日
 * 依赖表: 
 *   - ods.ods_ware_order_task_detail_full: 仓库订单任务明细表
 *   - ods.ods_ware_order_task_full: 仓库订单任务表
 *   - ods.ods_sku_info_full: 商品SKU信息表
 *   - ods.ods_ware_info_full: 仓库信息表
 *   - ods.ods_order_detail_full: 订单明细表
 *   - ods.ods_order_info_full: 订单信息表
 * 目标表: dwd.dwd_inventory_order_task_detail_inc
 * 主要功能: 
 *   1. 整合订单任务明细与相关维度信息
 *   2. 补充商品、仓库和订单相关信息
 *   3. 处理价格计算和数据类型转换
 *   4. 为库存和物流分析提供基础数据
 */

-- 库存域订单任务明细事实表
INSERT INTO dwd.dwd_inventory_order_task_detail_inc(
    id,                   -- 任务明细记录ID
    k1,                   -- 数据日期分区
    task_id,              -- 任务ID
    order_id,             -- 订单ID
    sku_id,               -- 商品SKU ID
    sku_name,             -- 商品名称
    sku_num,              -- 商品数量
    sku_price,            -- 商品单价
    warehouse_id,         -- 仓库ID
    warehouse_name,       -- 仓库名称
    create_time,          -- 创建时间
    source_id,            -- 来源ID
    source_type,          -- 来源类型
    split_total_amount,   -- 拆分后总金额
    split_activity_amount,-- 拆分活动金额
    split_coupon_amount,  -- 拆分优惠券金额
    task_status,          -- 任务状态
    tracking_no,          -- 物流单号
    user_id               -- 用户ID
)
select
    -- 保留原始ID值，不使用ABS函数，保证数据一致性和跟踪性
    CAST(wotd.id AS STRING) as id,                   -- 转换为字符串类型
    date('${pdate}') as k1,                          -- 使用参数日期作为k1值
    CAST(wotd.task_id AS STRING) as task_id,         -- 保留原始task_id
    CAST(wot.order_id AS STRING) as order_id,        -- 关联自订单任务表的订单ID
    CAST(wotd.sku_id AS STRING) as sku_id,           -- 商品SKU ID
    COALESCE(wotd.sku_name, si.sku_name, '未知商品') as sku_name, -- 优先使用任务明细表中的商品名称，其次是SKU表中的
    CAST(COALESCE(wotd.sku_num, 0) AS BIGINT) as sku_num, -- 确保商品数量为非空值
    -- 改进价格计算逻辑，避免除零错误
    CAST(CASE 
        WHEN oi.sku_num > 0 AND oi.split_total_amount IS NOT NULL 
        THEN oi.split_total_amount / oi.sku_num 
        ELSE 0.00 
    END AS DECIMAL(16,2)) as sku_price,               -- 从订单明细获取单价信息
    CAST(wot.warehouse_id AS STRING) as warehouse_id, -- 关联自订单任务表的仓库ID
    CAST(COALESCE(wi.name, '默认仓库') AS STRING) as warehouse_name, -- 获取仓库名称，缺失时使用默认值
    -- 使用统一的日期格式
    CAST(DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:%s') AS STRING) as create_time, -- 使用当前时间作为创建时间
    CAST(COALESCE(oi.source_id, '0') AS STRING) as source_id,       -- 订单来源ID
    CAST(COALESCE(oi.source_type, '0') AS STRING) as source_type,   -- 订单来源类型
    CAST(COALESCE(oi.split_total_amount, 0.00) AS DECIMAL(16,2)) as split_total_amount,    -- 确保金额字段不为空
    CAST(COALESCE(oi.split_activity_amount, 0.00) AS DECIMAL(16,2)) as split_activity_amount,
    CAST(COALESCE(oi.split_coupon_amount, 0.00) AS DECIMAL(16,2)) as split_coupon_amount,
    CAST(COALESCE(wot.task_status, '0') AS STRING) as task_status,  -- 任务状态，缺失时使用默认值
    CAST(COALESCE(wot.tracking_no, '0') AS STRING) as tracking_no,  -- 物流单号，缺失时使用默认值
    CAST(COALESCE(oi.user_id, '0') AS STRING) as user_id            -- 用户ID
from
    (
        -- 订单任务明细子查询：获取基础的任务明细信息
        select
            id,                          -- 任务明细ID
            task_id,                     -- 任务ID
            sku_id,                      -- 商品SKU ID
            sku_name,                    -- 商品名称
            sku_num                      -- 商品数量
        from ods.ods_ware_order_task_detail_full
        where id IS NOT NULL AND task_id IS NOT NULL -- 添加基本数据质量筛选
    ) wotd
    left join
    (
        -- 订单任务子查询：获取任务和订单的关联信息
        select
            id,                          -- 任务ID
            order_id,                    -- 订单ID
            ware_id as warehouse_id,     -- 仓库ID
            task_status,                 -- 任务状态
            tracking_no                  -- 物流单号
        from ods.ods_ware_order_task_full
        where k1 = date('${pdate}')      -- 按分区日期过滤，只处理当天数据
    ) wot
    on wotd.task_id = wot.id             -- 通过任务ID关联任务表
    left join
    (
        -- 商品信息子查询：获取商品名称
        select
            id,                          -- 商品SKU ID
            sku_name                     -- 商品名称
        from ods.ods_sku_info_full
        where k1 = date('${pdate}')      -- 按分区日期过滤，只处理当天数据
    ) si
    on wotd.sku_id = si.id               -- 通过商品SKU ID关联商品信息
    left join
    (
        -- 仓库信息子查询：获取仓库名称
        select
            id,                          -- 仓库ID
            name                         -- 仓库名称
        from ods.ods_ware_info_full
    ) wi
    on wot.warehouse_id = wi.id          -- 通过仓库ID关联仓库信息
    left join
    (
        -- 订单明细子查询：获取订单和价格信息
        select
            oi.id as order_id,           -- 订单ID
            od.sku_id,                   -- 商品SKU ID
            od.source_id,                -- 来源ID
            od.source_type,              -- 来源类型
            od.sku_num,                  -- 商品数量
            od.split_total_amount,       -- 拆分总金额
            od.split_activity_amount,    -- 拆分活动金额
            od.split_coupon_amount,      -- 拆分优惠券金额
            oi.user_id                   -- 用户ID
        from ods.ods_order_detail_full od
        join ods.ods_order_info_full oi
        on od.order_id = oi.id           -- 通过订单ID关联订单主表
        where od.k1 = date('${pdate}') and oi.k1 = date('${pdate}') -- 两个表都按分区日期过滤
    ) oi
    on wot.order_id = oi.order_id and wotd.sku_id = oi.sku_id -- 通过订单ID和商品ID双重关联
WHERE 
    -- 确保k1在动态分区范围内，与表定义匹配
    date('${pdate}') BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY) AND DATE_ADD(CURRENT_DATE(), INTERVAL 3 DAY)
    -- 增加数据有效性验证
    AND wotd.id IS NOT NULL; 

/*
 * 设计说明:
 * 1. 数据类型标准化:
 *    - 使用CAST函数统一转换字段类型，确保与目标表定义一致
 *    - 对数值型字段设置适当的精度，特别是金额字段使用DECIMAL(16,2)
 *    - ID字段统一使用字符串类型，避免整数溢出问题
 *    
 * 2. 空值处理策略:
 *    - 使用COALESCE函数处理可能为空的字段，提供合理的默认值
 *    - 对关键字段如商品名称提供友好的默认值（如"未知商品"）
 *    - 金额字段默认为0.00，避免空值带来的计算问题
 *
 * 3. 价格计算逻辑:
 *    - 优化sku_price计算，通过判断sku_num是否大于0来避免除零错误
 *    - 金额精确到两位小数，符合财务数据处理规范
 *
 * 4. 多表关联设计:
 *    - 以订单任务明细表为主表，关联其他维度信息
 *    - 使用left join保证主表数据不会因关联失败而丢失
 *    - 通过订单ID和商品ID双重关联订单明细，确保关联精确性
 *
 * 5. 数据有效性控制:
 *    - 添加日期范围验证，确保数据在合理的时间范围内
 *    - 对关键字段(wotd.id)添加非空验证，提高数据质量
 *
 * 6. 数据应用场景:
 *    - 库存管理：分析订单任务的执行情况和商品出库情况
 *    - 仓库绩效评估：评估不同仓库的订单处理效率
 *    - 物流追踪：结合物流单号分析订单配送情况
 *    - 商品销售分析：分析商品的销售和配送情况
 */ 
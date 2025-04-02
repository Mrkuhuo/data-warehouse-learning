-- 库存域订单任务明细事实表
INSERT INTO dwd.dwd_inventory_order_task_detail_inc(id, k1, task_id, order_id, sku_id, sku_name, sku_num, sku_price, warehouse_id, warehouse_name, create_time, source_id, source_type, split_total_amount, split_activity_amount, split_coupon_amount, task_status, tracking_no, user_id)
select
    -- 保留原始ID值，不使用ABS函数，保证数据一致性和跟踪性
    CAST(wotd.id AS STRING) as id,
    date('${pdate}') as k1,  -- 使用参数日期作为k1值
    CAST(wotd.task_id AS STRING) as task_id, -- 保留原始task_id
    CAST(wot.order_id AS STRING) as order_id,
    CAST(wotd.sku_id AS STRING) as sku_id,
    COALESCE(wotd.sku_name, si.sku_name, '未知商品') as sku_name,
    CAST(COALESCE(wotd.sku_num, 0) AS BIGINT) as sku_num,
    -- 改进价格计算逻辑，避免除零错误
    CAST(CASE 
        WHEN oi.sku_num > 0 AND oi.split_total_amount IS NOT NULL 
        THEN oi.split_total_amount / oi.sku_num 
        ELSE 0.00 
    END AS DECIMAL(16,2)) as sku_price,
    CAST(wot.warehouse_id AS STRING) as warehouse_id,
    CAST(COALESCE(wi.name, '默认仓库') AS STRING) as warehouse_name,
    -- 使用统一的日期格式
    CAST(DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:%s') AS STRING) as create_time,
    CAST(COALESCE(oi.source_id, '0') AS STRING) as source_id,
    CAST(COALESCE(oi.source_type, '0') AS STRING) as source_type,
    CAST(COALESCE(oi.split_total_amount, 0.00) AS DECIMAL(16,2)) as split_total_amount,
    CAST(COALESCE(oi.split_activity_amount, 0.00) AS DECIMAL(16,2)) as split_activity_amount,
    CAST(COALESCE(oi.split_coupon_amount, 0.00) AS DECIMAL(16,2)) as split_coupon_amount,
    CAST(COALESCE(wot.task_status, '0') AS STRING) as task_status,
    CAST(COALESCE(wot.tracking_no, '0') AS STRING) as tracking_no,
    CAST(COALESCE(oi.user_id, '0') AS STRING) as user_id
from
    (
        select
            id,
            task_id,
            sku_id,
            sku_name,
            sku_num
        from ods.ods_ware_order_task_detail_full
        where id IS NOT NULL AND task_id IS NOT NULL -- 添加基本数据质量筛选
    ) wotd
    left join
    (
        select
            id,
            order_id,
            ware_id as warehouse_id,
            task_status,
            tracking_no
        from ods.ods_ware_order_task_full
        where k1 = date('${pdate}')
    ) wot
    on wotd.task_id = wot.id
    left join
    (
        select
            id,
            sku_name
        from ods.ods_sku_info_full
        where k1 = date('${pdate}')
    ) si
    on wotd.sku_id = si.id
    left join
    (
        select
            id,
            name
        from ods.ods_ware_info_full
    ) wi
    on wot.warehouse_id = wi.id
    left join
    (
        select
            oi.id as order_id,
            od.sku_id,
            od.source_id,
            od.source_type,
            od.sku_num,
            od.split_total_amount,
            od.split_activity_amount,
            od.split_coupon_amount,
            oi.user_id
        from ods.ods_order_detail_full od
        join ods.ods_order_info_full oi
        on od.order_id = oi.id
        where od.k1 = date('${pdate}') and oi.k1 = date('${pdate}')
    ) oi
    on wot.order_id = oi.order_id and wotd.sku_id = oi.sku_id
WHERE 
    -- 确保k1在动态分区范围内，与表定义匹配
    date('${pdate}') BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY) AND DATE_ADD(CURRENT_DATE(), INTERVAL 3 DAY)
    -- 增加数据有效性验证
    AND wotd.id IS NOT NULL; 
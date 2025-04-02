-- 库存域订单任务事实表
INSERT INTO dwd.dwd_inventory_order_task_inc(id, k1, task_id, order_id, consignee, consignee_tel, delivery_address, order_comment, payment_way, task_status, order_body, tracking_no, warehouse_id, warehouse_name, create_time, consign_time, finish_time, cancel_time, cancel_reason, sku_num, sku_ids, province_id, city_id, region_id, user_id)
select
    -- 保留原始ID值，不使用ABS函数，保证数据一致性和跟踪性
    CAST(wot.id AS STRING) as id,
    date('${pdate}') as k1, -- 使用参数日期作为k1值
    CAST(wot.id AS STRING) as task_id,
    CAST(COALESCE(wot.order_id, '0') AS STRING) as order_id,
    CAST(COALESCE(wot.consignee, '') AS STRING) as consignee,
    CAST(COALESCE(wot.consignee_tel, '') AS STRING) as consignee_tel,
    CAST(COALESCE(wot.delivery_address, '') AS STRING) as delivery_address,
    CAST(COALESCE(wot.order_comment, '') AS STRING) as order_comment,
    CAST(COALESCE(wot.payment_way, '0') AS STRING) as payment_way,
    CAST(COALESCE(wot.task_status, '0') AS STRING) as task_status,
    CAST(COALESCE(wot.order_body, '') AS STRING) as order_body,
    CAST(COALESCE(wot.tracking_no, '') AS STRING) as tracking_no,
    CAST(COALESCE(wot.ware_id, '0') AS STRING) as warehouse_id,
    CAST(COALESCE(wi.name, '默认仓库') AS STRING) as warehouse_name,
    -- 使用统一的日期格式
    CAST(DATE_FORMAT(COALESCE(wot.create_time, NOW()), '%Y-%m-%d %H:%i:%s') AS STRING) as create_time,
    CAST(null AS STRING) as consign_time,    -- 需要根据实际情况获取，这里暂时为null
    CAST(null AS STRING) as finish_time,     -- 需要根据实际情况获取，这里暂时为null  
    CAST(null AS STRING) as cancel_time,     -- 需要根据实际情况获取，这里暂时为null
    CAST(null AS STRING) as cancel_reason,   -- 需要根据实际情况获取，这里暂时为null
    CAST(COALESCE(wtd.total_sku_num, 0) AS BIGINT) as sku_num,
    CAST(COALESCE(wtd.sku_ids, '') AS STRING) as sku_ids,
    CAST(COALESCE(oi.province_id, '0') AS STRING) as province_id,
    -- ods_user_address_full表中没有city_id和district_id字段，改为使用默认值
    CAST('0' AS STRING) as city_id,
    CAST('0' AS STRING) as region_id,
    CAST(COALESCE(oi.user_id, '0') AS STRING) as user_id
from
    (
        select
            id,
            order_id,
            consignee,
            consignee_tel,
            delivery_address,
            order_comment,
            payment_way,
            task_status,
            order_body,
            tracking_no,
            ware_id,           -- 确保这里使用ware_id
            create_time
        from ods.ods_ware_order_task_full
        where k1=date('${pdate}')
    ) wot
    left join
    (
        select
            id,
            name
        from ods.ods_ware_info_full
        where k1=date('${pdate}')
    ) wi
    on wot.ware_id = wi.id    -- 确保这里使用ware_id
    left join
    (
        select
            task_id,
            sum(sku_num) as total_sku_num,
            GROUP_CONCAT(sku_id, ',') as sku_ids
        from ods.ods_ware_order_task_detail_full
        group by task_id
    ) wtd
    on wot.id = wtd.task_id
    left join
    (
        select
            id,
            user_id,
            province_id,
            city_id,
            district_id
        from ods.ods_order_info_full
        where k1=date('${pdate}')
    ) oi
    on wot.order_id = oi.id
WHERE 
    -- 确保k1在动态分区范围内，与表定义匹配
    date('${pdate}') BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY) AND DATE_ADD(CURRENT_DATE(), INTERVAL 3 DAY)
    -- 增加数据有效性验证
    AND wot.id IS NOT NULL;
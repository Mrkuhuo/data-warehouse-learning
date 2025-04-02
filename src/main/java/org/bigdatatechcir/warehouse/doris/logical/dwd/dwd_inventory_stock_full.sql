-- 库存域库存全量表
INSERT INTO dwd.dwd_inventory_stock_full(id, k1, sku_id, sku_name, warehouse_id, warehouse_name, warehouse_address, warehouse_areacode, stock, stock_name, is_default, create_time)
select
    ws.id,
    date('${pdate}') as k1,
    ws.sku_id,
    si.sku_name,
    ws.warehouse_id,
    wi.name as warehouse_name,
    wi.address as warehouse_address,
    wi.areacode as warehouse_areacode,
    ws.stock,
    ws.stock_name,
    COALESCE(ws.is_default, '0') as is_default,
    ws.create_time
from
    (
        select
            id,
            sku_id,
            warehouse_id,
            stock,
            stock_name,
            stock_locked as is_default,
            null as create_time
        from ods.ods_ware_sku_full
    ) ws
    left join
    (
        select
            id,
            name,
            address,
            areacode
        from ods.ods_ware_info_full
    ) wi
    on ws.warehouse_id = wi.id
    left join
    (
        select
            id,
            sku_name
        from ods.ods_sku_info_full
        where k1=date('${pdate}')
    ) si
    on ws.sku_id = si.id; 
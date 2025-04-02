-- 营销域秒杀全量表
INSERT INTO dwd.dwd_marketing_seckill_full(id, k1, seckill_id, sku_id, sku_name, sku_default_img, original_price, seckill_price, create_time, check_time, status, start_time, end_time, total_num, stock_count, sku_desc, spu_id)
select
    sg.id,
    sg.k1,
    sg.id as seckill_id,
    sg.sku_id,
    si.sku_name,
    si.sku_default_img,
    sg.price as original_price,
    sg.cost_price as seckill_price,
    sg.create_time,
    sg.check_time,
    sg.status,
    sg.start_time,
    sg.end_time,
    sg.num as total_num,
    sg.stock_count,
    sg.sku_desc,
    sg.spu_id
from
    (
        select
            id,
            k1,
            sku_id,
            spu_id,
            price,
            cost_price,
            create_time,
            check_time,
            status,
            start_time,
            end_time,
            num,
            stock_count,
            sku_desc
        from ods.ods_seckill_goods_full
        where k1=date('${pdate}')
    ) sg
    left join
    (
        select
            id,
            sku_name,
            sku_default_img
        from ods.ods_sku_info_full
        where k1=date('${pdate}')
    ) si
    on sg.sku_id = si.id; 
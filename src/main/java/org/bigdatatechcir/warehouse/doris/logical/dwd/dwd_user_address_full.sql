-- 用户域用户地址全量表
INSERT INTO dwd.dwd_user_address_full(id, k1, user_id, province_id, province_name, city_id, city_name, district_id, district_name, detail_address, consignee, phone_num, is_default, create_time, operate_time, postal_code, full_address)
select
    ua.id,
    date('${pdate}') as k1,  -- 使用参数日期作为k1值
    ua.user_id,
    ua.province_id,
    bp.name as province_name,
    ua.city_id,
    bp.area_code as city_name, -- 这里假设使用area_code作为城市名称，实际应根据实际情况调整
    ua.district_id,
    bp.iso_code as district_name, -- 这里假设使用iso_code作为区域名称，实际应根据实际情况调整
    ua.user_address as detail_address, -- 将user_address字段映射为detail_address
    ua.consignee,
    ua.phone_num,
    ua.is_default,
    now() as create_time, -- 使用当前时间作为create_time
    now() as operate_time, -- 使用当前时间作为operate_time
    null as postal_code, -- 暂无此数据，可根据实际情况调整
    concat(bp.name, ' ', bp.area_code, ' ', bp.iso_code, ' ', ua.user_address) as full_address -- 完整地址拼接
from
    (
        select
            id,
            user_id,
            province_id,
            province_id as city_id, -- 暂用province_id代替city_id
            province_id as district_id, -- 暂用province_id代替district_id
            user_address,
            consignee,
            phone_num,
            is_default
        from ods.ods_user_address_full
    ) ua
    left join
    (
        select
            id,
            name,
            area_code,
            iso_code
        from ods.ods_base_province_full
    ) bp
    on ua.province_id = bp.id;
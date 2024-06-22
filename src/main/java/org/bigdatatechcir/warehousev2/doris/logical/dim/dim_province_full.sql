-- 地区维度表
insert into dim.dim_province_full(id, province_name, area_code, iso_code, iso_3166_2, region_id, region_name)
select
    province.id,
    province.name,
    province.area_code,
    province.iso_code,
    province.iso_3166_2,
    region_id,
    region_name
from
    (
        select
            id,
            name,
            region_id,
            area_code,
            iso_code,
            iso_3166_2
        from ods.ods_base_province_full
    )province
        left join
    (
        select
            id,
            region_name
        from ods.ods_base_region_full
    )region
    on province.region_id=region.id;
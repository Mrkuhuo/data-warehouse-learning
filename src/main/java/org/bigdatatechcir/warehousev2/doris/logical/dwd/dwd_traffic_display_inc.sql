-- 流量域曝光事务事实表
INSERT INTO dwd.dwd_traffic_display_inc(id, k1, province_id, brand, channel, is_new, model, mid_id, operate_system, user_id, version_code, during_time, page_item, page_item_type, last_page_id, page_id, source_type, date_id, display_time, display_type, display_item, display_item_type, display_order, display_pos_id)
select
    id,
    k1,
    province_id,
    brand,
    channel,
    common_is_new,
    model,
    mid_id,
    operate_system,
    user_id,
    version_code,
    page_during_time,
    page_item,
    page_item_type,
    page_last_page_id,
    page_page_id,
    page_source_type,
    date_format(FROM_UNIXTIME(ts,'GMT+8'),'yyyy-MM-dd') date_id,
    date_format(FROM_UNIXTIME(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') display_time,
    display_type,
    item,
    item_type,
    `order`,
    pos_id
from
    (
        select
            id,
            k1,
            common_ar area_code,
            common_ba brand,
            common_ch channel,
            common_is_new,
            common_md model,
            common_mid mid_id,
            common_os operate_system,
            common_uid user_id,
            common_vc version_code,
            page_during_time,
            page_item,
            page_item_type,
            page_last_page_id,
            page_page_id,
            page_source_type,
            json_extract(e1,'$.display_type') display_type,
            json_extract(e1,'$.item') item,
            json_extract(e1,'$.item_type') item_type,
            json_extract(e1,'$.order') `order`,
            json_extract(e1,'$.pos_id') pos_id,
            ts
        from  ods.ods_log_inc  lateral view explode_json_array_json(displays) tmp1 as e1
        where json_extract(e1,'$.display_type')  is not null
    )log
        left join
    (
        select
            id province_id,
            area_code
        from ods.ods_base_province_full
    )bp
    on log.area_code=bp.area_code;
-- 流量域错误事务事实表
INSERT INTO dwd.dwd_traffic_error_inc(id, k1, province_id, brand, channel, is_new, model, mid_id, operate_system, user_id, version_code, page_item, page_item_type, last_page_id, page_id, source_type, entry, loading_time, open_ad_id, open_ad_ms, open_ad_skip_ms, action_id, action_item, action_item_type, action_time, display_type, display_item, display_item_type, display_order, display_pos_id, date_id, error_time, error_code, error_msg)
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
    page_item,
    page_item_type,
    page_last_page_id,
    page_page_id,
    page_source_type,
    start_entry,
    start_loading_time,
    start_open_ad_id,
    start_open_ad_ms,
    start_open_ad_skip_ms,
    action_id,
    action_item,
    action_item_type,
    ts,
    display_type,
    item,
    item_type,
    `order`,
    pos_id,
    date_format(FROM_UNIXTIME(ts,'GMT+8'),'yyyy-MM-dd') date_id,
    date_format(FROM_UNIXTIME(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') error_time,
    err_error_code,
    error_msg
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
            page_item page_item,
            page_item_type page_item_type,
            page_last_page_id,
            page_page_id,
            page_source_type,
            start_entry,
            start_loading_time,
            start_open_ad_id,
            start_open_ad_ms,
            start_open_ad_skip_ms,
            json_extract(e1,'$.action_id') action_id,
            json_extract(e1,'$.item') action_item,
            json_extract(e1,'$.item_type') action_item_type,
            json_extract(e1,'$.ts') ts,
            json_extract(e2,'$.display_type') display_type,
            json_extract(e2,'$.item') item,
            json_extract(e2,'$.item_type') item_type,
            json_extract(e2,'$.order') `order`,
            json_extract(e2,'$.pos_id') pos_id,
            err_error_code,
            err_msg error_msg
        from ods.ods_log_inc
        lateral view explode_json_array_json(actions) tmp1 as e1
        lateral view explode_json_array_json(displays) tmp2 as e2
        where  err_error_code is not null
    )log
        join
    (
        select
            id province_id,
            area_code
        from ods.ods_base_province_full
    )bp
    on log.area_code=bp.area_code;
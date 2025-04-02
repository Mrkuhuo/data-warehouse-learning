-- 流量域错误事务事实表
INSERT INTO dwd.dwd_traffic_error_inc(id, k1, province_id, brand, channel, is_new, model, mid_id, operate_system, user_id, version_code, page_id, last_page_id, page_item, page_item_type, during_time, source_type, error_code, error_msg, date_id, error_time)
select
    id,
    k1,
    province_id,
    brand,
    channel,
    common_is_new as is_new,
    model,
    mid_id,
    operate_system,
    user_id,
    version_code,
    page_page_id as page_id,
    page_last_page_id as last_page_id,
    page_item,
    page_item_type,
    page_during_time as during_time,
    page_source_type as source_type,
    err_error_code as error_code,
    error_msg,
    date_format(FROM_UNIXTIME(ts,'GMT+8'),'yyyy-MM-dd') date_id,
    date_format(FROM_UNIXTIME(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') error_time
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
            json_extract(e1,'$.ts') ts,
            err_error_code,
            err_msg error_msg
        from ods.ods_log_full
                 lateral view explode_json_array_json(actions) tmp1 as e1
        where k1 = date('${pdate}')
          and err_error_code is not null
    )log
        join
    (
        select
            id province_id,
            area_code
        from ods.ods_base_province_full
    )bp
    on log.area_code=bp.area_code;
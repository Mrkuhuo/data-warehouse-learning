-- 流量域页面浏览事务事实表
INSERT INTO dwd.dwd_traffic_page_view_inc(id, k1, province_id, brand, channel, is_new, model, mid_id, operate_system, user_id, version_code, page_item, page_item_type, last_page_id, page_id, source_type, date_id, view_time, session_id, during_time)
select
    id,
    k1,
    province_id,
    brand,
    channel,
    is_new,
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
    date_format(FROM_UNIXTIME(ts,'GMT+8'),'yyyy-MM-dd') date_id,
    date_format(FROM_UNIXTIME(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') view_time,
    concat(mid_id,'-',LAST_VALUE(session_start_point) over (partition by mid_id order by ts)) session_id,
    page_during_time
from
    (
        select
            id,
            k1,
            common_ar area_code,
            common_ba brand,
            common_ch channel,
            common_is_new is_new,
            common_md model,
            common_mid mid_id,
            common_os operate_system,
            common_uid user_id,
            common_vc version_code,
            page_during_time,
            page_item ,
            page_item_type ,
            page_last_page_id,
            page_page_id,
            page_source_type,
            ts,
            if(page_last_page_id is null,ts,null) session_start_point
        from ods.ods_log_inc
        where  page_during_time is not null
    )log
        left join
    (
        select
            id province_id,
            area_code
        from ods.ods_base_province_full
    )bp
    on log.area_code=bp.area_code;
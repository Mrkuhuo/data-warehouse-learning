-- 流量域启动事务事实表
INSERT INTO dwd.dwd_traffic_start_inc(id, k1, province_id, brand, channel, is_new, model, mid_id, operate_system, user_id, version_code, entry, open_ad_id, date_id, start_time, loading_time_ms, open_ad_ms, open_ad_skip_ms)
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
    start_entry,
    start_open_ad_id,
    date_format(FROM_UNIXTIME(ts,'GMT+8'),'yyyy-MM-dd') date_id,
    date_format(FROM_UNIXTIME(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') action_time,
    start_loading_time,
    start_open_ad_ms,
    start_open_ad_skip_ms
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
            start_entry,
            start_loading_time,
            start_open_ad_id,
            start_open_ad_ms,
            start_open_ad_skip_ms,
            ts
        from ods.ods_log_inc
        where start_entry is not null
    )log
        left join
    (
        select
            id province_id,
            area_code
        from ods.ods_base_province_full
    )bp
    on log.area_code=bp.area_code;
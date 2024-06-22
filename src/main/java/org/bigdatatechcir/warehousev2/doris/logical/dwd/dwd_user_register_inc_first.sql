-- 用户域用户注册事务事实表
INSERT INTO dwd.dwd_user_register_inc(k1, user_id, date_id, create_time, channel, province_id, version_code, mid_id, brand, model, operate_system)
select
    k1,
    ui.user_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time,
    channel,
    province_id,
    version_code,
    mid_id,
    brand,
    model,
    operate_system
from
    (
        select
            id user_id,
            k1,
            create_time
        from ods.ods_user_info_inc
    )ui
        left join
    (
        select
            common_ar area_code,
            common_ba brand,
            common_ch channel,
            common_md model,
            common_mid mid_id,
            common_os operate_system,
            common_uid user_id,
            common_vc version_code
        from ods.ods_log_inc
        where page_page_id='register'
          and common_uid is not null
    )log
    on ui.user_id=log.user_id
        left join
    (
        select
            id province_id,
            area_code
        from ods.ods_base_province_full
    )bp
    on log.area_code=bp.area_code;
-- 用户域用户登录事务事实表
INSERT INTO dwd.dwd_user_login_inc(k1, user_id, date_id, login_time, channel, province_id, version_code, mid_id, brand, model, operate_system)
select
    k1,
    user_id,
    date_format(FROM_UNIXTIME(ts,'GMT+8'),'yyyy-MM-dd') date_id,
    date_format(FROM_UNIXTIME(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') login_time,
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
            user_id,
            k1,
            channel,
            area_code,
            version_code,
            mid_id,
            brand,
            model,
            operate_system,
            ts
        from
            (
                select
                    user_id,
                    k1,
                    channel,
                    area_code,
                    version_code,
                    mid_id,
                    brand,
                    model,
                    operate_system,
                    ts,
                    row_number() over (partition by session_id order by ts) rn
                from
                    (
                        select
                            user_id,
                            k1,
                            channel,
                            area_code,
                            version_code,
                            mid_id,
                            brand,
                            model,
                            operate_system,
                            ts,
                            concat(mid_id,'-',last_value(session_start_point) over(partition by mid_id order by ts)) session_id
                        from
                            (
                                select
                                    common_uid user_id,
                                    k1,
                                    common_ch channel,
                                    common_ar area_code,
                                    common_vc version_code,
                                    common_mid mid_id,
                                    common_ba brand,
                                    common_md model,
                                    common_os operate_system,
                                    ts,
                                    if(page_last_page_id is null,ts,null) session_start_point
                                from ods.ods_log_inc
                                where  page_last_page_id is not null
                            )t1
                    )t2
                where user_id is not null
            )t3
        where rn=1
    )t4
        left join
    (
        select
            id province_id,
            area_code
        from ods.ods_base_province_full
    )bp
    on t4.area_code=bp.area_code;
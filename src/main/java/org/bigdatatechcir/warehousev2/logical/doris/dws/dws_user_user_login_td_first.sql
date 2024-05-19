INSERT INTO dws.dws_user_user_login_td(user_id, k1, login_date_last, login_count_td)
select
    u.id,
    CURRENT_DATE(),
    nvl(login_date_last,date_format(create_time,'yyyy-MM-dd')),
    nvl(login_count_td,1)
from
    (
        select
            id,
            create_time
        from dim.dim_user_zip
    )u
        left join
    (
        select
            user_id,
            max(k1) login_date_last,
            count(*) login_count_td
        from dwd.dwd_user_login_inc
        group by user_id
    )l
    on u.id=l.user_id;
INSERT INTO ads.ads_user_stats(dt, recent_days, new_user_count, active_user_count)
select * from ads.ads_user_stats
union
select
    '2020-06-14' dt,
    t1.recent_days,
    new_user_count,
    active_user_count
from
    (
        select
            recent_days,
            sum(if(login_date_last>=date_add('2020-06-14',-recent_days+1),1,0)) new_user_count
        from dws.dws_user_user_login_td lateral view explode(array(1,7,30)) tmp as recent_days
        group by recent_days
    )t1
        join
    (
        select
            recent_days,
            sum(if(date_id>=date_add('2020-06-14',-recent_days+1),1,0)) active_user_count
        from dwd.dwd_user_register_inc lateral view explode(array(1,7,30)) tmp as recent_days
        group by recent_days
    )t2
    on t1.recent_days=t2.recent_days;
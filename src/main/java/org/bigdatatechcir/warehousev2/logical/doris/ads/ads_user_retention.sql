INSERT INTO ads.ads_user_retention(dt, create_date, retention_day, retention_count, new_user_count, retention_rate)
select * from ads.ads_user_retention
union
select
    '2020-06-14' dt,
    login_date_first create_date,
    datediff('2020-06-14',login_date_first) retention_day,
    sum(if(login_date_last='2020-06-14',1,0)) retention_count,
    count(*) new_user_count,
    cast(sum(if(login_date_last='2020-06-14',1,0))/count(*)*100 as decimal(16,2)) retention_rate
from
    (
        select
            user_id,
            date_id login_date_first
        from dwd.dwd_user_register_inc
    )t1
        join
    (
        select
            user_id,
            login_date_last
        from dws.dws_user_user_login_td
    )t2
    on t1.user_id=t2.user_id
group by login_date_first;
INSERT INTO ads.ads_user_change(dt, user_churn_count, user_back_count)
select * from ads.ads_user_change
union
select
    churn.dt,
    user_churn_count,
    user_back_count
from
    (
        select
            '2020-06-14' dt,
            count(*) user_churn_count
        from dws.dws_user_user_login_td
        where login_date_last=date_add('2020-06-14',-7)
    )churn
        join
    (
        select
            '2020-06-14' dt,
            count(*) user_back_count
        from
            (
                select
                    user_id,
                    login_date_last
                from dws.dws_user_user_login_td
            )t1
                join
            (
                select
                    user_id,
                    login_date_last login_date_previous
                from dws.dws_user_user_login_td
            )t2
            on t1.user_id=t2.user_id
        where datediff(login_date_last,login_date_previous)>=8
    )back
    on churn.dt=back.dt;
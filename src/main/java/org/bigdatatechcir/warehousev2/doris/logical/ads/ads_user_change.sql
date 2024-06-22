-- 用户变动统计
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
            date('${pdate}') dt,
    count(*) user_churn_count
    from dws.dws_user_user_login_td
where k1 = date('${pdate}')
  and login_date_last=date_add(date('${pdate}'),-7)
    )churn
    join
    (
select
    date('${pdate}') dt,
    count(*) user_back_count
from
    (
    select
    user_id,
    login_date_last
    from dws.dws_user_user_login_td
    where k1 = date('${pdate}')
    )t1
    join
    (
    select
    user_id,
    login_date_last login_date_previous
    from dws.dws_user_user_login_td
    where k1 = date_add(date('${pdate}'),-1)
    )t2
on t1.user_id=t2.user_id
where datediff(login_date_last,login_date_previous)>=8
    )back
on churn.dt=back.dt;
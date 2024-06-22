-- 用户域用户粒度登录历史至今汇总表
INSERT INTO dws.dws_user_user_login_td(user_id, k1, login_date_last, login_count_td)
select
    nvl(old.user_id,new.user_id),
    k1,
    if(new.user_id is null,old.login_date_last,'2020-06-15'),
    nvl(old.login_count_td,0)+nvl(new.login_count_1d,0)
from
    (
        select
            user_id,
            k1,
            login_date_last,
            login_count_td
        from dws.dws_user_user_login_td
        where k1=date('${pdate}')
    )old
        full outer join
    (
        select
            user_id,
            count(*) login_count_1d
        from dwd.dwd_user_login_inc
        where k1=date('${pdate}')
        group by user_id
    )new
on old.user_id=new.user_id;
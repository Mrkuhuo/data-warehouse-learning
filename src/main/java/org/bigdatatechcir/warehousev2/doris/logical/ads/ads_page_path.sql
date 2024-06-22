-- 路径分析
INSERT INTO ads.ads_page_path(dt, recent_days, source, target, path_count)
select * from ads.ads_page_path
union
select
    date('${pdate}') dt,
    recent_days,
    source,
    nvl(target,'null'),
    count(*) path_count
from
    (
    select
    recent_days,
    concat('step-',rn,':',page_id) source,
    concat('step-',rn+1,':',next_page_id) target
    from
    (
    select
    recent_days,
    page_id,
    lead(page_id,1,null) over(partition by session_id,recent_days) next_page_id,
    row_number() over (partition by session_id,recent_days order by view_time) rn
    from dwd.dwd_traffic_page_view_inc lateral view explode(array(1,7,30)) tmp as recent_days
    where k1>=date_add(date('${pdate}'),-recent_days+1)
    )t1
    )t2
group by recent_days,source,target;
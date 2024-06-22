-- 各渠道流量统计各渠道流量统计
INSERT INTO ads.ads_traffic_stats_by_channel(dt, recent_days, channel, uv_count, avg_duration_sec, avg_page_count, sv_count, bounce_rate)
select * from ads.ads_traffic_stats_by_channel
union
select
    date('${pdate}') dt,
    recent_days,
    channel,
    cast(count(distinct(mid_id)) as bigint) uv_count,
    cast(avg(during_time_1d)/1000 as bigint) avg_duration_sec,
    cast(avg(page_count_1d) as bigint) avg_page_count,
    cast(count(*) as bigint) sv_count,
    cast(sum(if(page_count_1d=1,1,0))/count(*) as decimal(16,2)) bounce_rate
from dws.dws_traffic_session_page_view_1d lateral view explode(array(1,7,30)) tmp as recent_days
where k1>=date_add(date('${pdate}'),-recent_days+1)
group by recent_days,channel;
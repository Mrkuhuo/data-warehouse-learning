-- =================================================================
-- 表名: ads_traffic_stats_by_channel
-- 说明: 各渠道流量统计报表ETL，分析不同渠道的流量质量和用户行为
-- 数据来源: dws.dws_traffic_session_page_view_1d
-- 计算粒度: 渠道和时间周期(1/7/30天)
-- 业务应用: 渠道效果评估、流量分析、营销投放决策、用户行为分析
-- 更新策略: 每日全量刷新
-- 字段说明:
--   dt: 统计日期
--   recent_days: 最近天数(1/7/30)
--   channel: 流量渠道
--   uv_count: 访客数(去重)
--   avg_duration_sec: 平均会话时长(秒)
--   avg_page_count: 平均页面浏览数
--   sv_count: 会话数
--   bounce_rate: 跳出率(只访问一个页面的会话占比)
-- =================================================================

-- 各渠道流量统计
INSERT INTO ads.ads_traffic_stats_by_channel(dt, recent_days, channel, uv_count, avg_duration_sec, avg_page_count, sv_count, bounce_rate)
select * from ads.ads_traffic_stats_by_channel
union
select
    date('${pdate}') dt,                       -- 统计日期
    recent_days,                               -- 统计周期(1/7/30天)
    channel,                                   -- 流量渠道
    cast(count(distinct(mid_id)) as bigint) uv_count,  -- 访客数(去重)
    cast(avg(during_time_1d)/1000 as bigint) avg_duration_sec,  -- 平均会话时长(秒)
    cast(avg(page_count_1d) as bigint) avg_page_count,  -- 平均页面浏览数
    cast(count(*) as bigint) sv_count,         -- 会话数
    cast(sum(if(page_count_1d=1,1,0))/count(*) as decimal(16,2)) bounce_rate  -- 跳出率
from dws.dws_traffic_session_page_view_1d lateral view explode(array(1,7,30)) tmp as recent_days  -- 展开成1天、7天、30天三行
where k1>=date_add(date('${pdate}'),-recent_days+1)  -- 按统计周期筛选
group by recent_days,channel;                  -- 按时间周期和渠道分组汇总
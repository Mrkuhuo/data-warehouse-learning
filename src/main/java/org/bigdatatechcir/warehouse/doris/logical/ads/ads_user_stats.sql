-- =================================================================
-- 表名: ads_user_stats
-- 说明: 用户新增活跃统计报表ETL，统计不同时间周期的新增用户和活跃用户数量
-- 数据来源: dws.dws_user_user_login_td, dwd.dwd_user_register_inc
-- 计算粒度: 时间周期(1/7/30天)
-- 业务应用: 用户增长分析、活跃度评估、产品健康度监控
-- 更新策略: 每日全量刷新
-- 字段说明:
--   dt: 统计日期
--   recent_days: 最近天数(1/7/30)
--   new_user_count: 新增用户数
--   active_user_count: 活跃用户数
-- =================================================================

-- 用户新增活跃统计
INSERT INTO ads.ads_user_stats(dt, recent_days, new_user_count, active_user_count)
select * from ads.ads_user_stats
union
select
    date('${pdate}') dt,                      -- 统计日期
    t1.recent_days,                           -- 统计周期(1/7/30天)
    new_user_count,                           -- 新增用户数
    active_user_count                         -- 活跃用户数
from
    (
    -- 新增用户统计: 计算最近1/7/30天内注册的用户数量
    select
    recent_days,
    sum(if(login_date_last>=date_add(date('${pdate}'),-recent_days+1),1,0)) new_user_count  -- 最近N天注册的用户数
    from dws.dws_user_user_login_td lateral view explode(array(1,7,30)) tmp as recent_days  -- 展开成1天、7天、30天三行
    WHERE k1 = date('${pdate}')               -- 取当天分区数据
    group by recent_days                      -- 按时间周期分组
    )t1
    join
    (
    -- 活跃用户统计: 计算最近1/7/30天内活跃的用户数量
    select
    recent_days,
    sum(if(date_id>=date_add(date('${pdate}'),-recent_days+1),1,0)) active_user_count  -- 最近N天活跃的用户数
    from dwd.dwd_user_register_inc lateral view explode(array(1,7,30)) tmp as recent_days  -- 展开成1天、7天、30天三行
    group by recent_days                      -- 按时间周期分组
    )t2
on t1.recent_days=t2.recent_days;             -- 按时间周期关联
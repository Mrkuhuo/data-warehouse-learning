-- =================================================================
-- 表名: ads_user_retention
-- 说明: 用户留存率分析报表ETL，计算不同注册日期的用户留存情况
-- 数据来源: dwd.dwd_user_register_inc, dws.dws_user_user_login_td
-- 计算粒度: 注册日期和留存天数
-- 业务应用: 用户留存分析、产品质量评估、用户生命周期管理
-- 更新策略: 每日全量刷新
-- 字段说明:
--   dt: 统计日期
--   create_date: 用户注册日期
--   retention_day: 留存天数(注册日期到今日的间隔)
--   retention_count: 留存用户数(当天活跃的该批次注册用户数)
--   new_user_count: 新增用户数(该批次注册的总用户数)
--   retention_rate: 留存率(留存用户数/新增用户数*100%)
-- =================================================================

-- 用户留存率
INSERT INTO ads.ads_user_retention(dt, create_date, retention_day, retention_count, new_user_count, retention_rate)
select * from ads.ads_user_retention
union
select
    date('${pdate}') dt,                      -- 统计日期
    login_date_first create_date,             -- 用户注册日期
    datediff(date('${pdate}'),login_date_first) retention_day,  -- 留存天数
    sum(if(login_date_last=date('${pdate}'),1,0)) retention_count,  -- 留存用户数
    count(*) new_user_count,                  -- 新增用户数
    cast(sum(if(login_date_last=date('${pdate}'),1,0))/count(*)*100 as decimal(16,2)) retention_rate  -- 留存率
from
    (
    -- 获取最近7天内注册的用户
    select
    user_id,
    date_id login_date_first                  -- 注册日期
    from dwd.dwd_user_register_inc            -- 使用DWD层的用户注册事实表
    where k1>=date_add(date('${pdate}'),-7)   -- 最近7天内注册的用户
    and k1 < date('${pdate}')                 -- 排除今日注册的用户

    )t1
    join
    (
    -- 获取用户的最后登录日期
    select
    user_id,
    login_date_last                           -- 最后登录日期
    from dws.dws_user_user_login_td           -- 使用DWS层的用户登录历史表
    where k1 = date('${pdate}')               -- 取当天分区数据
    )t2
on t1.user_id=t2.user_id                      -- 关联用户ID
group by login_date_first;                    -- 按注册日期分组
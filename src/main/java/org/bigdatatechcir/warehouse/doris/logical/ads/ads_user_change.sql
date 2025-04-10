-- =================================================================
-- 表名: ads_user_change
-- 说明: 用户变动统计报表ETL，分析用户流失和回流情况
-- 数据来源: dws.dws_user_user_login_td
-- 计算粒度: 日期
-- 业务应用: 用户生命周期管理、用户流失预警、用户召回策略制定
-- 更新策略: 每日全量刷新
-- 字段说明:
--   dt: 统计日期
--   user_churn_count: 流失用户数(最后登录日期是7天前的用户数)
--   user_back_count: 回流用户数(今日登录且登录间隔>=8天的用户数)
-- =================================================================

-- 用户变动统计
INSERT INTO ads.ads_user_change(dt, user_churn_count, user_back_count)
select * from ads.ads_user_change
union
select
    churn.dt,                                -- 统计日期
    user_churn_count,                        -- 流失用户数
    user_back_count                          -- 回流用户数
from
    (
    -- 计算流失用户数: 最后登录日期正好是7天前的用户数
    select
        date('${pdate}') dt,                 -- 统计日期
    count(*) user_churn_count                -- 流失用户数
    from dws.dws_user_user_login_td          -- 使用DWS层的用户登录历史表
where k1 = date('${pdate}')                  -- 取当天分区数据
  and login_date_last=date_add(date('${pdate}'),-7)  -- 最后登录日期正好是7天前
    )churn
    join
    (
    -- 计算回流用户数: 今日登录且登录间隔>=8天的用户数
select
    date('${pdate}') dt,                     -- 统计日期
    count(*) user_back_count                 -- 回流用户数
from
    (
    -- 获取今日的用户最后登录日期
    select
    user_id,
    login_date_last                          -- 最后登录日期
    from dws.dws_user_user_login_td          -- 使用DWS层的用户登录历史表
    where k1 = date('${pdate}')              -- 取当天分区数据
    )t1
    join
    (
    -- 获取昨日的用户最后登录日期
    select
    user_id,
    login_date_last login_date_previous      -- 前一次最后登录日期
    from dws.dws_user_user_login_td          -- 使用DWS层的用户登录历史表
    where k1 = date_add(date('${pdate}'),-1) -- 取前一天分区数据
    )t2
on t1.user_id=t2.user_id                     -- 关联用户ID
where datediff(login_date_last,login_date_previous)>=8  -- 今日登录且登录间隔>=8天
    )back
on churn.dt=back.dt;                          -- 按日期关联流失和回流数据
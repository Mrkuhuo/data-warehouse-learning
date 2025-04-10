-- =================================================================
-- 表名: ads_page_path
-- 说明: 页面路径分析报表ETL，分析用户在站内的浏览流转路径
-- 数据来源: dwd.dwd_traffic_page_view_inc
-- 计算粒度: 页面路径对(source->target)
-- 业务应用: 页面优化设计、用户体验改进、转化率优化、导航结构设计
-- 更新策略: 每日全量刷新
-- 字段说明:
--   dt: 统计日期
--   recent_days: 最近天数(1/7/30)
--   source: 来源页面(格式：step-序号:页面ID)
--   target: 目标页面(格式：step-序号:页面ID)
--   path_count: 路径访问次数
-- =================================================================

-- 路径分析
INSERT INTO ads.ads_page_path(dt, recent_days, source, target, path_count)
select * from ads.ads_page_path
union
select
    date('${pdate}') dt,              -- 统计日期
    recent_days,                      -- 统计周期(1/7/30天)
    source,                           -- 来源页面
    nvl(target,'null'),               -- 目标页面(如果为空则用'null'代替)
    count(*) path_count               -- 路径次数，统计两个页面相连的次数
from
    (
    -- 构造路径对：为每个会话的页面访问添加步骤序号，并形成source->target对
    select
    recent_days,
    concat('step-',rn,':',page_id) source,                    -- 构造来源页面标识
    concat('step-',rn+1,':',next_page_id) target              -- 构造目标页面标识
    from
    (
    -- 获取页面访问顺序
    select
    recent_days,
    page_id,
    lead(page_id,1,null) over(partition by mid_id, date(view_time), recent_days order by view_time) next_page_id,  -- 获取下一个访问的页面ID
    row_number() over (partition by mid_id, date(view_time), recent_days order by view_time) rn                   -- 会话内的访问序号
    from dwd.dwd_traffic_page_view_inc lateral view explode(array(1,7,30)) tmp as recent_days        -- 展开成1天、7天、30天三行
    where k1>=date_add(date('${pdate}'),-recent_days+1)                                             -- 按最近N天筛选数据
    )t1
    )t2
group by recent_days,source,target;  -- 按时间周期和页面路径分组汇总
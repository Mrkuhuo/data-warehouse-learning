-- =================================================================
-- 表名: ads_activity_stats
-- 说明: 活动统计报表ETL，分析各活动的补贴效果
-- 数据来源: dws.dws_trade_activity_order_nd
-- 计算粒度: 活动ID
-- 业务应用: 活动效果评估、营销策略优化、促销活动ROI分析
-- 更新策略: 每日全量刷新
-- 字段说明:
--   dt: 统计日期
--   activity_id: 活动ID
--   activity_name: 活动名称
--   start_date: 活动开始日期
--   reduce_rate: 活动补贴率（活动优惠金额/原始金额）
-- =================================================================

-- 最近30天发布的活动的补贴率
INSERT INTO ads.ads_activity_stats(dt, activity_id, activity_name, start_date, reduce_rate)
select * from ads.ads_activity_stats
union
select
    date('${pdate}') dt,                        -- 统计日期
    activity_id,                                -- 活动ID
    activity_name,                              -- 活动名称
    start_date,                                 -- 活动开始日期
    -- 计算补贴率: 活动优惠金额/原始金额，转换为小数形式
    cast(activity_reduce_amount_30d/original_amount_30d as decimal(16,2))
from dws.dws_trade_activity_order_nd           -- 使用DWS层的活动订单宽表
where k1=date('${pdate}');                     -- 取当前分区数据
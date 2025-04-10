-- =================================================================
-- 表名: ads_repeat_purchase_by_tm
-- 说明: 品牌复购率分析报表ETL，统计各品牌的复购率指标
-- 数据来源: dws.dws_trade_user_sku_order_nd
-- 计算粒度: 品牌(商标)
-- 业务应用: 品牌忠诚度分析、产品质量监控、复购激励策略制定
-- 更新策略: 每日全量刷新
-- 字段说明:
--   dt: 统计日期
--   recent_days: 最近天数(7/30)
--   tm_id: 品牌(商标)ID
--   tm_name: 品牌(商标)名称
--   order_repeat_rate: 复购率(重复购买人数/购买人数)
-- =================================================================

-- 最近7/30日各品牌复购率
INSERT INTO ads.ads_repeat_purchase_by_tm(dt, recent_days, tm_id, tm_name, order_repeat_rate)
select * from ads.ads_repeat_purchase_by_tm
union
select
    date('${pdate}') dt,                  -- 统计日期
    recent_days,                          -- 统计周期(7/30天)
    tm_id,                                -- 品牌ID
    tm_name,                              -- 品牌名称
    -- 计算复购率: 重复购买人数(>=2次)/总购买人数(>=1次)
    cast(sum(if(order_count>=2,1,0))/sum(if(order_count>=1,1,0)) as decimal(16,2))
from
    (
    -- 按用户和品牌统计购买次数
    select
    date('${pdate}') dt,                  -- 统计日期
    recent_days,                          -- 统计周期
    user_id,                              -- 用户ID
    tm_id,                                -- 品牌ID
    tm_name,                              -- 品牌名称
    sum(CAST(order_count as int)) order_count  -- 用户对该品牌的购买次数
    from
    (
    -- 从DWS层获取用户对各品牌商品的购买记录
    select
    recent_days,
    user_id,
    tm_id,
    tm_name,
    case recent_days
    when 7 then order_count_7d            -- 7天购买次数
    when 30 then order_count_30d          -- 30天购买次数
    end order_count
    from dws.dws_trade_user_sku_order_nd lateral view explode(array(7,30)) tmp as recent_days  -- 展开成7天、30天两行
    where k1 = date('${pdate}')           -- 取当前分区数据
    )t1
    group by recent_days,user_id,tm_id,tm_name  -- 按用户和品牌分组汇总
    )t2
group by recent_days,tm_id,tm_name;       -- 按品牌分组计算复购率
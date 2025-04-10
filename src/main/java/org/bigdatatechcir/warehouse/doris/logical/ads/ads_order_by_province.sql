-- =================================================================
-- 表名: ads_order_by_province
-- 说明: 各省份交易统计报表ETL，统计不同省份的订单量和交易金额
-- 数据来源: dws.dws_trade_province_order_1d, dws.dws_trade_province_order_nd
-- 计算粒度: 省份
-- 业务应用: 区域销售分析、市场拓展决策、区域营销策略制定
-- 更新策略: 每日全量刷新
-- 字段说明:
--   dt: 统计日期
--   recent_days: 最近天数(1/7/30)
--   province_id: 省份ID
--   province_name: 省份名称
--   area_code: 区域编码
--   iso_code: ISO国际编码
--   iso_code_3166_2: ISO 3166-2编码
--   order_count: 订单数
--   order_total_amount: 订单总金额
-- =================================================================

-- 各省份交易统计
INSERT INTO ads.ads_order_by_province(dt, recent_days, province_id, province_name, area_code, iso_code, iso_code_3166_2, order_count, order_total_amount)
select * from ads.ads_order_by_province
union
select
    date('${pdate}') dt,                -- 统计日期
    1 recent_days,                      -- 统计周期为1天
    province_id,                        -- 省份ID
    province_name,                      -- 省份名称
    area_code,                          -- 区域编码
    iso_code,                           -- ISO国际编码
    iso_3166_2,                         -- ISO 3166-2编码
    order_count_1d,                     -- 1天订单数
    order_total_amount_1d               -- 1天订单总金额
from dws.dws_trade_province_order_1d    -- 使用DWS层的省份1日汇总表
where k1 = date('${pdate}')             -- 取当前分区数据
union
select
    date('${pdate}') dt,                -- 统计日期
    recent_days,                        -- 统计周期(7/30天)
    province_id,                        -- 省份ID
    province_name,                      -- 省份名称
    area_code,                          -- 区域编码
    iso_code,                           -- ISO国际编码
    iso_3166_2,                         -- ISO 3166-2编码
    sum(order_count),                   -- 统计周期内的订单数
    sum(order_total_amount)             -- 统计周期内的订单总金额
from
    (
    -- 从DWS层获取7天和30天的省份订单数据，通过case when选择对应的指标
    select
    recent_days,
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    case recent_days
    when 7 then order_count_7d          -- 7天订单数
    when 30 then order_count_30d        -- 30天订单数
    end order_count,
    case recent_days
    when 7 then order_total_amount_7d   -- 7天订单总金额
    when 30 then order_total_amount_30d -- 30天订单总金额
    end order_total_amount
    from dws.dws_trade_province_order_nd lateral view explode(array(7,30)) tmp as recent_days  -- 展开成7天、30天两行
    where k1 = date('${pdate}')         -- 取当前分区数据
    )t1
group by recent_days,province_id,province_name,area_code,iso_code,iso_3166_2;  -- 按时间周期和省份分组汇总
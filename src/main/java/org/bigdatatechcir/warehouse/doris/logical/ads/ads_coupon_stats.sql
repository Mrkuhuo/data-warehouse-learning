-- =================================================================
-- 表名: ads_coupon_stats
-- 说明: 优惠券统计报表ETL，分析各优惠券的使用效果和补贴情况
-- 数据来源: dws.dws_trade_coupon_order_nd
-- 计算粒度: 优惠券ID
-- 业务应用: 优惠券效果评估、促销策略优化、用户营销决策支持
-- 更新策略: 每日全量刷新
-- 字段说明:
--   dt: 统计日期
--   coupon_id: 优惠券ID
--   coupon_name: 优惠券名称
--   start_date: 优惠券开始日期
--   rule_name: 优惠券规则名称
--   reduce_rate: 优惠券补贴率（优惠券优惠金额/原始金额）
-- =================================================================

-- 最近30天发布的优惠券的补贴率
INSERT INTO ads.ads_coupon_stats(dt, coupon_id, coupon_name, start_date, rule_name, reduce_rate)
select * from ads.ads_coupon_stats
union
select
    date('${pdate}') dt,                         -- 统计日期
    coupon_id,                                   -- 优惠券ID
    coupon_name,                                 -- 优惠券名称
    start_date,                                  -- 优惠券开始日期
    coupon_rule,                                 -- 优惠券规则
    -- 计算补贴率: 优惠券优惠金额/原始金额，转换为小数形式
    cast(coupon_reduce_amount_30d/original_amount_30d as decimal(16,2))
from dws.dws_trade_coupon_order_nd              -- 使用DWS层的优惠券订单宽表
where k1=date('${pdate}');                      -- 取当前分区数据
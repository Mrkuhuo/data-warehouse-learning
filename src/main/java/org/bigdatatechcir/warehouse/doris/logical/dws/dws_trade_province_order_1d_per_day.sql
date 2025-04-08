/*
 * 文件名: dws_trade_province_order_1d_per_day.sql
 * 功能描述: 交易域省份粒度订单最近1日汇总表加载脚本
 * 数据粒度: 省份 + 日期
 * 调度周期: 每日执行
 * 调度依赖:
 *   - dwd层订单明细事实表数据已准备完毕
 *   - dim层省份维度表数据已准备完毕
 * 数据来源:
 *   - dwd.dwd_trade_order_detail_inc: 交易域订单明细事实表
 *   - dim.dim_province_full: 省份维度表
 * 目标表: dws.dws_trade_province_order_1d
 * 聚合周期: 最近1日
 * 
 * 说明: 本脚本使用嵌套查询结构加载每日省份订单数据
 */

-- 交易域省份粒度订单最近1日汇总表数据加载
INSERT INTO dws.dws_trade_province_order_1d(
    province_id,       -- 省份ID：唯一标识省份的编码
    k1,                -- 日期分区键：数据的业务日期，用于分区
    province_name,     -- 省份名称：省份的中文名称
    area_code,         -- 地区编码：省份对应的电话区号
    iso_code,          -- 旧版ISO编码：旧版国际标准省份编码
    iso_3166_2,        -- 新版ISO编码：新版国际标准省份编码
    order_count_1d,    -- 订单数：某省份一天内的订单总数
    order_original_amount_1d,   -- 订单原始金额：未优惠前的订单总金额
    activity_reduce_amount_1d,  -- 活动优惠金额：各类营销活动带来的优惠总金额
    coupon_reduce_amount_1d,    -- 优惠券优惠金额：各类优惠券带来的优惠总金额
    order_total_amount_1d       -- 订单最终金额：优惠后实际支付的总金额
)
select
    province_id,       -- 省份ID
    k1,                -- 日期键
    province_name,     -- 省份名称
    area_code,         -- 地区编码
    iso_code,          -- 旧版ISO编码
    iso_3166_2,        -- 新版ISO编码
    order_count_1d,    -- 订单数
    order_original_amount_1d,   -- 订单原始金额
    activity_reduce_amount_1d,  -- 活动优惠金额
    coupon_reduce_amount_1d,    -- 优惠券优惠金额
    order_total_amount_1d       -- 订单最终金额
from
    (
        -- 子查询：按省份和日期聚合订单数据
        select
            province_id,                          -- 省份ID
            k1,                                   -- 日期
            count(distinct(order_id)) order_count_1d,  -- 计算唯一订单数
            sum(split_original_amount) order_original_amount_1d,  -- 原始金额汇总
            sum(nvl(split_activity_amount,0)) activity_reduce_amount_1d,  -- 活动优惠金额汇总(空值处理为0)
            sum(nvl(split_coupon_amount,0)) coupon_reduce_amount_1d,  -- 优惠券优惠金额汇总(空值处理为0)
            sum(split_total_amount) order_total_amount_1d  -- 最终金额汇总
        from dwd.dwd_trade_order_detail_inc  -- 订单明细事实表
        where k1=date('${pdate}')  -- 参数化日期过滤，只处理指定日期数据
        group by province_id,k1  -- 按省份和日期分组聚合
    )o  -- 订单聚合结果别名为o
        left join  -- 左连接，确保包含所有订单数据
    (
        -- 子查询：获取省份维度信息
        select
            id,                 -- 省份ID
            province_name,      -- 省份名称
            area_code,          -- 地区编码
            iso_code,           -- 旧版ISO编码
            iso_3166_2          -- 新版ISO编码
        from dim.dim_province_full  -- 省份维度表
    )p  -- 省份维度结果别名为p
on o.province_id=p.id;  -- 关联条件：订单表的省份ID = 省份维度表的ID

/*
 * 业务分析价值：
 * 1. 地域差异分析：了解不同省份的消费能力和购买偏好
 * 2. 区域营销效果：通过活动和优惠券金额分析各地区营销策略效果
 * 3. 销售预测基础：提供按地区分析的历史数据，支持销售预测
 * 4. 区域市场洞察：识别高潜力地区，指导差异化营销策略
 *
 * 查询优化说明：
 * 1. 采用两层嵌套查询结构，先聚合订单数据，再关联省份维度
 * 2. 使用LEFT JOIN确保所有订单数据都被保留
 * 3. 对可能为NULL的金额字段使用nvl()函数处理，确保计算准确性
 * 4. 使用date('${pdate}')参数化处理日期，支持灵活调度
 */
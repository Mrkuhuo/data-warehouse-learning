-- =================================================================
-- 表名: ads_sku_cart_num_top10_by_cate
-- 说明: 各分类商品购物车存量Top分析报表ETL，统计各品类购物车中数量最多的商品
-- 数据来源: dwd.dwd_trade_cart_full, dim.dim_sku_full
-- 计算粒度: SKU、品类
-- 业务应用: 购物车分析、库存管理、商品推广策略、促销活动设计
-- 更新策略: 每日全量刷新
-- 字段说明:
--   dt: 统计日期
--   category1_id: 一级类目ID
--   category1_name: 一级类目名称
--   category2_id: 二级类目ID
--   category2_name: 二级类目名称
--   category3_id: 三级类目ID
--   category3_name: 三级类目名称
--   sku_id: 商品ID
--   sku_name: 商品名称
--   cart_num: 购物车中商品数量
--   rk: 排名(按类目内购物车数量降序)
-- =================================================================

--  各分类商品购物车存量Top10
INSERT INTO ads.ads_sku_cart_num_top10_by_cate(dt, category1_id, category1_name, category2_id, category2_name, category3_id, category3_name, sku_id, sku_name, cart_num, rk)
select * from ads.ads_sku_cart_num_top10_by_cate
union
select
    date('${pdate}') dt,                -- 统计日期
    category1_id,                       -- 一级类目ID
    category1_name,                     -- 一级类目名称
    category2_id,                       -- 二级类目ID
    category2_name,                     -- 二级类目名称
    category3_id,                       -- 三级类目ID
    category3_name,                     -- 三级类目名称
    sku_id,                             -- 商品ID
    sku_name,                           -- 商品名称
    cart_num,                           -- 购物车中商品数量
    rk                                  -- 排名
from
    (
    -- 对各级类目内的商品按购物车数量进行排名
    select
    sku_id,
    sku_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    cart_num,
    -- 计算在三级类目内的购物车数量排名
    rank() over (partition by category1_id,category2_id,category3_id order by cart_num desc) rk
    from
    (
    -- 计算购物车中各商品的数量
    select
    sku_id,
    sum(sku_num) cart_num               -- 购物车中商品总数量
    from dwd.dwd_trade_cart_full        -- 使用全量购物车事实表
    where k1 = date('${pdate}')         -- 取当天分区数据
    group by sku_id
    )cart
    left join
    (
    -- 获取商品维度信息
    select
    id,
    sku_name,                          -- 商品名称
    category1_id,                      -- 一级类目ID
    category1_name,                    -- 一级类目名称
    category2_id,                      -- 二级类目ID
    category2_name,                    -- 二级类目名称
    category3_id,                      -- 三级类目ID
    category3_name                     -- 三级类目名称
    from dim.dim_sku_full              -- 使用商品维度全量表
    )sku
    on cart.sku_id=sku.id              -- 关联购物车数据和商品维度信息
    )t1
where rk<=3;                           -- 只取排名前3的商品
-- ===============================================================================
-- 各分类商品购物车存量Top10报表(ads_sku_cart_num_top10_by_cate)
-- 功能描述：统计各品类中购物车商品数量排名前10的商品
-- 数据来源：dws_trade_user_sku_order_nd、dws_trade_user_cart_add_nd
-- 刷新策略：每日刷新
-- 应用场景：商品转化率分析、滞留商品分析、品类热度监控
-- ===============================================================================

-- DROP TABLE IF EXISTS ads.ads_sku_cart_num_top3_by_cate;
CREATE  TABLE ads.ads_sku_cart_num_top10_by_cate
(
    `dt`          VARCHAR(255) COMMENT '统计日期',
    `category1_id`   STRING COMMENT '一级分类ID',
    `category1_name` STRING COMMENT '一级分类名称',
    `category2_id`   STRING COMMENT '二级分类ID',
    `category2_name` STRING COMMENT '二级分类名称',
    `category3_id`   STRING COMMENT '三级分类ID',
    `category3_name` STRING COMMENT '三级分类名称',
    `sku_id`         STRING COMMENT '商品id',
    `sku_name`       STRING COMMENT '商品名称',
    `cart_num`       BIGINT COMMENT '购物车中商品数量',
    `rk`             BIGINT COMMENT '排名'
)
    ENGINE=OLAP
DUPLICATE KEY(`dt`)
COMMENT '各分类商品购物车存量Top10'
DISTRIBUTED BY HASH(`dt`)
PROPERTIES
(
    "replication_allocation" = "tag.location.default: 1",
    "is_being_synced" = "false",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false"
);

/*
 * 表设计说明：
 *
 * 1. 表用途与价值：
 *    本表是购物车行为分析的核心报表，统计各品类中被加入购物车最多的商品TOP10排名，
 *    帮助业务团队发现各品类的热门商品和购物车滞留商品，优化库存和营销策略。
 *
 * 2. 数据组成与维度：
 *    - 时间维度：统计日期，反映当天购物车的存量情况
 *    - 品类维度：包含三级品类结构(一级/二级/三级分类)，支持多级品类分析
 *    - 商品维度：记录商品的ID和名称，明确具体商品信息
 *    - 排名维度：记录商品在该品类中的购物车数量排名
 *
 * 3. 关键指标解读：
 *    - 购物车中商品数量：反映商品受关注程度，但需结合转化率评估
 *    - 品类内排名：反映商品在同类中的相对热度，排名越靠前表示该商品在品类中越受欢迎
 *    - 加购未购买比例：可结合订单数据计算，评估商品从加购到购买的转化效率
 *
 * 4. 典型应用场景：
 *    - 品类热门商品识别：发现各品类中最受用户关注的商品，优先保证库存和推广
 *    - 购物车滞留分析：分析长期滞留在购物车但未转化的商品，找出原因并优化
 *    - 促销策略制定：针对购物车中积累较多但转化率低的商品，设计定向促销策略
 *    - 品类热度监控：通过品类商品加购情况，评估各品类的用户关注度变化
 *    - 商品推荐优化：根据购物车热门商品，优化首页和详情页的商品推荐策略
 *
 * 5. 查询示例：
 *    - 查询昨日各一级品类购物车TOP3商品：
 *      SELECT category1_name, sku_name, cart_num, rk
 *      FROM ads.ads_sku_cart_num_top10_by_cate
 *      WHERE dt = '${yesterday}' AND rk <= 3
 *      ORDER BY category1_id, rk;
 *    
 *    - 查询特定品类的购物车热门商品：
 *      SELECT sku_name, cart_num
 *      FROM ads.ads_sku_cart_num_top10_by_cate
 *      WHERE dt = '${yesterday}' 
 *      AND category1_id = '${category1_id}'
 *      ORDER BY rk;
 *
 * 6. 建议扩展：
 *    - 添加转化指标：增加下单数、转化率等指标，便于直接评估商品从加购到购买的转化效率
 *    - 添加趋势指标：增加环比变化指标，监控商品热度趋势
 *    - 添加时间维度：记录平均加购停留时间，分析用户购买决策时间
 *    - 添加价格维度：增加商品价格信息，分析价格对加购行为的影响
 *    - 添加来源维度：记录加购来源页面，优化用户购买路径
 */
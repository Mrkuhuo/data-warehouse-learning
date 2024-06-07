-- mall.tmp_warehouse_product definition

CREATE TABLE `tmp_warehouse_product` (
  `wp_id` int unsigned NOT NULL AUTO_INCREMENT COMMENT '商品库存ID',
  `product_id` int unsigned NOT NULL COMMENT '商品ID',
  `w_id` smallint unsigned NOT NULL COMMENT '仓库ID',
  `current_cnt` int unsigned NOT NULL DEFAULT '0' COMMENT '当前商品数量',
  `lock_cnt` int unsigned NOT NULL DEFAULT '0' COMMENT '当前占用数据',
  `in_transit_cnt` int unsigned NOT NULL DEFAULT '0' COMMENT '在途数据',
  `average_cost` decimal(8,2) NOT NULL DEFAULT '0.00' COMMENT '移动加权成本',
  `modified_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后修改时间',
  PRIMARY KEY (`wp_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='商品库存表';
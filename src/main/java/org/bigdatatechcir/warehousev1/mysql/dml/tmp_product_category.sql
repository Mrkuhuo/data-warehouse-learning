-- mall.tmp_product_category definition

CREATE TABLE `tmp_product_category` (
  `category_id` smallint unsigned NOT NULL AUTO_INCREMENT COMMENT '分类ID',
  `category_name` varchar(10) NOT NULL COMMENT '分类名称',
  `category_code` varchar(10) NOT NULL COMMENT '分类编码',
  `parent_id` smallint unsigned NOT NULL DEFAULT '0' COMMENT '父分类ID',
  `category_level` tinyint NOT NULL DEFAULT '1' COMMENT '分类层级',
  `category_status` tinyint NOT NULL DEFAULT '1' COMMENT '分类状态',
  `modified_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后修改时间',
  PRIMARY KEY (`category_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='商品分类表';
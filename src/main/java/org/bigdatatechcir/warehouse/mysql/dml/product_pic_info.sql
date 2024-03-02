-- mall.product_pic_info definition

CREATE TABLE `product_pic_info` (
  `product_pic_id` int unsigned NOT NULL AUTO_INCREMENT COMMENT '商品图片ID',
  `product_id` int unsigned NOT NULL COMMENT '商品ID',
  `pic_desc` varchar(50) DEFAULT NULL COMMENT '图片描述',
  `pic_url` varchar(200) NOT NULL COMMENT '图片URL',
  `is_master` tinyint NOT NULL DEFAULT '0' COMMENT '是否主图：0.非主图1.主图',
  `pic_order` tinyint NOT NULL DEFAULT '0' COMMENT '图片排序',
  `pic_status` tinyint NOT NULL DEFAULT '1' COMMENT '图片是否有效：0无效 1有效',
  `modified_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后修改时间',
  PRIMARY KEY (`product_pic_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='商品图片信息表';
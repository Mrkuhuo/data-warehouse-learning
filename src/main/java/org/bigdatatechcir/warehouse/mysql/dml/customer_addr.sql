-- mall.customer_addr definition

CREATE TABLE `customer_addr` (
  `customer_addr_id` int unsigned NOT NULL AUTO_INCREMENT COMMENT '自增主键ID',
  `customer_id` int unsigned NOT NULL COMMENT 'customer_login表的自增ID',
  `zip` smallint NOT NULL COMMENT '邮编',
  `province` smallint NOT NULL COMMENT '地区表中省份的ID',
  `city` smallint NOT NULL COMMENT '地区表中城市的ID',
  `district` smallint NOT NULL COMMENT '地区表中的区ID',
  `address` varchar(200) NOT NULL COMMENT '具体的地址门牌号',
  `is_default` tinyint NOT NULL COMMENT '是否默认',
  `modified_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后修改时间',
  PRIMARY KEY (`customer_addr_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='用户地址表';
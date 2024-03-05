-- mall.customer_level_inf definition

CREATE TABLE mall.`customer_level_inf` (
  `customer_level_id` tinyint NOT NULL AUTO_INCREMENT COMMENT '会员级别ID',
  `customer_level` tinyint NOT NULL COMMENT '会员级别',
  `level_name` varchar(10) NOT NULL COMMENT '会员级别名称',
  `min_point` int unsigned NOT NULL DEFAULT '0' COMMENT '该级别最低积分',
  `max_point` int unsigned NOT NULL DEFAULT '0' COMMENT '该级别最高积分',
  `modified_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后修改时间',
  PRIMARY KEY (`customer_level_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='用户级别信息表';
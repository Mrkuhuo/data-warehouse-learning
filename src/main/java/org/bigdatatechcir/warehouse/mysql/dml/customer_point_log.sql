-- mall.customer_point_log definition

CREATE TABLE `customer_point_log` (
  `point_id` int unsigned NOT NULL AUTO_INCREMENT COMMENT '积分日志ID',
  `customer_id` int unsigned NOT NULL COMMENT '用户ID',
  `source` tinyint unsigned NOT NULL COMMENT '积分来源：0订单，1登陆，2活动',
  `refer_number` int unsigned NOT NULL DEFAULT '0' COMMENT '积分来源相关编号',
  `change_point` smallint NOT NULL DEFAULT '0' COMMENT '变更积分数',
  `create_time` timestamp NOT NULL COMMENT '积分日志生成时间',
  PRIMARY KEY (`point_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='用户积分日志表';
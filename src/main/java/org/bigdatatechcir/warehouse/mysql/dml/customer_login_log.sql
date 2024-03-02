-- mall.customer_login_log definition

CREATE TABLE `customer_login_log` (
  `login_id` int unsigned NOT NULL AUTO_INCREMENT COMMENT '登陆日志ID',
  `customer_id` int unsigned NOT NULL COMMENT '登陆用户ID',
  `login_time` timestamp NOT NULL COMMENT '用户登陆时间',
  `login_ip` int unsigned NOT NULL COMMENT '登陆IP',
  `login_type` tinyint NOT NULL COMMENT '登陆类型：0未成功，1成功',
  PRIMARY KEY (`login_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='用户登陆日志表';
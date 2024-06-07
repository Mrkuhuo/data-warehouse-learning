-- mall.tmp_customer_balance_log definition

CREATE TABLE `tmp_customer_balance_log` (
  `balance_id` int unsigned NOT NULL AUTO_INCREMENT COMMENT '余额日志ID',
  `customer_id` int unsigned NOT NULL COMMENT '用户ID',
  `source` tinyint unsigned NOT NULL DEFAULT '1' COMMENT '记录来源：1订单，2退货单',
  `source_sn` int unsigned NOT NULL COMMENT '相关单据ID',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录生成时间',
  `amount` decimal(8,2) NOT NULL DEFAULT '0.00' COMMENT '变动金额',
  PRIMARY KEY (`balance_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='用户余额变动表';
-- mall.tmp_product_comment definition

CREATE TABLE `tmp_product_comment` (
  `comment_id` int unsigned NOT NULL AUTO_INCREMENT COMMENT '评论ID',
  `product_id` int unsigned NOT NULL COMMENT '商品ID',
  `order_id` bigint unsigned NOT NULL COMMENT '订单ID',
  `customer_id` int unsigned NOT NULL COMMENT '用户ID',
  `title` varchar(50) NOT NULL COMMENT '评论标题',
  `content` varchar(300) NOT NULL COMMENT '评论内容',
  `audit_status` tinyint NOT NULL COMMENT '审核状态：0未审核，1已审核',
  `audit_time` timestamp NOT NULL COMMENT '评论时间',
  `modified_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后修改时间',
  PRIMARY KEY (`comment_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='商品评论表';
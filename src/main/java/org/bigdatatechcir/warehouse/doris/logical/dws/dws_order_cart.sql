-- 统计用户每天加入购物车的商品数

INSERT INTO  dws.dws_order_cart (
  customer_id,
  modified_day,
  cart_count
)
select customer_id,
	date_format(modified_time , 'yyyy-MM-dd') as modified_day,
	COUNT(cart_id) as cart_count
from dwd.dwd_order_cart WHERE modified_time >= DATE('${modified_time}')
GROUP BY
	customer_id,
	modified_day;
-- 统计每个用户每天的订单总额

INSERT  INTO dws.dws_order_master (
  customer_id,
  modified_day,
  order_sum
)
select customer_id,
	date_format(modified_time , 'yyyy-MM-dd') as modified_day,
	SUM(order_money) as order_sum
from dwd.dwd_order_master WHERE modified_time >= DATE('${modified_time}')
GROUP BY
	customer_id,
	modified_day;
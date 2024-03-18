-- 统计用户每日新增地址总数

insert into dws.dws_customer_addr(customer_id, modified_day, address_count)
SELECT
	customer_id,
	date_format(modified_time , 'yyyy-MM-dd') as modified_day,
	COUNT(customer_id) as address_count
FROM
	dwd.dwd_customer_addr WHERE modified_time >= DATE('${modified_time}')
GROUP BY
	customer_id,
	modified_day ;
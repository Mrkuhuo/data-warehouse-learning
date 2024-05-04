-- 统计用户每天登录次数

INSERT INTO dws.dws_customer_login_log (
      customer_id,
      login_day,
      login_count
)
select customer_id,
	date_format(login_time , 'yyyy-MM-dd') as login_day,
	COUNT(customer_id) as login_count
FROM dwd.dwd_customer_login_log WHERE login_time >= DATE('${modified_time}')
GROUP BY
	customer_id,
	login_day;
INSERT INTO dws.dws_customer_login( customer_id, login_name, password, user_stats, modified_day )
SELECT
	customer_id,
	login_name,
	password,
	user_stats,
	date_format(modified_time , 'yyyy-MM-dd') as modified_day
FROM
	dwd.dwd_customer_login
	WHERE modified_time >= DATE('${modified_time}');
INSERT INTO
	ads.ads_customer ( customer_id, customer_name, address_count, login_count, cart_count, order_sum, modified_day )
SELECT
	login.customer_id,
	inf.customer_name,
	addr.address_count,
	log.login_count,
	cart.cart_count,
	master.order_sum,
	login.modified_day
FROM
	dws.dws_customer_login login
LEFT JOIN dws.dws_customer_addr addr ON
	login.customer_id = addr.customer_id
LEFT JOIN dws.dws_customer_login_log log ON
	login.customer_id = log.customer_id
LEFT JOIN dws.dws_customer_inf inf ON
	login.customer_id = inf.customer_id
LEFT JOIN dws.dws_order_cart cart ON
	login.customer_id = cart.customer_id
LEFT JOIN dws.dws_order_master master ON
	login.customer_id = master.customer_id
	AND login.modified_day >= '${modified_time}'
	AND addr.modified_day >= '${modified_time}'
	AND log.login_day >= '${modified_time}'
	AND inf.modified_day >= '${modified_time}'
	AND cart.modified_day >= '${modified_time}'
	AND master.modified_day >= '${modified_time}'
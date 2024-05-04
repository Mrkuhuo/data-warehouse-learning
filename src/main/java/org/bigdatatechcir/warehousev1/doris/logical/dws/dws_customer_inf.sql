-- 因为是明细数据直接抽取上来即可

INSERT INTO dws.dws_customer_inf (
	  customer_inf_id,
	  customer_id,
	  customer_name,
	  identity_card_type,
	  identity_card_no,
	  mobile_phone,
	  customer_email,
	  gender,
	  user_point,
	  register_time,
	  birthday,
	  customer_level,
	  user_money,
	  modified_day
  )
SELECT
 	  customer_inf_id,
	  customer_id,
	  customer_name,
	  identity_card_type,
	  identity_card_no,
	  mobile_phone,
	  customer_email,
	  gender,
	  user_point,
	  register_time,
	  birthday,
	  customer_level,
	  user_money,
	  date_format(modified_time , 'yyyy-MM-dd') as modified_day
FROM dwd.dwd_customer_inf
     WHERE modified_time >= DATE('${modified_time}');
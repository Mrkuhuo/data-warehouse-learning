INSERT INTO dwd.dwd_order_master (
    order_id,
    order_sn,
    customer_id,
    shipping_user,
    province,
    city,
    district,
    address,
    payment_method,
    order_money,
    district_money,
    shipping_money,
    payment_money,
    shipping_comp_name,
    shipping_sn,
    create_time,
    shipping_time,
    pay_time,
    receive_time,
    order_status,
    order_point,
    invoice_time,
    modified_time
)
SELECT
    order_id,
    order_sn,
    customer_id,
    shipping_user,
    province,
    city,
    district,
    address,
    payment_method,
    -- 清洗订单金额，确保它是非负数
    CASE
        WHEN order_money >= 0 THEN order_money
        ELSE NULL
    END AS order_money,
    -- 清洗优惠金额，确保它是非正数（可能是负数表示折扣）
    CASE
        WHEN district_money <= 0 THEN district_money
        ELSE NULL
    END AS district_money,
    -- 清洗运费金额，确保它是非负数
    CASE
        WHEN shipping_money >= 0 THEN shipping_money
        ELSE NULL
    END AS shipping_money,
    -- 清洗支付金额，确保它是非负数
    CASE
        WHEN payment_money >= 0 THEN payment_money
        ELSE NULL
    END AS payment_money,
    shipping_comp_name,
    shipping_sn,
    -- 清洗时间字段，尝试转换为正确的日期时间格式
    CASE
        WHEN create_time REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN STR_TO_DATE(create_time, '%Y-%m-%d %H:%i:%s')
        ELSE NULL
    END AS create_time,
    CASE
        WHEN shipping_time REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN STR_TO_DATE(shipping_time, '%Y-%m-%d %H:%i:%s')
        ELSE NULL
    END AS shipping_time,
    CASE
        WHEN pay_time REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN STR_TO_DATE(pay_time, '%Y-%m-%d %H:%i:%s')
        ELSE NULL
    END AS pay_time,
    CASE
        WHEN receive_time REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN STR_TO_DATE(receive_time, '%Y-%m-%d %H:%i:%s')
        ELSE NULL
    END AS receive_time,
    order_status,
    order_point,
    invoice_time,
  	modified_time
FROM ods.ods_order_master
WHERE modified_time >= DATE('${modified_time}');
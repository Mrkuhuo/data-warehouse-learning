INSERT INTO dwd.dwd_order_cart (
    cart_id,
    customer_id,
    product_id,
    product_amount,
    price,
    add_time,
    modified_time
)
SELECT
    cart_id,
    customer_id,
    product_id,
    -- 清洗product_amount，确保它是非负数
    CASE
        WHEN product_amount >= 0 THEN product_amount
        ELSE NULL
    END AS product_amount,
    -- 清洗price，确保它是非负数并且没有超过DECIMAL(27, 2)的范围
    CASE
        WHEN price >= 0  THEN price
        ELSE NULL
    END AS price,
    -- 清洗add_time，尝试转换为正确的日期时间格式
    CASE
        WHEN add_time REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN STR_TO_DATE(add_time, '%Y-%m-%d %H:%i:%s')
        ELSE NULL
    END AS add_time,
    -- 清洗modified_time，尝试转换为正确的日期时间格式
    CASE
        WHEN modified_time REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN STR_TO_DATE(modified_time, '%Y-%m-%d %H:%i:%s')
        ELSE NULL
    END AS modified_time
FROM ods.ods_order_cart
-- 确保cart_id不为空
WHERE cart_id IS NOT NULL
AND modified_time >= DATE('${modified_time}');

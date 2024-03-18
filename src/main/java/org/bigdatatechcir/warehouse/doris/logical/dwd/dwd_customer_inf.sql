INSERT INTO dwd.dwd_customer_inf (
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
    modified_time
)
SELECT
    customer_inf_id,
    customer_id,
    -- 清洗customer_name，去除前后的空格
    TRIM(customer_name) AS customer_name,
    -- 确保identity_card_type是一个有效的证件类型
    CASE
        WHEN identity_card_type IN (1, 2, 3) THEN identity_card_type
        ELSE 1 -- 默认设置为身份证，根据实际情况可以调整
    END AS identity_card_type,
    -- 清洗identity_card_no，去除空格和非法字符
    TRIM(REPLACE(identity_card_no, ' ', '')) AS identity_card_no,
    -- 假设mobile_phone字段存储的是纯数字，如果不是，则设为NULL
    CASE
        WHEN mobile_phone REGEXP '^[0-9]+$' THEN mobile_phone
        ELSE NULL
    END AS mobile_phone,
    -- 清洗customer_email，去除前后的空格，并检查是否为有效的邮箱格式
    CASE
        WHEN customer_email LIKE '%_@_%.%' AND LOCATE('.', customer_email, LOCATE('@', customer_email)) > LOCATE('@', customer_email) THEN TRIM(customer_email)
        ELSE NULL
    END AS customer_email,
    -- 清洗gender，确保是有效的性别标识，这里假设有效的性别为'M', 'F', 'Other'
    CASE
        WHEN gender IN ('M', 'F', 'Other') THEN gender
        ELSE NULL
    END AS gender,
    user_point,
    -- 确保register_time和modified_time是有效的日期时间格式
    STR_TO_DATE(register_time, '%Y-%m-%d %H:%i:%S') AS register_time,
    STR_TO_DATE(birthday, '%Y-%m-%d') AS birthday, -- 假设birthday只包含日期
    -- 确保customer_level是一个有效的会员级别
    CASE
        WHEN customer_level BETWEEN 1 AND 5 THEN customer_level
        ELSE 1 -- 默认设置为普通会员
    END AS customer_level,
    user_money,
    STR_TO_DATE(modified_time, '%Y-%m-%d %H:%i:%S') AS modified_time
FROM ods.ods_customer_inf
WHERE customer_inf_id IS NOT NULL
AND customer_id IS NOT NULL
AND customer_name != ''
AND identity_card_type IN (1, 2, 3)
AND identity_card_no IS NOT NULL AND identity_card_no != ''
AND (mobile_phone IS NULL OR mobile_phone REGEXP '^[0-9]+$')
AND (customer_email IS NULL OR (customer_email LIKE '%_@_%.%' AND LOCATE('.', customer_email, LOCATE('@', customer_email)) > LOCATE('@', customer_email)))
AND modified_time >= DATE('${modified_time}');
INSERT INTO dwd.dwd_customer_login (
    customer_id,
    login_name,
    password,
    user_stats,
    modified_time
)
SELECT
    customer_id,
    -- 清洗login_name，去除前后的空格
    TRIM(login_name) AS login_name,
    password,
    -- 确保user_stats是一个有效的用户状态标识，如果不是则设为NULL（根据实际需要可以设置为其他默认值）
    CASE
        WHEN user_stats BETWEEN 0 AND 255 THEN user_stats
        ELSE NULL
    END AS user_stats,
    -- 确保modified_time是有效的日期时间格式，如果不是则设为NULL
    CASE
        WHEN STR_TO_DATE(modified_time, '%Y-%m-%d %H:%i:%S') IS NOT NULL THEN STR_TO_DATE(modified_time, '%Y-%m-%d %H:%i:%S')
        ELSE NULL
    END AS modified_time
FROM ods.ods_customer_login
WHERE customer_id IS NOT NULL
AND modified_time >= DATE('${modified_time}');
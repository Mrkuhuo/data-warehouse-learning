INSERT INTO dwd.dwd_customer_addr (
    customer_addr_id,
    customer_id,
    zip,
    province,
    city,
    district,
    address,
    is_default,
    modified_time
)
SELECT
    customer_addr_id,
    customer_id,
    -- 使用正则表达式清洗zip，假设zip只包含数字
    CASE
        WHEN zip REGEXP '^[0-9]+$' THEN zip
        ELSE NULL
    END AS zip,
    -- 清洗province, city, district, address，去除前后的空格
    TRIM(province) AS province,
    TRIM(city) AS city,
    TRIM(district) AS district,
    TRIM(address) AS address,
    -- 确保is_default是一个布尔值
    CASE
        WHEN is_default IN (0, 1) THEN is_default
        ELSE NULL
    END AS is_default,
    -- 日期格式转换，同时检查日期是否有效
    CASE
        WHEN STR_TO_DATE(modified_time, '%Y-%m-%d %H:%i:%S') IS NOT NULL THEN STR_TO_DATE(modified_time, '%Y-%m-%d %H:%i:%S')
        ELSE NULL
    END AS modified_timestamp
FROM ods_customer_addr
WHERE customer_id IS NOT NULL
AND zip IS NOT NULL AND zip REGEXP '^[0-9]+$'
AND province != '' AND TRIM(province) IS NOT NULL
AND city != '' AND TRIM(city) IS NOT NULL
AND district != '' AND TRIM(district) IS NOT NULL
AND address != '' AND TRIM(address) IS NOT NULL
AND is_default IS NOT NULL AND is_default IN (0, 1)
AND modified_time IS NOT NULL AND STR_TO_DATE(modified_time, '%Y-%m-%d %H:%i:%S') IS NOT NULL AND modified_time >= DATE('${modified_time}');
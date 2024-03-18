INSERT INTO dwd.dwd_customer_login_log (
    login_id,
    customer_id,
    login_time,
    login_ip,
    login_type
)
SELECT
    login_id,
    -- 清洗customer_id，这里假设不需要特殊处理
    customer_id,
    -- 清洗login_time，尝试转换为正确的日期时间格式
    CASE
        WHEN login_time REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN STR_TO_DATE(login_time, '%Y-%m-%d %H:%i:%s')
        ELSE NULL
    END AS login_time,
    -- 清洗login_ip，这里假设IP地址是合法的，没有进行进一步的验证
    login_ip,
    -- 清洗login_type，确保它是0或1，否则设为NULL
    CASE
        WHEN login_type IN (0, 1) THEN login_type
        ELSE NULL
    END AS login_type
FROM ods.ods_customer_login_log
-- 确保login_id不为空
WHERE login_id IS NOT NULL;
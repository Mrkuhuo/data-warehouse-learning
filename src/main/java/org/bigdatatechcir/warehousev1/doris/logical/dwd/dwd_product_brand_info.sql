INSERT INTO dwd.dwd_product_brand_info (
    brand_id,
    brand_name,
    telephone,
    brand_web,
    brand_logo,
    brand_desc,
    brand_status,
    brand_order,
    modified_time
)
SELECT
    brand_id,
    brand_name,
    telephone,
    brand_web,
    brand_logo,
    brand_desc,
    -- 清洗品牌状态，确保它是0或1
    CASE
        WHEN brand_status IN (0, 1) THEN brand_status
        ELSE NULL
    END AS brand_status,
    brand_order,
    -- 清洗最后修改时间，可能需要转换为正确的日期时间格式
    -- 这里假设modified_time是字符串，且格式为'YYYY-MM-DD HH:MM:SS'
    CASE
        WHEN modified_time REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN STR_TO_DATE(modified_time, '%Y-%m-%d %H:%i:%s')
        ELSE NULL
    END AS modified_time
FROM
    ods.ods_product_brand_info
    WHERE modified_time >= DATE('${modified_time}');
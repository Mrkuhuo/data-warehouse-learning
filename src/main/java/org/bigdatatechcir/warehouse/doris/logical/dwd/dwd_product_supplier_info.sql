INSERT INTO dwd.dwd_product_supplier_info (
    supplier_id,
    supplier_code,
    supplier_name,
    supplier_type,
    link_man,
    phone_number,
    bank_name,
    bank_account,
    address,
    supplier_status,
    modified_time
)
SELECT
    supplier_id,
    supplier_code,
    supplier_name,
    -- 清洗供应商类型，确保它是1或2
    CASE
        WHEN supplier_type IN (1, 2) THEN supplier_type
        ELSE NULL
    END AS supplier_type,
    link_man,
    phone_number,
    bank_name,
    bank_account,
    address,
    -- 清洗供应商状态，确保它是0或1
    CASE
        WHEN supplier_status IN (0, 1) THEN supplier_status
        ELSE NULL
    END AS supplier_status,
    -- 清洗最后修改时间，可能需要转换为正确的日期时间格式
    -- 这里假设modified_time是字符串，且格式为'YYYY-MM-DD HH:MM:SS'
    CASE
        WHEN modified_time REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN STR_TO_DATE(modified_time, '%Y-%m-%d %H:%i:%s')
        ELSE NULL
    END AS modified_time
FROM
    ods.ods_product_supplier_info
    WHERE modified_time >= DATE('${modified_time}');
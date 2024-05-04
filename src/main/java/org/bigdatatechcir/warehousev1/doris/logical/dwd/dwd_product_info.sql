INSERT INTO dwd.dwd_product_info (
    product_id,
    product_core,
    product_name,
    bar_code,
    brand_id,
    one_category_id,
    two_category_id,
    three_category_id,
    supplier_id,
    price,
    average_cost,
    publish_status,
    audit_status,
    weight,
    length,
    height,
    width,
    color_type,
    production_date,
    shelf_life,
    descript,
    indate,
    modified_time
)
SELECT
    product_id,
    product_core,
    product_name,
    bar_code,
    brand_id,
    one_category_id,
    two_category_id,
    three_category_id,
    supplier_id,
    price,
    average_cost,
    -- 清洗上下架状态，确保它是0或1
    CASE
        WHEN publish_status IN (0, 1) THEN publish_status
        ELSE NULL
    END AS publish_status,
    -- 清洗审核状态，确保它是0或1
    CASE
        WHEN audit_status IN (0, 1) THEN audit_status
        ELSE NULL
    END AS audit_status,
    weight,
    length,
    height,
    width,
    color_type,
    -- 清洗生产日期，可能需要转换为正确的日期格式
    -- 这里假设production_date是字符串，且格式为'YYYY-MM-DD'
    CASE
        WHEN production_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(production_date, '%Y-%m-%d')
        ELSE NULL
    END AS production_date,
    shelf_life,
    descript,
    -- 清洗商品录入时间，可能需要转换为正确的日期时间格式
    -- 这里假设indate是字符串，且格式为'YYYY-MM-DD HH:MM:SS'
    CASE
        WHEN indate REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN STR_TO_DATE(indate, '%Y-%m-%d %H:%i:%s')
        ELSE NULL
    END AS indate,
    -- 清洗最后修改时间，可能需要转换为正确的日期时间格式
    -- 这里假设modified_time是字符串，且格式为'YYYY-MM-DD HH:MM:SS'
    CASE
        WHEN modified_time REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN STR_TO_DATE(modified_time, '%Y-%m-%d %H:%i:%s')
    END AS modified_time
FROM ods.ods_product_info
    WHERE modified_time >= DATE('${modified_time}');
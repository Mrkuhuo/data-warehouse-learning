INSERT INTO dwd.dwd_warehouse_shipping_info (
    ship_id,
    ship_name,
    ship_contact,
    telephone,
    price,
    modified_time
)
SELECT
    ship_id,
    ship_name,
    ship_contact,
    telephone,
    price,
    modified_time
FROM
    ods.ods_warehouse_shipping_info
    WHERE ship_id IS NOT NULL
    AND modified_time >= DATE('${modified_time}');
-- 营销域优惠券全量表
INSERT INTO dwd.dwd_marketing_coupon_info_full(id, k1, coupon_id, coupon_name, coupon_type, condition_amount, benefit_amount, start_time, end_time, create_time, range_type, limit_num, range_ids, range_names)
with
    cou_info as
    (
        select
            id,
            k1,
            coupon_name,
            coupon_type,
            condition_amount,
            benefit_amount,
            create_time,
            range_type,
            limit_num,
            start_time,
            end_time,
            operate_time,
            expire_time
        from ods.ods_coupon_info_full
        where k1=date('${pdate}')
    ),
    cou_range as
    (
        select
            coupon_id,
            GROUP_CONCAT(
                case 
                    when range_type = '1' then concat('商品：', COALESCE(range_id, ''))
                    when range_type = '2' then concat('品类：', COALESCE(range_id, ''))
                    when range_type = '3' then concat('品牌：', COALESCE(range_id, ''))
                    else ''
                end,
                ';'
            ) as range_desc,
            GROUP_CONCAT(COALESCE(range_id, ''), ';') as range_ids,
            GROUP_CONCAT(
                case 
                    when range_type = '1' then COALESCE(range_id, '')  -- 商品ID
                    when range_type = '2' then COALESCE(range_id, '')  -- 品类ID
                    when range_type = '3' then COALESCE(range_id, '')  -- 品牌ID
                    else ''
                end,
                ';'
            ) as range_names  -- 实际上这里存储的是ID，后续可能需要连接其他表获取实际名称
        from ods.ods_coupon_range_full
        group by coupon_id
    )
select
    ci.id,
    ci.k1,
    ci.id as coupon_id,
    ci.coupon_name,
    ci.coupon_type,
    ci.condition_amount,
    ci.benefit_amount,
    ci.start_time,
    ci.end_time,
    ci.create_time,
    ci.range_type,
    ci.limit_num,
    cr.range_ids,
    cr.range_names
from cou_info ci
    left join cou_range cr on ci.id = cr.coupon_id; 
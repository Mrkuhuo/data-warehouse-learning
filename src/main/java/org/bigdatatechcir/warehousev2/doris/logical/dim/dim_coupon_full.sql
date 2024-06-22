-- 优惠券维度表
insert into dim.dim_coupon_full(id, k1, coupon_name, coupon_type_code, coupon_type_name, condition_amount, condition_num, activity_id, benefit_amount, benefit_discount, benefit_rule, create_time, range_type_code, range_type_name, limit_num, taken_count, start_time, end_time, operate_time, expire_time)
select
    id,
    k1,
    coupon_name,
    coupon_type,
    coupon_dic.dic_name,
    condition_amount,
    condition_num,
    activity_id,
    benefit_amount,
    benefit_discount,
    case coupon_type
        when '3201' then concat('满',condition_amount,'元减',benefit_amount,'元')
        when '3202' then concat('满',condition_num,'件打',10*(1-benefit_discount),'折')
        when '3203' then concat('减',benefit_amount,'元')
        end benefit_rule,
    create_time,
    range_type,
    range_dic.dic_name,
    limit_num,
    taken_count,
    start_time,
    end_time,
    operate_time,
    expire_time
from
    (
        select
            id,
            k1,
            coupon_name,
            coupon_type,
            condition_amount,
            condition_num,
            activity_id,
            benefit_amount,
            benefit_discount,
            create_time,
            range_type,
            limit_num,
            taken_count,
            start_time,
            end_time,
            operate_time,
            expire_time
        from ods.ods_coupon_info_full
        where k1=date('${pdate}')
    )ci
        left join
    (
        select
            dic_code,
            dic_name
        from ods.ods_base_dic_full
        where k1=date('${pdate}')
        and parent_code='32'
    )coupon_dic
on ci.coupon_type=coupon_dic.dic_code
    left join
    (
    select
    dic_code,
    dic_name
    from ods.ods_base_dic_full
    where k1=date('${pdate}')
    and parent_code='33'
    )range_dic
    on ci.range_type=range_dic.dic_code;
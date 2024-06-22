-- 活动维度表
insert into dim.dim_activity_full(activity_rule_id, activity_id, k1, activity_name, activity_type_code, activity_type_name, activity_desc, start_time, end_time, create_time, condition_amount, condition_num, benefit_amount, benefit_discount, benefit_rule, benefit_level)
select
    rule.id,
    info.id,
    current_date() as k1,
    activity_name,
    rule.activity_type,
    dic.dic_name,
    activity_desc,
    start_time,
    end_time,
    create_time,
    condition_amount,
    condition_num,
    benefit_amount,
    benefit_discount,
    case rule.activity_type
        when '3101' then concat('满',condition_amount,'元减',benefit_amount,'元')
        when '3102' then concat('满',condition_num,'件打',10*(1-benefit_discount),'折')
        when '3103' then concat('打',10*(1-benefit_discount),'折')
        end benefit_rule,
    benefit_level
from
    (
        select
            id,
            activity_id,
            activity_type,
            condition_amount,
            condition_num,
            benefit_amount,
            benefit_discount,
            benefit_level
        from ods.ods_activity_rule_full
    )rule
        left join
    (
        select
            id,
            activity_name,
            activity_type,
            activity_desc,
            start_time,
            end_time,
            create_time
        from ods.ods_activity_info_full
        where k1=date('${pdate}')
    )info
on rule.activity_id=info.id
    left join
    (
    select
    dic_code,
    dic_name
    from ods.ods_base_dic_full
    where  parent_code='31'
    and k1=date('${pdate}')
    )dic
    on rule.activity_type=dic.dic_code;
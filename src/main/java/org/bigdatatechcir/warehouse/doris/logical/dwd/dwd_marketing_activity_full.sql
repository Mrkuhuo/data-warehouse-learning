-- 营销域活动全量表
INSERT INTO dwd.dwd_marketing_activity_full(id, k1, activity_id, activity_name, activity_type, start_time, end_time, create_time, activity_desc, rules, sku_ids, status)
with
    act_info as
    (
        select
            id,
            k1,
            activity_name,
            activity_type,
            start_time,
            end_time,
            create_time,
            activity_desc
        from ods.ods_activity_info_full
        where k1=date('${pdate}')
    ),
    act_rule as
    (
        select
            activity_id,
            GROUP_CONCAT(CONCAT(condition_amount, ':', benefit_amount, ':', benefit_discount), ';') as rules
        from ods.ods_activity_rule_full
        group by activity_id
    ),
    act_sku as
    (
        select
            activity_id,
            GROUP_CONCAT(sku_id, ',') as sku_ids
        from ods.ods_activity_sku_full
        where k1=date('${pdate}')
        group by activity_id
    )
select
    ai.id,
    ai.k1,
    ai.id as activity_id,
    ai.activity_name,
    ai.activity_type,
    ai.start_time,
    ai.end_time,
    ai.create_time,
    ai.activity_desc,
    ar.rules,
    sku.sku_ids,
    -- 根据时间动态设置状态
    case 
        when current_timestamp() < ai.start_time then '未开始'
        when current_timestamp() > ai.end_time then '已结束'
        else '进行中'
    end as status
from act_info ai
    left join act_rule ar on ai.id = ar.activity_id
    left join act_sku sku on ai.id = sku.activity_id; 
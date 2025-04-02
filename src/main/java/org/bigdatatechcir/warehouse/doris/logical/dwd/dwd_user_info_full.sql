-- 用户域用户基础信息全量表
INSERT INTO dwd.dwd_user_info_full(id, k1, user_id, login_name, nick_name, name, phone_num, email, gender, birthday, user_level, create_time, operate_time, status)
select
    id,
    k1,
    id as user_id,
    login_name,
    nick_name,
    name,
    phone_num,
    email,
    gender,
    birthday,
    user_level,
    create_time,
    operate_time,
    status
from ods.ods_user_info_full
where k1=date('${pdate}'); 
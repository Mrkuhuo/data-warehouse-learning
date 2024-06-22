-- 用户维度表
insert into dim.dim_user_zip(id, k1, login_name, nick_name, name, phone_num, email, user_level, birthday, gender, create_time, operate_time, start_date, end_date)
select
    id,
    k1,
    login_name,
    nick_name,
    md5(name),
    md5(phone_num),
    md5(email),
    user_level,
    birthday,
    gender,
    create_time,
    operate_time,
    '2020-06-14' start_date,
    '9999-12-31' end_date
from ods.ods_user_info_inc
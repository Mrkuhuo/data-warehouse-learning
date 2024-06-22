-- 用户维度表
insert into dim.dim_user_zip(id, k1, login_name, nick_name, name, phone_num, email, user_level, birthday, gender, create_time, operate_time, start_date, end_date)
with
    tmp as
        (
            select
                old.id old_id,
                old.k1,
                old.login_name old_login_name,
                old.nick_name old_nick_name,
                old.name old_name,
                old.phone_num old_phone_num,
                old.email old_email,
                old.user_level old_user_level,
                old.birthday old_birthday,
                old.gender old_gender,
                old.create_time old_create_time,
                old.operate_time old_operate_time,
                old.start_date old_start_date,
                old.end_date old_end_date,
                new.id new_id,
                new.k2,
                new.login_name new_login_name,
                new.nick_name new_nick_name,
                new.name new_name,
                new.phone_num new_phone_num,
                new.email new_email,
                new.user_level new_user_level,
                new.birthday new_birthday,
                new.gender new_gender,
                new.create_time new_create_time,
                new.operate_time new_operate_time,
                new.start_date new_start_date,
                new.end_date new_end_date
            from
                (
                    select
                        id,
                        k1,
                        login_name,
                        nick_name,
                        name,
                        phone_num,
                        email,
                        user_level,
                        birthday,
                        gender,
                        create_time,
                        operate_time,
                        start_date,
                        end_date
                    from dim.dim_user_zip
                    where end_date = '9999-12-31'
                )old
                    full outer join
                (
                    select
                        id,
                        k1 as k2,
                        login_name,
                        nick_name,
                        md5(name) name,
                        md5(phone_num) phone_num,
                        md5(email) email,
                        user_level,
                        birthday,
                        gender,
                        create_time,
                        operate_time,
                        '2024-06-15' start_date,
                        '9999-12-31' end_date
                    from
                        (
                            select
                                id,
                                k1,
                                login_name,
                                nick_name,
                                name,
                                phone_num,
                                email,
                                user_level,
                                birthday,
                                gender,
                                create_time,
                                operate_time,
                                row_number() over (partition by id order by create_time desc) rn
                            from ods.ods_user_info_inc
                            -- where k1=date('${pdate}')
                        )t1
                    where rn=1
                )new
                on old.id=new.id
        )
select
    if(new_id is not null,new_id,old_id),
    k2,
    if(new_id is not null,new_login_name,old_login_name),
    if(new_id is not null,new_nick_name,old_nick_name),
    if(new_id is not null,new_name,old_name),
    if(new_id is not null,new_phone_num,old_phone_num),
    if(new_id is not null,new_email,old_email),
    if(new_id is not null,new_user_level,old_user_level),
    if(new_id is not null,new_birthday,old_birthday),
    if(new_id is not null,new_gender,old_gender),
    if(new_id is not null,new_create_time,old_create_time),
    if(new_id is not null,new_operate_time,old_operate_time),
    if(new_id is not null,new_start_date,old_start_date),
    if(new_id is not null,new_end_date,old_end_date)
from tmp
where k2 is not NULL
union all
select
    old_id,
    k1,
    old_login_name,
    old_nick_name,
    old_name,
    old_phone_num,
    old_email,
    old_user_level,
    old_birthday,
    old_gender,
    old_create_time,
    old_operate_time,
    old_start_date,
    cast(date_add(date('${pdate}'),-1) as string) old_end_date

from tmp
where k1 is not NULL
  and  old_id is not null
  and new_id is not null;
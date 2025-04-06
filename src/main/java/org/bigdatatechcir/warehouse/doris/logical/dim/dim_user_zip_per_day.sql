-- ===============================================================================
-- 用户维度拉链表ETL逻辑(dim_user_zip_per_day)
-- 功能描述：使用拉链表技术处理用户属性变化，保留历史版本，支持有效版本和历史追溯
-- 数据来源：ods_user_info_full(增量表)、dim_user_zip(当前拉链表)
-- 调度策略：每日增量更新
-- 特殊说明：采用SCD Type 2设计模式，使用start_date和end_date标记版本生效期
-- ===============================================================================

-- 用户维度拉链表数据更新
insert into dim.dim_user_zip(id, k1, login_name, nick_name, name, phone_num, email, user_level, birthday, gender, create_time, operate_time, start_date, end_date)
    with
        -- 临时表：合并当前拉链表数据和新增数据，进行新旧数据比对
        tmp as
            (
                select
                    -- 当前拉链表最新有效数据
                    old.id old_id,                    -- 原用户ID
                    old.k1,                           -- 原分区日期
                    old.login_name old_login_name,    -- 原登录名
                    old.nick_name old_nick_name,      -- 原昵称
                    old.name old_name,                -- 原姓名
                    old.phone_num old_phone_num,      -- 原手机号
                    old.email old_email,              -- 原邮箱
                    old.user_level old_user_level,    -- 原用户等级
                    old.birthday old_birthday,        -- 原生日
                    old.gender old_gender,            -- 原性别
                    old.create_time old_create_time,  -- 原创建时间
                    old.operate_time old_operate_time, -- 原操作时间
                    old.start_date old_start_date,    -- 原版本开始日期
                    old.end_date old_end_date,        -- 原版本结束日期

                    -- 新增数据(当日变化)
                    new.id new_id,                    -- 新用户ID
                    new.k2,                           -- 新分区日期
                    new.login_name new_login_name,    -- 新登录名
                    new.nick_name new_nick_name,      -- 新昵称
                    new.name new_name,                -- 新姓名(加密)
                    new.phone_num new_phone_num,      -- 新手机号(加密)
                    new.email new_email,              -- 新邮箱(加密)
                    new.user_level new_user_level,    -- 新用户等级
                    new.birthday new_birthday,        -- 新生日
                    new.gender new_gender,            -- 新性别
                    new.create_time new_create_time,  -- 新创建时间
                    new.operate_time new_operate_time, -- 新操作时间
                    new.start_date new_start_date,    -- 新版本开始日期
                    new.end_date new_end_date         -- 新版本结束日期
                from
                    -- 查询当前拉链表中最新有效数据(end_date = '9999-12-31'表示当前有效版本)
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
                        where end_date = '9999-12-31'  -- 筛选当前有效版本
                    ) old

        -- 使用FULL OUTER JOIN合并新旧数据，处理新增用户和变更用户两种情况
        full outer join

            -- 当日增量数据处理(从增量表中提取最新状态)
            (
                select
                    cast(t1.id as VARCHAR(64)) as id,  -- 转换用户ID为VARCHAR类型并命名为id，确保能被new.id引用
                    t1.k1 as k2,                      -- 重命名为k2避免与old.k1冲突
                    t1.login_name,                    -- 保持原字段名
                    t1.nick_name,                     -- 保持原字段名
                    md5(t1.name) as name,            -- 对敏感信息进行加密并命名为name
                    md5(t1.phone_num) as phone_num,  -- 对敏感信息进行加密并命名为phone_num
                    md5(t1.email) as email,          -- 对敏感信息进行加密并命名为email
                    t1.user_level,                    -- 保持原字段名
                    t1.birthday,                      -- 保持原字段名
                    t1.gender,                        -- 保持原字段名
                    t1.create_time,                   -- 保持原字段名
                    t1.operate_time,                  -- 保持原字段名
                    '2024-06-15' as start_date,       -- 新版本开始日期(此处应为当前日期，硬编码仅用于演示)
                    '9999-12-31' as end_date          -- 新版本结束日期(9999-12-31表示当前有效版本)
                from
                    -- 对增量表数据按用户ID分组，取最新一条记录
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
                            -- 按用户ID分组，按创建时间倒序排序，取每个用户最新的记录
                            row_number() over (partition by id order by create_time desc) rn
                        from ods.ods_user_info_full
                        -- where k1=date('${pdate}')  -- 在实际执行时应取当日分区
                    ) t1
                where rn=1  -- 只取每个用户的最新记录
            ) new
on old.id=new.id  -- 按用户ID关联新旧数据
    )

-- 查询1：处理有变更的用户数据，生成新版本记录
select
    -- 如果有新记录则使用新值，否则使用旧值
    if(new_id is not null, new_id, old_id),         -- 用户ID
    k2,                                             -- 分区日期
    if(new_id is not null, new_login_name, old_login_name), -- 登录名
    if(new_id is not null, new_nick_name, old_nick_name),   -- 昵称
    if(new_id is not null, new_name, old_name),             -- 姓名
    if(new_id is not null, new_phone_num, old_phone_num),   -- 手机号
    if(new_id is not null, new_email, old_email),           -- 邮箱
    if(new_id is not null, new_user_level, old_user_level), -- 用户等级
    if(new_id is not null, new_birthday, old_birthday),     -- 生日
    if(new_id is not null, new_gender, old_gender),         -- 性别
    if(new_id is not null, new_create_time, old_create_time), -- 创建时间
    if(new_id is not null, new_operate_time, old_operate_time), -- 操作时间
    if(new_id is not null, new_start_date, old_start_date), -- 版本开始日期
    if(new_id is not null, new_end_date, old_end_date)      -- 版本结束日期
from tmp
where k2 is not NULL  -- 只处理有新分区数据的记录

-- 合并查询结果
union all

-- 查询2：处理原记录的历史版本化，更新结束日期
select
    old_id,                       -- 用户ID
    k1,                           -- 分区日期
    old_login_name,               -- 登录名
    old_nick_name,                -- 昵称
    old_name,                     -- 姓名
    old_phone_num,                -- 手机号
    old_email,                    -- 邮箱
    old_user_level,               -- 用户等级
    old_birthday,                 -- 生日
    old_gender,                   -- 性别
    old_create_time,              -- 创建时间
    old_operate_time,             -- 操作时间
    old_start_date,               -- 版本开始日期
    cast(date_add(date('${pdate}'), -1) as string) old_end_date  -- 更新版本结束日期为当前日期前一天
from tmp
where k1 is not NULL    -- 有原分区数据
  and old_id is not null  -- 有原用户ID
  and new_id is not null; -- 同时有新用户ID，说明是变更记录
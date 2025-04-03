/*
 * 文件名: dwd_user_login_inc.sql
 * 功能描述: 用户域用户登录事务事实表 - 提取用户的登录会话数据
 * 数据粒度: 登录会话
 * 刷新策略: 增量刷新
 * 调度周期: 每日
 * 依赖表: 
 *   - ods.ods_log_full: 用户行为日志数据
 *   - ods.ods_base_province_full: 省份维度数据
 * 目标表: dwd.dwd_user_login_inc
 * 主要功能: 
 *   1. 从用户行为日志中识别用户登录会话
 *   2. 提取会话开始时间和设备信息
 *   3. 关联省份信息丰富地域维度
 *   4. 标准化时间格式和渠道信息
 */

-- 用户域用户登录事务事实表
INSERT INTO dwd.dwd_user_login_inc(
    k1,             -- 数据日期分区
    user_id,        -- 用户ID
    date_id,        -- 日期ID，格式yyyy-MM-dd
    login_time,     -- 登录时间，格式yyyy-MM-dd HH:mm:ss
    channel,        -- 渠道来源
    province_id,    -- 省份ID
    version_code,   -- 应用版本号
    mid_id,         -- 设备ID
    brand,          -- 设备品牌
    model,          -- 设备型号
    operate_system  -- 操作系统
)
select
    k1,                                                               -- 数据分区日期
    user_id,                                                          -- 用户ID
    date_format(FROM_UNIXTIME(ts,'GMT+8'),'yyyy-MM-dd') date_id,      -- 转换时间戳为日期ID
    date_format(FROM_UNIXTIME(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') login_time, -- 转换时间戳为登录时间
    channel,                                                          -- 渠道
    province_id,                                                      -- 省份ID
    version_code,                                                     -- 应用版本号
    mid_id,                                                           -- 设备ID
    brand,                                                            -- 设备品牌
    model,                                                            -- 设备型号
    operate_system                                                    -- 操作系统
from
    (
        -- 第四层子查询：提取最终需要的字段，准备与省份维度关联
        select
            user_id,         -- 用户ID
            k1,              -- 分区日期
            channel,         -- 渠道
            area_code,       -- 地区编码，用于关联省份信息
            version_code,    -- 版本号
            mid_id,          -- 设备ID
            brand,           -- 品牌
            model,           -- 型号
            operate_system,  -- 操作系统
            ts               -- 时间戳
        from
            (
                -- 第三层子查询：按会话分组，只保留每个会话的第一条记录(登录记录)
                select
                    user_id,
                    k1,
                    channel,
                    area_code,
                    version_code,
                    mid_id,
                    brand,
                    model,
                    operate_system,
                    ts,
                    row_number() over (partition by session_id order by ts) rn -- 为每个会话内的记录按时间排序编号
                from
                    (
                        -- 第二层子查询：识别会话，通过设备ID和会话开始点确定唯一会话
                        select
                            user_id,
                            k1,
                            channel,
                            area_code,
                            version_code,
                            mid_id,
                            brand,
                            model,
                            operate_system,
                            ts,
                            concat(mid_id,'-',last_value(session_start_point) over(partition by mid_id order by ts)) session_id -- 生成会话ID
                        from
                            (
                                -- 第一层子查询：从日志表提取基础字段，标记会话开始点
                                select
                                    common_uid user_id,               -- 用户ID
                                    k1,                               -- 分区日期
                                    common_ch channel,                -- 渠道
                                    common_ar area_code,              -- 地区编码
                                    common_vc version_code,           -- 版本号
                                    common_mid mid_id,                -- 设备ID
                                    common_ba brand,                  -- 品牌
                                    common_md model,                  -- 型号
                                    common_os operate_system,         -- 操作系统
                                    ts,                               -- 时间戳
                                    if(page_last_page_id is null,ts,null) session_start_point -- 如果last_page_id为空，表示是会话的第一个页面(登录页)
                                from ods.ods_log_full
                                where page_last_page_id is not null   -- 只选择有页面浏览记录的日志
                            ) t1
                    ) t2
                where user_id is not null -- 只关注已登录用户(有user_id)的记录
            ) t3
        where rn=1 -- 只保留每个会话的第一条记录，即登录记录
    ) t4
    left join
    (
        -- 省份维度子查询：用于关联地区信息
        select
            id province_id,    -- 省份ID
            area_code          -- 地区编码
        from ods.ods_base_province_full
    ) bp
    on t4.area_code=bp.area_code; -- 通过地区编码关联省份信息

/*
 * 设计说明:
 * 1. 会话识别逻辑:
 *    - 使用page_last_page_id为空作为会话开始标志
 *    - 通过设备ID(mid_id)和会话开始时间点确定唯一会话
 *    - 使用session_id将同一会话的行为分组
 *    
 * 2. 窗口函数应用:
 *    - last_value()函数获取同一设备最近的会话开始点
 *    - row_number()函数为每个会话内的记录分配序号
 *    - 这种组合使用有效识别了用户登录行为
 *
 * 3. 增量表特点:
 *    - 每次只处理新增数据，累积历史登录记录
 *    - 适用于事实表和事务型数据
 *
 * 4. 时区处理:
 *    - 使用FROM_UNIXTIME(ts,'GMT+8')显式指定时区为东八区
 *    - 确保时间表示的一致性，避免时区问题
 *
 * 5. 省份关联策略:
 *    - 使用left join确保即使没有匹配的省份信息，也不会丢失登录记录
 *    - 通过area_code关联省份ID，丰富地域维度
 *
 * 6. 数据质量控制:
 *    - 过滤掉user_id为空的记录，确保只分析已登录用户
 *    - 使用多层子查询逐步提炼数据，提高代码可读性和可维护性
 */
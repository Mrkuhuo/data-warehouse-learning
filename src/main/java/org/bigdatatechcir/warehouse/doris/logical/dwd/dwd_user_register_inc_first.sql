/*
 * 文件名: dwd_user_register_inc_first.sql
 * 功能描述: 用户域用户注册事务事实表(首次加载) - 记录历史用户注册行为
 * 数据粒度: 用户注册事件
 * 刷新策略: 首次增量加载
 * 调度周期: 一次性执行
 * 依赖表: 
 *   - ods.ods_user_info_full: 用户基本信息
 *   - ods.ods_log_full: 用户行为日志
 *   - ods.ods_base_province_full: 省份基础数据
 * 目标表: dwd.dwd_user_register_inc
 * 主要功能: 
 *   1. 结合用户基本信息和注册行为日志
 *   2. 提取并关联用户注册时的设备信息
 *   3. 关联省份信息丰富地域维度
 *   4. 为用户注册行为分析提供基础数据
 */

-- 用户域用户注册事务事实表(首次加载)
INSERT INTO dwd.dwd_user_register_inc(
    k1,              -- 数据日期分区
    user_id,         -- 用户ID
    date_id,         -- 日期ID
    create_time,     -- 创建时间
    channel,         -- 注册渠道
    province_id,     -- 省份ID
    version_code,    -- 应用版本号
    mid_id,          -- 设备ID
    brand,           -- 设备品牌
    model,           -- 设备型号
    operate_system   -- 操作系统
)
select
    k1,                                              -- 分区字段
    ui.user_id,                                      -- 用户ID
    date_format(create_time,'yyyy-MM-dd') date_id,   -- 将时间戳转换为日期ID
    create_time,                                     -- 用户创建时间
    channel,                                         -- 注册渠道
    province_id,                                     -- 省份ID(关联自省份表)
    version_code,                                    -- 应用版本号
    mid_id,                                          -- 设备ID
    brand,                                           -- 设备品牌
    model,                                           -- 设备型号
    operate_system                                   -- 操作系统
from
    (
        -- 用户信息子查询：获取用户基本信息
        select
            id user_id,                              -- 用户ID
            k1,                                      -- 分区字段
            create_time                              -- 用户创建时间(注册时间)
        from ods.ods_user_info_full
        -- 注：首次加载不使用k1过滤，加载所有历史数据
    )ui
    left join
    (
        -- 日志数据子查询：获取注册时的设备信息
        select
            common_ar area_code,                     -- 地区编码
            common_ba brand,                         -- 设备品牌
            common_ch channel,                       -- 渠道
            common_md model,                         -- 设备型号
            common_mid mid_id,                       -- 设备ID
            common_os operate_system,                -- 操作系统
            common_uid user_id,                      -- 用户ID
            common_vc version_code                   -- 应用版本号
        from ods.ods_log_full
        where page_page_id='register'                -- 筛选注册页面的日志
          and common_uid is not null                 -- 确保用户ID不为空
    )log
    on ui.user_id=log.user_id                        -- 通过用户ID关联日志信息
    left join
    (
        -- 省份维度子查询：获取省份信息
        select
            id province_id,                          -- 省份ID
            area_code                                -- 地区编码
        from ods.ods_base_province_full
    )bp
    on log.area_code=bp.area_code;                   -- 通过地区编码关联省份信息

/*
 * 设计说明:
 * 1. 首次加载特点:
 *    - 不使用时间过滤条件，加载所有历史注册用户数据
 *    - 为DWD层初始化提供完整的历史用户注册记录
 *    - 后续日常加载应使用dwd_user_register_inc_per_day.sql
 *    
 * 2. 多源数据整合:
 *    - 以用户表为主表，获取用户ID和注册时间
 *    - 关联日志表获取注册时的设备和渠道信息
 *    - 关联省份表丰富地域维度数据
 *
 * 3. 日志数据筛选:
 *    - 使用page_page_id='register'条件筛选注册页面的日志
 *    - 确保common_uid不为空，只处理已关联用户的注册日志
 *    - 这种筛选方式可能导致部分未记录日志的注册用户缺少设备信息
 *
 * 4. 关联策略:
 *    - 使用left join确保所有用户记录都会保留，即使没有对应的日志或省份信息
 *    - 对于缺少日志的用户，设备和地域信息将为NULL
 *
 * 5. 数据应用场景:
 *    - 用户增长分析：评估历史用户增长趋势和模式
 *    - 渠道分析：分析不同渠道的获客效果
 *    - 设备偏好：了解用户使用的设备分布
 *    - 地域分布：分析用户的地理位置分布
 */
/*
 * 文件名: dwd_user_register_inc_per_day.sql
 * 功能描述: 用户域用户注册事务事实表(每日增量) - 记录每日新增用户注册行为
 * 数据粒度: 用户注册事件
 * 刷新策略: 每日增量加载
 * 调度周期: 每日
 * 依赖表: 
 *   - ods.ods_user_info_full: 用户基本信息
 *   - ods.ods_log_full: 用户行为日志
 *   - ods.ods_base_province_full: 省份基础数据
 * 目标表: dwd.dwd_user_register_inc
 * 主要功能: 
 *   1. 提取每日新增注册用户信息
 *   2. 关联注册时的设备和渠道信息
 *   3. 关联地域维度数据
 *   4. 为用户增长分析提供数据支持
 */

-- 用户域用户注册事务事实表(每日增量)
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
        -- 用户信息子查询：获取当日新增用户信息
        select
            id user_id,                              -- 用户ID
            k1,                                      -- 分区字段
            create_time                              -- 用户创建时间(注册时间)
        from ods.ods_user_info_full
        where k1=date('${pdate}')                    -- 按分区日期过滤，只处理当天注册的用户
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
          and k1=date('${pdate}')                    -- 按分区日期过滤，只处理当天的日志
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
 * 1. 每日增量加载策略:
 *    - 使用k1=date('${pdate}')条件过滤用户表和日志表
 *    - 只处理当天新增的注册用户及其行为数据
 *    - 与首次加载脚本(dwd_user_register_inc_first.sql)配合使用
 *    
 * 2. 多表过滤一致性:
 *    - 用户表和日志表都应用相同的时间过滤条件
 *    - 确保数据时间上的一致性，避免关联不匹配
 *
 * 3. 省份维度处理:
 *    - 省份表作为基础维度表不需要时间过滤
 *    - 假设省份数据变化不频繁，使用最新的省份信息即可
 *
 * 4. 数据完整性策略:
 *    - 使用left join确保所有新注册用户都会被保留
 *    - 对于没有捕获到注册日志的用户，设备和地域信息可能为NULL
 *    - 这是设计权衡，保证用户主体信息的完整性
 *
 * 5. 数据应用场景:
 *    - 每日用户增长监控：实时掌握新增用户情况
 *    - 注册渠道效果评估：分析各渠道的获客效果
 *    - 用户属性分析：了解新增用户的设备偏好和地域分布
 *    - 营销策略优化：针对用户特征调整获客策略
 */
/*
 * 文件名: dwd_traffic_start_inc.sql
 * 功能描述: 流量域启动事务事实表 - 记录用户APP启动行为
 * 数据粒度: 应用启动事件
 * 刷新策略: 增量处理
 * 调度周期: 每日
 * 依赖表: 
 *   - ods.ods_log_full: 日志全量数据
 *   - ods.ods_base_province_full: 省份基础数据
 * 目标表: dwd.dwd_traffic_start_inc
 * 主要功能: 
 *   1. 提取应用启动相关的日志数据
 *   2. 捕获启动渠道、开屏广告展示等信息
 *   3. 转换时间戳为可读时间格式
 *   4. 关联地域维度信息
 */

-- 流量域启动事务事实表
INSERT INTO dwd.dwd_traffic_start_inc(
    id,                 -- 日志记录ID
    k1,                 -- 数据日期分区
    province_id,        -- 省份ID
    brand,              -- 设备品牌
    channel,            -- 渠道
    is_new,             -- 是否新用户
    model,              -- 设备型号
    mid_id,             -- 设备ID
    operate_system,     -- 操作系统
    user_id,            -- 用户ID
    version_code,       -- 应用版本号
    entry,              -- 启动入口
    open_ad_id,         -- 开屏广告ID
    date_id,            -- 日期ID
    start_time,         -- 启动时间
    loading_time_ms,    -- 加载时长(毫秒)
    open_ad_ms,         -- 开屏广告时长(毫秒)
    open_ad_skip_ms     -- 开屏广告跳过时长(毫秒)
)
select
    id,                                                           -- 日志ID
    k1,                                                           -- 分区字段
    province_id,                                                  -- 省份ID(关联自省份表)
    brand,                                                        -- 设备品牌
    channel,                                                      -- 渠道
    common_is_new,                                                -- 是否新用户(1-新用户,0-老用户)
    model,                                                        -- 设备型号
    mid_id,                                                       -- 设备唯一标识
    operate_system,                                               -- 操作系统类型
    user_id,                                                      -- 用户ID
    version_code,                                                 -- 应用版本号
    start_entry,                                                  -- 启动入口(icon/notice/install)
    start_open_ad_id,                                             -- 开屏广告ID
    date_format(FROM_UNIXTIME(ts,'GMT+8'),'yyyy-MM-dd') date_id,  -- 将时间戳转换为日期ID
    date_format(FROM_UNIXTIME(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') action_time, -- 转换为完整时间
    start_loading_time,                                           -- 应用加载时长
    start_open_ad_ms,                                             -- 开屏广告展示时长
    start_open_ad_skip_ms                                         -- 开屏广告跳过时长
from
    (
        -- 日志数据子查询：提取应用启动相关数据
        select
            id,                        -- 日志ID
            k1,                        -- 分区字段
            common_ar area_code,       -- 地区编码
            common_ba brand,           -- 设备品牌
            common_ch channel,         -- 渠道
            common_is_new,             -- 是否新用户
            common_md model,           -- 设备型号
            common_mid mid_id,         -- 设备ID
            common_os operate_system,  -- 操作系统
            common_uid user_id,        -- 用户ID
            common_vc version_code,    -- 应用版本号
            start_entry,               -- 启动入口
            start_loading_time,        -- 加载时长
            start_open_ad_id,          -- 开屏广告ID
            start_open_ad_ms,          -- 开屏广告展示时长
            start_open_ad_skip_ms,     -- 开屏广告跳过时长
            ts                         -- 时间戳(秒)
        from ods.ods_log_full
        where start_entry is not null  -- 筛选启动事件日志
    )log
    left join
    (
        -- 省份维度子查询：获取省份信息
        select
            id province_id,            -- 省份ID
            area_code                  -- 地区编码
        from ods.ods_base_province_full
    )bp
    on log.area_code=bp.area_code;     -- 通过地区编码关联省份信息

/*
 * 设计说明:
 * 1. 应用启动事件识别:
 *    - 使用start_entry字段是否为空来识别应用启动事件
 *    - 启动入口可能包括桌面图标点击(icon)、通知栏点击(notice)或应用安装后首次启动(install)
 *    
 * 2. 启动性能指标:
 *    - 捕获start_loading_time记录应用加载性能，可用于优化启动速度
 *    - 开屏广告相关字段(展示时长、跳过时长)可用于分析广告体验
 *
 * 3. 时间处理:
 *    - 将Unix时间戳转换为可读时间格式，使用东八区(GMT+8)
 *    - 生成date_id用于按天分析，生成action_time用于精确时间分析
 *
 * 4. 多维度分析支持:
 *    - 保留设备信息(品牌、型号、操作系统)用于技术兼容性分析
 *    - 包含用户标识和新用户标志，支持用户层面分析
 *    - 关联地域信息便于区域分析
 *
 * 5. 数据应用场景:
 *    - 应用启动频次分析：评估用户活跃度和使用习惯
 *    - 启动性能监控：分析不同设备和版本的启动性能
 *    - 开屏广告效果评估：分析广告展示和用户跳过行为
 *    - 渠道质量分析：评估不同渠道带来用户的启动行为差异
 */
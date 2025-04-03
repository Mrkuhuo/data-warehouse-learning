/*
 * 文件名: dwd_traffic_error_inc.sql
 * 功能描述: 流量域错误事务事实表 - 记录应用错误日志与相关上下文信息
 * 数据粒度: 应用错误事件
 * 刷新策略: 增量加载
 * 调度周期: 每日
 * 依赖表: 
 *   - ods.ods_log_full: 应用日志全量表
 *   - ods.ods_base_province_full: 省份基础信息表
 * 目标表: dwd.dwd_traffic_error_inc
 * 主要功能: 
 *   1. 提取应用错误相关日志数据
 *   2. 记录错误码与错误信息
 *   3. 保存错误发生时的上下文信息(用户、设备、页面等)
 *   4. 为应用稳定性分析和故障排查提供数据支持
 */

-- 流量域错误事务事实表
INSERT INTO dwd.dwd_traffic_error_inc(
    id,               -- 日志记录唯一标识
    k1,               -- 数据日期分区
    province_id,      -- 省份ID
    brand,            -- 设备品牌
    channel,          -- 应用渠道
    is_new,           -- 是否新用户(1-新用户,0-老用户)
    model,            -- 设备型号
    mid_id,           -- 设备唯一标识
    operate_system,   -- 操作系统
    user_id,          -- 用户ID
    version_code,     -- 应用版本号
    page_id,          -- 当前页面ID
    last_page_id,     -- 上一页面ID
    page_item,        -- 页面项目
    page_item_type,   -- 页面项目类型
    during_time,      -- 页面停留时间
    source_type,      -- 页面来源类型
    error_code,       -- 错误代码
    error_msg,        -- 错误信息
    date_id,          -- 日期ID
    error_time        -- 错误发生时间
)
select
    id,                                                            -- 日志ID
    k1,                                                            -- 分区字段
    province_id,                                                   -- 省份ID(关联自省份表)
    brand,                                                         -- 设备品牌
    channel,                                                       -- 访问渠道
    common_is_new as is_new,                                       -- 是否新用户
    model,                                                         -- 设备型号
    mid_id,                                                        -- 设备唯一标识
    operate_system,                                                -- 操作系统类型
    user_id,                                                       -- 用户ID(登录用户)
    version_code,                                                  -- 应用版本号
    page_page_id as page_id,                                       -- 当前页面ID
    page_last_page_id as last_page_id,                             -- 上一页面ID
    page_item,                                                     -- 页面项目
    page_item_type,                                                -- 页面项目类型
    page_during_time as during_time,                               -- 页面停留时长
    page_source_type as source_type,                               -- 页面来源类型
    err_error_code as error_code,                                  -- 错误代码
    error_msg,                                                     -- 错误信息描述
    date_format(FROM_UNIXTIME(ts,'GMT+8'),'yyyy-MM-dd') date_id,   -- 日期ID
    date_format(FROM_UNIXTIME(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') error_time -- 错误发生时间
from
    (
        -- 日志数据子查询：提取错误日志相关数据
        select
            id,                        -- 日志ID
            k1,                        -- 分区字段
            common_ar area_code,       -- 地区编码
            common_ba brand,           -- 设备品牌
            common_ch channel,         -- 访问渠道
            common_is_new,             -- 是否新用户
            common_md model,           -- 设备型号
            common_mid mid_id,         -- 设备ID
            common_os operate_system,  -- 操作系统
            common_uid user_id,        -- 用户ID
            common_vc version_code,    -- 应用版本号
            page_during_time,          -- 页面停留时长
            page_item,                 -- 页面项目
            page_item_type,            -- 页面项目类型
            page_last_page_id,         -- 上一页面ID
            page_page_id,              -- 当前页面ID
            page_source_type,          -- 页面来源类型
            json_extract(e1,'$.ts') ts, -- 从JSON中提取时间戳
            err_error_code,            -- 错误代码
            err_msg error_msg          -- 错误信息
        from ods.ods_log_full
                 lateral view explode_json_array_json(actions) tmp1 as e1 -- 解析JSON数组
        where k1 = date('${pdate}')    -- 按日期分区筛选
          and err_error_code is not null -- 只筛选包含错误代码的记录
    )log
        join
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
 * 1. 错误日志提取:
 *    - 从ods_log_full表中提取包含错误代码(err_error_code非空)的记录
 *    - 使用lateral view explode_json_array_json处理JSON数组，提取错误事件
 *    - 保留错误发生时的上下文信息，便于故障排查和问题定位
 *    
 * 2. 时间处理:
 *    - 将Unix时间戳转换为可读时间格式
 *    - 使用FROM_UNIXTIME函数并指定时区为GMT+8(东八区)
 *    - 生成date_id字段(yyyy-MM-dd格式)用于按天分析错误分布
 *    - 生成error_time字段(yyyy-MM-dd HH:mm:ss格式)用于精确的时间点分析
 *
 * 3. 错误信息管理:
 *    - 保留原始错误代码(error_code)和错误信息(error_msg)
 *    - 这些信息是排查应用问题的关键数据
 *    - 结合设备和页面上下文，有助于重现和解决问题
 *
 * 4. 关联省份维度:
 *    - 通过area_code关联省份表，获取规范化的province_id
 *    - 使用内连接(join)而非左连接，确保地理位置信息的完整性
 *    - 地理位置信息有助于分析错误的地域分布特征
 *
 * 5. 数据应用场景:
 *    - 应用稳定性监控：评估应用的稳定性和可靠性
 *    - 错误趋势分析：分析错误发生的频率和时间分布
 *    - 版本质量评估：比较不同版本的错误率和错误类型
 *    - 故障排查：为开发团队提供详细的错误上下文信息
 *    - 用户体验优化：识别影响用户体验的常见错误
 *    - 地域化问题分析：发现特定地区用户面临的独特问题
 *
 * 6. 关键指标构建:
 *    - 错误率：特定时间段内的错误数量/总访问量
 *    - 影响用户数：受特定错误影响的唯一用户数
 *    - 错误分布：按设备类型、操作系统、应用版本等维度的错误分布
 *    - 关键页面错误率：核心业务页面的错误发生频率
 */
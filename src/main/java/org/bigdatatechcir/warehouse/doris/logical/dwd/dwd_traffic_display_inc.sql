/*
 * 文件名: dwd_traffic_display_inc.sql
 * 功能描述: 流量域曝光事务事实表 - 记录页面元素展示和曝光信息
 * 数据粒度: 元素曝光事件
 * 刷新策略: 增量加载
 * 调度周期: 每日
 * 依赖表: 
 *   - ods.ods_log_full: 应用日志全量表
 *   - ods.ods_base_province_full: 省份基础信息表
 * 目标表: dwd.dwd_traffic_display_inc
 * 主要功能: 
 *   1. 提取日志中的元素曝光相关数据
 *   2. 解析JSON格式的displays数组，获取曝光元素信息
 *   3. 关联省份信息丰富地域维度
 *   4. 为流量分析和曝光效果分析提供基础数据
 */

-- 流量域曝光事务事实表
INSERT INTO dwd.dwd_traffic_display_inc(
    id,                -- 日志记录唯一标识
    k1,                -- 数据日期分区
    province_id,       -- 省份ID
    brand,             -- 设备品牌
    channel,           -- 应用渠道
    is_new,            -- 是否新用户(1-新用户,0-老用户)
    model,             -- 设备型号
    mid_id,            -- 设备唯一标识
    operate_system,    -- 操作系统
    user_id,           -- 用户ID
    version_code,      -- 应用版本号
    during_time,       -- 页面停留时间
    page_item,         -- 页面项目(如商品ID等)
    page_item_type,    -- 页面项目类型(如商品、品类等)
    last_page_id,      -- 上一页面ID
    page_id,           -- 当前页面ID
    source_type,       -- 页面来源类型
    date_id,           -- 日期ID
    display_time,      -- 曝光时间
    display_type,      -- 曝光类型(如商品、广告、推荐等)
    display_item,      -- 曝光元素(如商品ID、广告ID等)
    display_item_type, -- 曝光元素类型
    display_order,     -- 曝光元素顺序(排序位置)
    display_pos_id     -- 曝光位置ID(页面中的位置标识)
)
select
    id,                                                             -- 日志ID
    k1,                                                             -- 分区字段
    province_id,                                                    -- 省份ID(关联自省份表)
    brand,                                                          -- 设备品牌
    channel,                                                        -- 访问渠道
    common_is_new,                                                  -- 是否新用户
    model,                                                          -- 设备型号
    mid_id,                                                         -- 设备唯一标识
    operate_system,                                                 -- 操作系统类型
    user_id,                                                        -- 用户ID(登录用户)
    version_code,                                                   -- 应用版本号
    page_during_time,                                               -- 页面停留时长
    page_item,                                                      -- 页面项目
    page_item_type,                                                 -- 页面项目类型
    page_last_page_id,                                              -- 上一页面ID
    page_page_id,                                                   -- 当前页面ID
    page_source_type,                                               -- 页面来源类型
    date_format(FROM_UNIXTIME(ts,'GMT+8'),'yyyy-MM-dd') date_id,    -- 日期ID
    date_format(FROM_UNIXTIME(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') display_time, -- 曝光时间
    display_type,                                                   -- 曝光类型
    item,                                                           -- 曝光元素
    item_type,                                                      -- 曝光元素类型
    `order`,                                                        -- 曝光元素顺序
    pos_id                                                          -- 曝光位置ID
from
    (
        -- 日志数据子查询：提取曝光相关数据
        select
            id,                                     -- 日志ID
            k1,                                     -- 分区字段
            common_ar area_code,                    -- 地区编码
            common_ba brand,                        -- 设备品牌
            common_ch channel,                      -- 访问渠道
            common_is_new,                          -- 是否新用户
            common_md model,                        -- 设备型号
            common_mid mid_id,                      -- 设备ID
            common_os operate_system,               -- 操作系统
            common_uid user_id,                     -- 用户ID
            common_vc version_code,                 -- 应用版本号
            page_during_time,                       -- 页面停留时长
            page_item,                              -- 页面项目
            page_item_type,                         -- 页面项目类型
            page_last_page_id,                      -- 上一页面ID
            page_page_id,                           -- 当前页面ID
            page_source_type,                       -- 页面来源类型
            json_extract(e1,'$.display_type') display_type, -- 从JSON中提取曝光类型
            json_extract(e1,'$.item') item,                 -- 从JSON中提取曝光元素
            json_extract(e1,'$.item_type') item_type,       -- 从JSON中提取元素类型
            json_extract(e1,'$.order') `order`,             -- 从JSON中提取排序位置
            json_extract(e1,'$.pos_id') pos_id,             -- 从JSON中提取曝光位置ID
            ts                                      -- 时间戳
        from ods.ods_log_full
        lateral view explode_json_array_json(displays) tmp1 as e1 -- 解析JSON数组中的曝光信息
        where json_extract(e1,'$.display_type') is not null       -- 只筛选有曝光类型的记录
        and k1=date('${pdate}')                                   -- 按分区日期筛选
    )log
    left join
    (
        -- 省份维度子查询：获取省份信息
        select
            id province_id,                         -- 省份ID
            area_code                               -- 地区编码
        from ods.ods_base_province_full
    )bp
    on log.area_code=bp.area_code;                  -- 通过地区编码关联省份信息

/*
 * 设计说明:
 * 1. JSON数据处理:
 *    - 使用lateral view explode_json_array_json函数处理日志中的displays数组
 *    - 通过json_extract提取JSON对象中的特定字段(display_type, item, item_type等)
 *    - 这种设计允许从单条日志中提取多个曝光事件，提高数据颗粒度
 *    
 * 2. 曝光数据特点:
 *    - display_type标识曝光内容的类型，如商品、广告、推荐等
 *    - display_item表示曝光的具体对象ID，如商品ID、广告ID等
 *    - display_order记录曝光元素在列表中的排序位置，用于分析位置对点击率的影响
 *    - display_pos_id标识页面中的曝光位置，用于区分不同区域的曝光效果
 *
 * 3. 时间处理:
 *    - 将曝光事件的时间戳转换为标准时间格式
 *    - 使用FROM_UNIXTIME函数并指定时区为GMT+8(东八区)
 *    - 生成date_id字段用于按天分析，display_time字段用于精确时间分析
 *
 * 4. 页面上下文保留:
 *    - 保留曝光发生时的页面上下文信息(page_id, page_item等)
 *    - 通过page_id可追踪曝光发生的页面，便于分析不同页面的曝光效果
 *    - 页面上下文信息有助于理解用户浏览路径与曝光的关系
 *
 * 5. 数据应用场景:
 *    - 曝光点击转化分析：计算不同元素的曝光-点击转化率(CTR)
 *    - 位置效果分析：评估不同位置的曝光效果差异
 *    - 推荐系统评估：分析推荐内容的曝光和点击情况
 *    - 广告效果分析：评估广告的展示效果和用户反应
 *    - 商品曝光分析：分析不同商品获得的曝光机会和效果
 *
 * 6. 曝光类型说明:
 *    - 常见的display_type值及其含义:
 *      > product: 商品曝光
 *      > ad: 广告曝光
 *      > recommend: 推荐内容曝光
 *      > banner: 横幅广告曝光
 *      > category: 品类曝光
 *    - 通过分析不同类型曝光的效果，可优化内容展示策略
 *
 * 7. 与动作事实表的关联:
 *    - 曝光事实表与动作事实表(dwd_traffic_action_inc)紧密关联
 *    - 通过关联这两张表，可分析用户看到内容(曝光)后的行为反应(动作)
 *    - 典型分析路径：元素曝光→用户点击→最终转化
 *    - 可通过曝光元素ID与动作对象ID进行关联，追踪完整的用户交互路径
 */
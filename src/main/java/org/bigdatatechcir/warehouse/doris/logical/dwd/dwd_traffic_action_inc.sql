/*
 * 文件名: dwd_traffic_action_inc.sql
 * 功能描述: 流量域动作事务事实表 - 记录用户在页面上的交互行为
 * 数据粒度: 用户交互动作事件
 * 刷新策略: 增量加载
 * 调度周期: 每日
 * 依赖表: 
 *   - ods.ods_log_full: 应用日志全量表
 *   - ods.ods_base_province_full: 省份基础信息表
 * 目标表: dwd.dwd_traffic_action_inc
 * 主要功能: 
 *   1. 提取用户页面交互动作相关日志数据
 *   2. 解析JSON格式的动作数据，提取动作ID、交互对象和类型
 *   3. 关联省份信息丰富地域维度
 *   4. 为用户行为分析和交互路径分析提供基础数据
 */

-- 流量域动作事务事实表
INSERT INTO dwd.dwd_traffic_action_inc(
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
    action_id,         -- 动作ID(如点击、滑动、输入等)
    action_item,       -- 动作对象(如按钮ID、商品ID等)
    action_item_type,  -- 动作对象类型
    date_id,           -- 日期ID
    action_time        -- 动作发生时间
)
select
    id,                                                            -- 日志ID
    k1,                                                            -- 分区字段
    province_id,                                                   -- 省份ID(关联自省份表)
    brand,                                                         -- 设备品牌
    channel,                                                       -- 访问渠道
    common_is_new,                                                 -- 是否新用户
    model,                                                         -- 设备型号
    mid_id,                                                        -- 设备唯一标识
    operate_system,                                                -- 操作系统类型
    user_id,                                                       -- 用户ID(登录用户)
    version_code,                                                  -- 应用版本号
    page_during_time,                                              -- 页面停留时长
    page_item,                                                     -- 页面项目
    page_item_type,                                                -- 页面项目类型
    page_last_page_id,                                             -- 上一页面ID
    page_page_id,                                                  -- 当前页面ID
    page_source_type,                                              -- 页面来源类型
    action_id,                                                     -- 动作ID
    action_item,                                                   -- 动作对象
    action_item_type,                                              -- 动作对象类型
    date_format(FROM_UNIXTIME(ts,'GMT+8'),'yyyy-MM-dd') date_id,   -- 日期ID
    date_format(FROM_UNIXTIME(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') action_time -- 动作发生时间
from
    (
        -- 日志数据子查询：提取用户动作相关数据
        select
            id,                                  -- 日志ID
            k1,                                  -- 分区字段
            common_ar area_code,                 -- 地区编码
            common_ba brand,                     -- 设备品牌
            common_ch channel,                   -- 访问渠道
            common_is_new,                       -- 是否新用户
            common_md model,                     -- 设备型号
            common_mid mid_id,                   -- 设备ID
            common_os operate_system,            -- 操作系统
            common_uid user_id,                  -- 用户ID
            common_vc version_code,              -- 应用版本号
            page_during_time,                    -- 页面停留时长
            page_item page_item,                 -- 页面项目
            page_item_type page_item_type,       -- 页面项目类型
            page_last_page_id,                   -- 上一页面ID
            page_page_id,                        -- 当前页面ID
            page_source_type,                    -- 页面来源类型
            json_extract(e1,'$.action_id') action_id,      -- 从JSON中提取动作ID
            json_extract(e1,'$.item') action_item,         -- 从JSON中提取动作对象
            json_extract(e1,'$.item_type') action_item_type, -- 从JSON中提取动作对象类型
            json_extract(e1,'$.ts') ts                     -- 从JSON中提取时间戳
        from ods.ods_log_full
        lateral view explode_json_array_json(actions) tmp1 as e1  -- 解析JSON数组中的动作信息
        where json_extract(e1,'$.action_id') is not null          -- 只筛选有动作ID的记录
        and k1=date('${pdate}')                                   -- 按分区日期筛选
    )log
    left join
    (
        -- 省份维度子查询：获取省份信息
        select
            id province_id,                      -- 省份ID
            area_code                            -- 地区编码
        from ods.ods_base_province_full
    )bp
    on log.area_code=bp.area_code;               -- 通过地区编码关联省份信息

/*
 * 设计说明:
 * 1. JSON数据处理:
 *    - 使用lateral view explode_json_array_json函数处理日志中的actions数组
 *    - 通过json_extract提取JSON对象中的特定字段(action_id, item, item_type, ts)
 *    - 这种设计允许从单条日志中提取多个动作事件，提高数据颗粒度
 *    
 * 2. 动作数据识别:
 *    - 通过action_id字段识别不同类型的用户动作，如点击、滑动、输入等
 *    - 使用action_item记录动作的具体对象，如按钮ID、商品ID等
 *    - action_item_type标识动作对象的类型，区分不同交互元素
 *
 * 3. 时间处理:
 *    - 将动作事件的时间戳转换为标准时间格式
 *    - 使用FROM_UNIXTIME函数并指定时区为GMT+8(东八区)
 *    - 生成date_id字段用于按天分析，action_time字段用于精确时间分析
 *
 * 4. 页面上下文保留:
 *    - 保留动作发生时的页面上下文信息(page_id, page_item等)
 *    - 这种设计使分析人员能够理解动作发生的完整环境
 *    - 便于构建用户在特定页面上的行为序列
 *
 * 5. 数据应用场景:
 *    - 用户行为分析：识别用户在应用内的主要交互行为模式
 *    - 界面优化：分析不同UI元素的交互频率和效果
 *    - 转化漏斗分析：追踪用户从浏览到交互再到转化的完整路径
 *    - 用户体验评估：分析用户交互行为的流畅度和效率
 *    - A/B测试分析：比较不同版本设计中用户动作的差异
 *
 * 6. 动作类型说明:
 *    - 常见的action_id值及其含义:
 *      > click: 点击行为
 *      > scroll: 滚动行为
 *      > input: 输入行为
 *      > select: 选择行为
 *      > add_cart: 加入购物车
 *      > favorite: 收藏
 *      > share: 分享
 *    - 通过分析这些动作的频率和顺序，可以构建完整的用户行为模型
 *
 * 7. 与其他事实表的关系:
 *    - 与页面浏览事实表(dwd_traffic_page_view_inc)共同构成用户行为分析基础
 *    - 页面浏览表记录整体访问情况，动作表记录具体交互细节
 *    - 可以通过日志ID、设备ID和时间戳关联这些表，构建完整的用户行为链路
 */
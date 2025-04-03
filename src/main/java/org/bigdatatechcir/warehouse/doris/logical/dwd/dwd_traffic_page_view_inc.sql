/*
 * 文件名: dwd_traffic_page_view_inc.sql
 * 功能描述: 流量域页面浏览事务事实表 - 记录用户页面浏览行为
 * 数据粒度: 页面浏览事件
 * 刷新策略: 增量处理
 * 调度周期: 每日
 * 依赖表: 
 *   - ods.ods_log_full: 日志全量数据
 *   - ods.ods_base_province_full: 省份基础数据
 * 目标表: dwd.dwd_traffic_page_view_inc
 * 主要功能: 
 *   1. 提取页面浏览相关日志数据
 *   2. 转换时间戳为可读时间格式
 *   3. 关联省份信息丰富地域维度
 *   4. 为页面流量分析提供基础数据
 *   5. 添加会话起始点标记，支持会话分析
 */

-- 流量域页面浏览事务事实表
INSERT INTO dwd.dwd_traffic_page_view_inc(
    id,               -- 日志记录ID
    k1,               -- 数据日期分区
    province_id,      -- 省份ID
    brand,            -- 设备品牌
    channel,          -- 渠道
    is_new,           -- 是否新用户
    model,            -- 设备型号
    mid_id,           -- 设备ID
    operate_system,   -- 操作系统
    user_id,          -- 用户ID
    version_code,     -- 应用版本号
    page_item,        -- 页面项目(如商品ID、品类ID等)
    page_item_type,   -- 页面项目类型(如商品、品类、活动等)
    last_page_id,     -- 上一页面ID
    page_id,          -- 当前页面ID
    source_type,      -- 来源类型(1-系统页面，2-外部链接，3-推荐，4-广告等)
    date_id,          -- 日期ID
    view_time,        -- 浏览时间
    during_time       -- 页面停留时间(毫秒)
)
select
    id,                                                           -- 日志ID
    k1,                                                           -- 分区字段
    province_id,                                                  -- 省份ID(关联自省份表)
    brand,                                                        -- 设备品牌
    channel,                                                      -- 访问渠道
    is_new,                                                       -- 是否新用户(1-新用户,0-老用户)
    model,                                                        -- 设备型号
    mid_id,                                                       -- 设备唯一标识
    operate_system,                                               -- 操作系统类型
    user_id,                                                      -- 用户ID(登录用户)
    version_code,                                                 -- 应用版本号
    page_item,                                                    -- 页面项目标识
    page_item_type,                                               -- 页面项目类型
    page_last_page_id,                                            -- 上一页面ID
    page_page_id,                                                 -- 当前页面ID
    page_source_type,                                             -- 页面来源类型
    date_format(FROM_UNIXTIME(ts,'GMT+8'),'yyyy-MM-dd') date_id,  -- 将时间戳转换为日期ID
    date_format(FROM_UNIXTIME(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') view_time, -- 将时间戳转换为完整时间
    page_during_time                                              -- 页面停留时长(毫秒)
from
    (
        -- 日志数据子查询：提取页面浏览相关数据
        select
            id,                        -- 日志ID
            k1,                        -- 分区字段
            common_ar area_code,       -- 地区编码
            common_ba brand,           -- 设备品牌
            common_ch channel,         -- 访问渠道
            common_is_new is_new,      -- 是否新用户
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
            ts,                        -- 时间戳(秒)
            if(page_last_page_id is null,ts,null) session_start_point -- 会话起始点标记
        from ods.ods_log_full
        where page_during_time is not null -- 筛选有页面停留时间的记录，即页面浏览事件
        and k1=date('${pdate}')       -- 按分区日期筛选
    ) log
    left join
    (
        -- 省份维度子查询：获取省份信息
        select
            id province_id,            -- 省份ID
            area_code                  -- 地区编码
        from ods.ods_base_province_full
    ) bp
    on log.area_code = bp.area_code;   -- 通过地区编码关联省份信息

/*
 * 设计说明:
 * 1. 日志数据处理:
 *    - 从ods_log_full表中提取页面浏览相关事件数据
 *    - 使用page_during_time is not null条件筛选有效的页面浏览记录
 *    - 保留原始字段命名，便于理解数据来源
 *    
 * 2. 时间处理:
 *    - 将Unix时间戳(秒)转换为可读时间格式
 *    - 使用FROM_UNIXTIME函数并指定时区为GMT+8(东八区)
 *    - 生成date_id字段(yyyy-MM-dd格式)用于按天分析
 *    - 生成view_time字段(yyyy-MM-dd HH:mm:ss格式)用于精确时间分析
 *
 * 3. 会话识别设计:
 *    - 添加session_start_point字段标记会话起始点
 *    - 当page_last_page_id为null时，表示这是用户的首次页面访问
 *    - 此标记可用于后续会话分析和路径分析
 *    - 会话分析通常需要：
 *      > 会话边界识别：首次访问页面为会话开始
 *      > 会话持续时间：会话内所有页面停留时间总和
 *      > 会话页面深度：会话内访问的页面数量
 *      > 会话退出率：特定页面作为会话最后一个页面的比例
 *
 * 4. 关联省份维度:
 *    - 通过area_code关联省份表，获取规范化的province_id
 *    - 使用left join确保不会因为省份关联失败而丢失页面浏览记录
 *
 * 5. 页面来源类型说明:
 *    - page_source_type字段解释:
 *      > 1: 系统内部页面跳转
 *      > 2: 外部链接/二维码扫描
 *      > 3: 推荐系统引导
 *      > 4: 广告点击
 *      > 5: 搜索结果
 *      > 6: 社交媒体分享
 *    - 来源类型分析可评估不同渠道引流效果
 *
 * 6. 页面流量指标计算:
 *    - 基于此表可计算的关键指标:
 *      > PV(页面浏览量): 页面被浏览的总次数
 *      > UV(独立访客数): 不同设备/用户访问页面的数量
 *      > 平均停留时间: 页面停留时间的平均值
 *      > 跳出率: 只访问一个页面就离开的会话比例
 *      > 转化路径: 用户从入口到目标页面的路径分析
 *      > 页面热度: 页面的访问频次和访问时长综合评估
 *
 * 7. 数据应用场景:
 *    - 页面流量分析：评估各页面访问量、停留时间等指标
 *    - 用户行为路径：分析用户在应用内的浏览路径和跳转模式
 *    - 地域分布分析：结合省份信息分析不同地区的用户行为差异
 *    - 版本比较：分析不同应用版本的用户体验变化
 *    - 渠道效果评估：分析不同渠道带来的流量质量
 *    - A/B测试分析：比较不同页面设计对用户行为的影响
 */
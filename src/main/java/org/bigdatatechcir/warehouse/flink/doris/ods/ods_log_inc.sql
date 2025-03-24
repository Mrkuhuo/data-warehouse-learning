-- ==================== Flink 作业配置参数 ====================
-- 设置检查点间隔为10秒，定期保存作业状态以实现容错
SET 'execution.checkpointing.interval' = '10s';
-- 设置状态的生存时间为100天（以毫秒为单位）
SET 'table.exec.state.ttl'= '8640000';
-- 启用微批处理模式，提高吞吐量
SET 'table.exec.mini-batch.enabled' = 'true';
-- 设置微批处理的延迟时间，每60秒触发一次微批处理
SET 'table.exec.mini-batch.allow-latency' = '60s';
-- 设置每个微批次的最大记录数为10000条
SET 'table.exec.mini-batch.size' = '10000';
-- 设置本地时区为亚洲/上海
SET 'table.local-time-zone' = 'Asia/Shanghai';
-- 设置当遇到非空字段为空时的处理策略为丢弃该记录
SET 'table.exec.sink.not-null-enforcer'='DROP';

-- ==================== Kafka 数据源表定义 ====================
CREATE TABLE kafka_source (
    -- Kafka 元数据字段
    kafka_partition INT METADATA FROM 'partition', -- Kafka分区号
    kafka_offset INT METADATA FROM 'offset',       -- Kafka偏移量
    kafka_timestamp TIMESTAMP(3) METADATA FROM 'timestamp', -- 消息时间戳
    
    -- 通用信息字段，包含设备和用户相关信息
    `common` ROW<
        ar STRING,         -- 地区
        ba STRING,         -- 品牌
        ch STRING,         -- 渠道
        is_new STRING,     -- 是否新用户
        md STRING,         -- 设备型号
        mid STRING,        -- 设备ID
        os STRING,         -- 操作系统
        uid STRING,        -- 用户ID
        vc STRING          -- 版本号
    > NULL,
    
    -- APP启动相关信息
    `start`  ROW<
        entry STRING,           -- 入口
        loading_time STRING,    -- 加载时间
        open_ad_id STRING,      -- 开屏广告ID
        open_ad_ms STRING,      -- 开屏广告展示时间(毫秒)
        open_ad_skip_ms STRING  -- 开屏广告跳过时间(毫秒)
    > NULL,
    
    -- 页面浏览相关信息
    `page`   ROW<
        during_time BIGINT,     -- 页面停留时间
        item STRING,            -- 目标ID
        item_type STRING,       -- 目标类型
        last_page_id STRING,    -- 上一页ID
        page_id STRING,         -- 页面ID
        source_type STRING      -- 来源类型
    > NULL,
    
    -- 用户行为数据
    `actions` STRING,           -- 用户动作JSON字符串
    `displays` STRING,          -- 曝光数据JSON字符串
    
    -- 错误信息
    `err` ROW<
        error_code BIGINT,      -- 错误代码
        msg STRING              -- 错误信息
    >,
    
    -- 时间戳
    `ts` BIGINT                 -- 数据生成时间戳
) WITH (
    -- Kafka连接器配置
    'connector' = 'kafka',                        -- 使用Kafka连接器
    'topic' = 'ods_log_full',                     -- Kafka主题名
    'properties.bootstrap.servers' = '192.168.241.128:9092', -- Kafka服务器地址
    'properties.group.id' = 'ods_log_full',        -- 消费者组ID
    -- 'scan.startup.mode' = 'group-offsets',     -- 从上次消费位置开始读取(已注释)
    'scan.startup.mode' = 'earliest-offset',       -- 从最早的偏移量开始读取
    'properties.enable.auto.commit'='true',        -- 启用自动提交偏移量
    'properties.auto.commit.interval.ms'='5000',   -- 自动提交间隔为5秒
    'format' = 'json',                             -- 消息格式为JSON
    'json.ignore-parse-errors' = 'true',           -- 忽略JSON解析错误
    'json.fail-on-missing-field' = 'false'         -- 字段缺失时不失败
);

-- ==================== 目标表定义(Doris/MySQL) ====================
CREATE TABLE IF NOT EXISTS ods_log_full(
    -- 主键
    `id`                            STRING,        -- 主键ID
    `k1`                            DATE,          -- 日期分区键
    
    -- 通用信息字段
    `common_ar`                     STRING,        -- 地区
    `common_ba`                     STRING,        -- 品牌
    `common_ch`                     STRING,        -- 渠道
    `common_is_new`                 STRING,        -- 是否新用户
    `common_md`                     STRING,        -- 设备型号
    `common_mid`                    STRING,        -- 设备ID
    `common_os`                     STRING,        -- 操作系统
    `common_uid`                    STRING,        -- 用户ID
    `common_vc`                     STRING,        -- 版本号
    
    -- 启动信息字段
    `start_entry`                   STRING,        -- 启动入口
    `start_loading_time`            STRING,        -- 加载时间
    `start_open_ad_id`              STRING,        -- 开屏广告ID
    `start_open_ad_ms`              STRING,        -- 开屏广告展示时间
    `start_open_ad_skip_ms`         STRING,        -- 开屏广告跳过时间
    
    -- 页面信息字段
    `page_during_time`              BIGINT,        -- 页面停留时间
    `page_item`                     STRING,        -- 目标ID
    `page_item_type`                STRING,        -- 目标类型
    `page_last_page_id`             STRING,        -- 上一页ID
    `page_page_id`                  STRING,        -- 页面ID
    `page_source_type`              STRING,        -- 来源类型
    
    -- 行为数据
    `actions`                       STRING COMMENT '动作信息',    -- 用户行为JSON
    `displays`                      STRING COMMENT '曝光信息',    -- 曝光数据JSON
    
    -- 错误信息
    `err_error_code`                BIGINT,        -- 错误代码
    `err_msg`                       STRING,        -- 错误信息
    
    -- 时间信息
    `ts`                            BIGINT  COMMENT '时间戳'      -- 数据生成时间戳
) WITH (
    -- JDBC连接器配置，指向Doris数据库
    'connector' = 'jdbc',                           -- 使用JDBC连接器
    'url' = 'jdbc:mysql://192.168.241.128:9030/ods', -- Doris的JDBC连接URL
    'table-name' = 'ods_log_full',                  -- 目标表名
    'username' = 'root',                            -- 数据库用户名
    'password' = ''                                 -- 数据库密码
);

-- ==================== 数据转换插入语句 ====================
-- 将Kafka数据插入到Doris表中，转换字段结构
insert into ods_log_full(
    `id`, `k1`, `common_ar`,`common_ba`,`common_ch`,`common_is_new`,`common_md`,`common_mid`,`common_os`,`common_uid`,`common_vc`,`start_entry`,`start_loading_time`,`start_open_ad_id`,`start_open_ad_ms`,`start_open_ad_skip_ms`,`page_during_time`,`page_item`,`page_item_type`,`page_last_page_id`,`page_page_id`,`page_source_type`,`actions`,`displays`,`err_error_code`,`err_msg`,`ts`
)
select
    -- 生成唯一ID，组合Kafka元数据
    CONCAT(cast(kafka_partition as string), cast(kafka_offset as string), cast(kafka_timestamp as string)) as id,
    current_date,  -- 使用当前日期作为分区键
    
    -- 提取common结构体中的字段
    `common`.ar,
    `common`.ba,
    `common`.ch,
    `common`.is_new,
    `common`.md,
    `common`.mid,
    `common`.os,
    `common`.uid,
    `common`.vc,
    
    -- 提取start结构体中的字段
    `start`.entry,
    `start`.loading_time,
    `start`.open_ad_id,
    `start`.open_ad_ms,
    `start`.open_ad_skip_ms,
    
    -- 提取page结构体中的字段
    `page`.during_time,
    `page`.item,
    `page`.item_type,
    `page`.last_page_id,
    `page`.page_id,
    `page`.source_type,
    
    -- 直接传递JSON字符串
    `actions`,
    `displays`,
    
    -- 提取err结构体中的字段
    `err`.error_code,
    `err`.msg,
    
    -- 时间戳
    `ts`
from kafka_source;
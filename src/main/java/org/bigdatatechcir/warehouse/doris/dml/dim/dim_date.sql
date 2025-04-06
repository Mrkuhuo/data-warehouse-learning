-- ===============================================================================
-- 日期维度表(dim_date)
-- 功能描述：构建标准化的日期维度信息，支持多种时间粒度和日期属性的分析
-- 更新策略：一次性生成，定期扩展
-- 主键策略：日期
-- 分区策略：不分区
-- 应用场景：支持时间维度分析、季节性分析、同环比计算等多种时间相关分析
-- ===============================================================================

-- DROP TABLE IF EXISTS dim.dim_date;
CREATE TABLE dim.dim_date
(
    `date_id`            DATE COMMENT '日期ID，格式YYYY-MM-DD，主键',
    `year`               SMALLINT COMMENT '年份，如2023',
    `month`              TINYINT COMMENT '月份，1-12',
    `day`                TINYINT COMMENT '日，1-31',
    `quarter`            TINYINT COMMENT '季度，1-4',
    `week_of_year`       TINYINT COMMENT '一年中的第几周，1-53',
    `week_of_month`      TINYINT COMMENT '一月中的第几周，1-5',
    `day_of_week`        TINYINT COMMENT '周几，1-7，1代表周一',
    `day_of_year`        SMALLINT COMMENT '一年中的第几天，1-366',
    `is_weekend`         BOOLEAN COMMENT '是否周末，true表示周末',
    `is_holiday`         BOOLEAN COMMENT '是否法定节假日，true表示节假日',
    `holiday_name`       STRING COMMENT '节假日名称，如"春节"、"国庆节"等',
    `fiscal_year`        SMALLINT COMMENT '财年，公司财务年度',
    `fiscal_quarter`     TINYINT COMMENT '财季，公司财务季度1-4',
    `date_string`        STRING COMMENT '日期字符串，格式YYYYMMDD，用于关联'
)
    ENGINE=OLAP
UNIQUE KEY(`date_id`) -- 使用日期作为主键
COMMENT '日期维度表 - 支持多维度时间分析'
DISTRIBUTED BY HASH(`date_id`) -- 按日期哈希分布
PROPERTIES
(
    "replication_allocation" = "tag.location.default: 1", -- 默认副本分配策略
    "storage_format" = "V2" -- 使用V2存储格式，提升性能
);

-- 初始化日期维度表数据(通常不在此处执行，而是通过单独的数据脚本生成)
-- 下面示例初始化代码仅供参考，实际生产中应使用更完善的日期生成程序
/*
INSERT INTO dim.dim_date 
WITH date_range AS (
    SELECT DISTINCT date_add('2020-01-01', s.i) AS d
    FROM (
        SELECT c.i + b.i * 10 + a.i * 100 + a2.i * 1000 AS i
        FROM (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) a(i),
             (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) a2(i),
             (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) b(i),
             (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) c(i)
    ) s
    WHERE date_add('2020-01-01', s.i) <= '2030-12-31'
)
SELECT 
    d AS date_id,
    YEAR(d) AS year,
    MONTH(d) AS month,
    DAYOFMONTH(d) AS day,
    QUARTER(d) AS quarter,
    WEEKOFYEAR(d) AS week_of_year,
    CEILING(DAYOFMONTH(d)/7.0) AS week_of_month,
    DAYOFWEEK(d) AS day_of_week,
    DAYOFYEAR(d) AS day_of_year,
    CASE WHEN DAYOFWEEK(d) IN (6,7) THEN true ELSE false END AS is_weekend,
    false AS is_holiday, -- 节假日需要单独维护
    NULL AS holiday_name,
    YEAR(d) AS fiscal_year, -- 假设财年与日历年一致
    QUARTER(d) AS fiscal_quarter,
    DATE_FORMAT(d, '%Y%m%d') AS date_string
FROM date_range;
*/
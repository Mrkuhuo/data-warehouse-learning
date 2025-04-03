/*
 * 文件名: dwd_user_info_full.sql
 * 功能描述: 用户域用户基础信息全量表 - 清洗和标准化用户基本信息
 * 数据粒度: 用户
 * 刷新策略: 全量刷新
 * 调度周期: 每日
 * 依赖表: 
 *   - ods.ods_user_info_full: 用户基本信息原始数据
 * 目标表: dwd.dwd_user_info_full
 * 主要功能: 
 *   1. 提取和规范化用户基础属性信息
 *   2. 为用户维度建模提供基础数据源
 *   3. 为其他业务过程分析提供用户维度数据
 */

-- 用户域用户基础信息全量表
INSERT INTO dwd.dwd_user_info_full(
    id,           -- 唯一标识
    k1,           -- 数据日期，用于分区
    user_id,      -- 用户ID(冗余字段，与id相同)
    login_name,   -- 登录名
    nick_name,    -- 昵称
    name,         -- 真实姓名
    phone_num,    -- 手机号码
    email,        -- 电子邮箱
    gender,       -- 性别
    birthday,     -- 出生日期
    user_level,   -- 用户等级
    create_time,  -- 创建时间
    operate_time, -- 操作时间
    status        -- 用户状态
)
select
    id,                    -- 主键ID
    k1,                    -- 分区字段
    id as user_id,         -- 用户ID(冗余设计，便于关联查询)
    login_name,            -- 登录名
    nick_name,             -- 昵称
    name,                  -- 真实姓名
    phone_num,             -- 手机号码
    email,                 -- 电子邮箱
    gender,                -- 性别
    birthday,              -- 出生日期
    user_level,            -- 用户等级
    create_time,           -- 用户创建时间
    operate_time,          -- 最后操作时间
    status                 -- 用户状态标识
from ods.ods_user_info_full
where k1=date('${pdate}'); -- 按分区日期过滤，只处理当天数据

/*
 * 设计说明:
 * 1. 用户主题设计:
 *    - 用户是整个业务的核心主题之一
 *    - 本表保留了用户的所有基础属性，便于用户分析
 *    
 * 2. 冗余字段设计:
 *    - 设置user_id字段与id字段保持一致
 *    - 这种设计可以保证字段含义更明确，提高可读性
 *    - 方便其他表直接与user_id字段关联
 *
 * 3. 全量表特点:
 *    - 使用k1=date('${pdate}')条件过滤，每天加载全量数据
 *    - 每次运行会覆盖前一天的全量数据
 *
 * 4. 数据应用场景:
 *    - 用户画像分析：了解用户的基本属性特征
 *    - 用户分群：基于性别、年龄等维度划分用户群体
 *    - 营销策略：针对不同用户群体制定差异化营销策略
 */ 
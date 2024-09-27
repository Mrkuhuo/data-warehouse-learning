--创建源表datagen_source
CREATE TABLE datagen_source
(
    id BIGINT,
    name STRING
)
    WITH ( 'connector' = 'datagen');
--创建结果表blackhole_sink
CREATE TABLE blackhole_sink
(
    id BIGINT,
    name STRING
)
    WITH ( 'connector' = 'blackhole');
--将源表数据插入到结果表
INSERT INTO blackhole_sink
SELECT id,
       name
from datagen_source;
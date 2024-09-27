SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
set 'execution.savepoint.ignore-unclaimed-state' = 'true';

CREATE TABLE show_log_table (
    log_id BIGINT,
    show_params STRING,
    row_time AS CURRENT_TIMESTAMP,
    WATERMARK FOR row_time AS row_time
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1',
    'fields.show_params.length' = '1',
    'fields.log_id.min' = '1',
    'fields.log_id.max' = '10'
    );

CREATE TABLE click_log_table (
    log_id BIGINT,
    click_params STRING,
    row_time AS CURRENT_TIMESTAMP,
    WATERMARK FOR row_time AS row_time
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1',
    'fields.click_params.length' = '1',
    'fields.log_id.min' = '1',
    'fields.log_id.max' = '10'
    );

CREATE TABLE sink_table (
    s_id BIGINT,
    s_params STRING,
    c_id BIGINT,
    c_params STRING
) WITH (
    'connector' = 'print'
    );


INSERT INTO sink_table
SELECT
    show_log_table.log_id as s_id,
    show_log_table.show_params as s_params,
    click_log_table.log_id as c_id,
    click_log_table.click_params as c_params
FROM show_log_table INNER JOIN click_log_table
    ON show_log_table.log_id = click_log_table.log_id
    AND show_log_table.row_time BETWEEN click_log_table.row_time-INTERVAL'4'HOUR AND click_log_table.row_time;
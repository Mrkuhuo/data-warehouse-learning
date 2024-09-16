package org.bigdatatechcir.learn_flink.part11_flink_join;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class IntervalLeftJoinDemo {
    public static void main(String[] args) throws Exception
    {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        tableEnv.executeSql(
                "CREATE TABLE show_log (\n" +
                        "    log_id BIGINT,\n" +
                        "    show_params STRING,\n" +
                        "    row_time AS CURRENT_TIMESTAMP,\n" +
                        "    WATERMARK FOR row_time AS row_time\n" +
                        ") WITH (\n" +
                        "  'connector' = 'datagen',\n" +
                        "  'rows-per-second' = '1',\n" +
                        "  'fields.show_params.length' = '1',\n" +
                        "  'fields.log_id.min' = '1',\n" +
                        "  'fields.log_id.max' = '10'\n" +
                        ")");

        tableEnv.executeSql(
                "CREATE TABLE click_log (\n" +
                        "    log_id BIGINT,\n" +
                        "    click_params STRING,\n" +
                        "    row_time AS CURRENT_TIMESTAMP,\n" +
                        "    WATERMARK FOR row_time AS row_time\n" +
                        ")\n" +
                        "WITH (\n" +
                        "  'connector' = 'datagen',\n" +
                        "  'rows-per-second' = '1',\n" +
                        "  'fields.click_params.length' = '1',\n" +
                        "  'fields.log_id.min' = '1',\n" +
                        "  'fields.log_id.max' = '10'\n" +
                        ")");

        tableEnv.executeSql(
                "CREATE TABLE sink_table (\n" +
                        "    s_id BIGINT,\n" +
                        "    s_params STRING,\n" +
                        "    c_id BIGINT,\n" +
                        "    c_params STRING\n" +
                        ") WITH (\n" +
                        "  'connector' = 'print'\n" +
                        ")");


        tableEnv.executeSql("INSERT INTO sink_table\n" +
                "SELECT\n" +
                "    show_log.log_id as s_id,\n" +
                "    show_log.show_params as s_params,\n" +
                "    click_log.log_id as c_id,\n" +
                "    click_log.click_params as c_params\n" +
                "FROM show_log LEFT JOIN click_log ON show_log.log_id = click_log.log_id\n" +
                "AND show_log.row_time BETWEEN click_log.row_time - INTERVAL '5' SECOND AND click_log.row_time + INTERVAL '5' SECOND");

    }
}

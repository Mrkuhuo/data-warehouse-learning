package org.bigdatatechcir.learn_flink.part11_flink_join;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class RegularInnerJoinDemo {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        tableEnv.executeSql(
                "CREATE TABLE show_log_table (" +
                        "`log_id` BIGINT," +
                        "`show_params` STRING" +
                        ") WITH (" +
                        "'connector' = 'datagen'," +
                        "'rows-per-second' = '2'," +
                        "'fields.show_params.length' = '1'," +
                        "'fields.log_id.min' = '1'," +
                        "'fields.log_id.max' = '100'" +
                        ")");

        tableEnv.executeSql(
                "CREATE TABLE click_log_table (" +
                        "`log_id` BIGINT," +
                        "`click_params` STRING" +
                        ")" +
                        "WITH (" +
                        "'connector' = 'datagen'," +
                        "'rows-per-second' = '2'," +
                        "'fields.click_params.length' = '1'," +
                        "'fields.log_id.min' = '1'," +
                        "'fields.log_id.max' = '10'" +
                        ");");

        tableEnv.executeSql(
                "CREATE TABLE sink_table (" +
                        "`s_id` BIGINT," +
                        "`s_params` STRING," +
                        "`c_id` BIGINT," +
                        "`c_params` STRING" +
                        ") WITH (" +
                        "'connector' = 'print'" +
                        ");");

        // INNER JOIN
        tableEnv.executeSql("INSERT INTO sink_table SELECT show_log_table.`log_id` as s_id, show_log_table.`show_params` as s_params, click_log_table.`log_id` as c_id, click_log_table.`click_params` as c_params FROM show_log_table INNER JOIN click_log_table ON show_log_table.`log_id` = click_log_table.`log_id`;");
       }
}

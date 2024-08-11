package org.bigdatatechcir.learn_flink.part1_flink_apis;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import java.time.LocalDate;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.lit;
import static org.apache.flink.table.api.Expressions.range;
import static org.apache.flink.table.api.Expressions.withColumns;
public class TableApiDemo {
    public static void main(String[] args) throws Exception {

        // 初始化Flink的运行环境，设置为批处理模式
        final EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inBatchMode().build();
        final TableEnvironment env = TableEnvironment.create(settings);

        // 创建一个包含客户信息的表，数据是预定义的几行Row
        final Table rawCustomers =
                env.fromValues(
                        Row.of(
                                "Guillermo Smith",
                                LocalDate.parse("1992-12-12"),
                                "4081 Valley Road",
                                "08540",
                                "New Jersey",
                                "m",
                                true,
                                0,
                                78,
                                3),
                        Row.of(
                                "Valeria Mendoza",
                                LocalDate.parse("1970-03-28"),
                                "1239  Rainbow Road",
                                "90017",
                                "Los Angeles",
                                "f",
                                true,
                                9,
                                39,
                                0),
                        Row.of(
                                "Leann Holloway",
                                LocalDate.parse("1989-05-21"),
                                "2359 New Street",
                                "97401",
                                "Eugene",
                                null,
                                true,
                                null,
                                null,
                                null),
                        Row.of(
                                "Brandy Sanders",
                                LocalDate.parse("1956-05-26"),
                                "4891 Walkers-Ridge-Way",
                                "73119",
                                "Oklahoma City",
                                "m",
                                false,
                                9,
                                39,
                                0),
                        Row.of(
                                "John Turner",
                                LocalDate.parse("1982-10-02"),
                                "2359 New Street",
                                "60605",
                                "Chicago",
                                "m",
                                true,
                                12,
                                39,
                                0),
                        Row.of(
                                "Ellen Ortega",
                                LocalDate.parse("1985-06-18"),
                                "2448 Rodney STreet",
                                "85023",
                                "Phoenix",
                                "f",
                                true,
                                0,
                                78,
                                3));

        // 从原始客户表中选择需要的列，形成一个新的表
        final Table truncatedCustomers = rawCustomers.select(withColumns(range(1, 7)));

        // 为新表指定列名
        final Table namedCustomers =
                truncatedCustomers.as(
                        "name",
                        "date_of_birth",
                        "street",
                        "zip_code",
                        "city",
                        "gender",
                        "has_newsletter");

        // 注册临时视图，以便通过SQL查询操作
        env.createTemporaryView("customers", namedCustomers);
        final Table youngCustomers =
                env.from("customers")
                        .filter($("gender").isNotNull())
                        .filter($("has_newsletter").isEqual(true))
                        .filter($("date_of_birth").isGreaterOrEqual(LocalDate.parse("1980-01-01")))
                        .select(
                                $("name").upperCase().as("姓名"),
                                $("date_of_birth").as("出生日期"),
                                call(AddressNormalizer.class, $("street"), $("zip_code"), $("city"))
                                        .as("家庭住址"));

        youngCustomers.execute().print();
    }

    public static class AddressNormalizer extends ScalarFunction {

        public String eval(String street, String zipCode, String city) {
            return normalize(street) + ", " + normalize(zipCode) + ", " + normalize(city);
        }

        private String normalize(String s) {
            return s.toUpperCase().replaceAll("\\W", " ").replaceAll("\\s+", " ").trim();
        }
    }
}

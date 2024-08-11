package org.bigdatatechcir.learn_flink.part1_flink_apis;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

public class StreamSQLDemo {
    public static void main(String[] args) throws Exception {

        // 初始化流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 初始化流表环境
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 从集合创建订单流A
        final DataStream<Order> orderA =
                env.fromCollection(
                        Arrays.asList(
                                new Order(1L, "beer", 3),
                                new Order(1L, "diaper", 4),
                                new Order(3L, "rubber", 2)));
        // 从集合创建订单流B
        final DataStream<Order> orderB =
                env.fromCollection(
                        Arrays.asList(
                                new Order(2L, "pen", 3),
                                new Order(2L, "rubber", 3),
                                new Order(4L, "beer", 1)));

        // 将订单流A转换为表
        final Table tableA = tableEnv.fromDataStream(orderA);
        // 将订单流B注册为临时视图
        tableEnv.createTemporaryView("TableB", orderB);

        // 执行SQL查询，合并表A和表B的数据，筛选出符合条件的订单
        final Table result =
                tableEnv.sqlQuery(
                        "SELECT * FROM "
                                + tableA
                                + " WHERE amount > 2 UNION ALL "
                                + "SELECT * FROM TableB WHERE amount < 2");

        // 将查询结果转换回订单流并打印
        tableEnv.toDataStream(result, Order.class).print();

        // 启动执行环境
        env.execute();
    }

    /** Simple POJO. */
    public static class Order {
        public Long user;
        public String product;
        public int amount;

        public Order() {}

        public Order(Long user, String product, int amount) {
            this.user = user;
            this.product = product;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return "Order{"
                    + "user="
                    + user
                    + ", product='"
                    + product
                    + '\''
                    + ", amount="
                    + amount
                    + '}';
        }
    }
}

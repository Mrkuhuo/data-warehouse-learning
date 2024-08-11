package org.bigdatatechcir.learn_flink.part3_flink_time;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class IngestionTimeDemo {

    public static void main(String[] args) throws Exception {
        // 创建流处理环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度为1，方便调试
        env.setParallelism(1);

        // 定义输入数据
        DataStream<Event> events = env.fromElements(
                new Event("user1", "/page1"),
                new Event("user2", "/page2"),
                new Event("user3", "/page3"),
                new Event("user3", "/page3")
        );

        // 使用Ingestion time作为时间特征
        DataStream<Event> timestampedEvents = events.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis())
        );

        // 定义窗口逻辑
        DataStream<Tuple3<String, Integer, Long>> result = timestampedEvents
                .keyBy(event -> event.user)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(1)))
                .process(new ProcessWindowFunction<Event, Tuple3<String, Integer, Long>, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Event> elements, Collector<Tuple3<String, Integer, Long>> out) throws Exception {
                        int count = 0;
                        for (Event event : elements) {
                            count++;
                        }
                        // 获取窗口的结束时间
                        long end = context.window().getEnd();
                        out.collect(new Tuple3<>(s, count, end));
                    }
                });

        // 输出结果
        result.print();

        // 执行任务
        env.execute("Ingestion Time Demo");
    }

    public static class Event {
        public String user;
        public String page;

        public Event() {}

        public Event(String user, String page) {
            this.user = user;
            this.page = page;
        }
    }
}

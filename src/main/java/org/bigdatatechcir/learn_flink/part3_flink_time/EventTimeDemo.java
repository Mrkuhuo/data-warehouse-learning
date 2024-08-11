package org.bigdatatechcir.learn_flink.part3_flink_time;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class EventTimeDemo {
    public static void main(String[] args) throws Exception {
        // 创建流处理环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 定义输入数据
        DataStream<Event> events = env.fromElements(
                new Event("user1", "/page1", 1543218199000L),
                new Event("user2", "/page2", 1543218200000L),
                new Event("user3", "/page3", 1543218202000L),
                new Event("user3", "/page3", 1543218202001L)
        );

        // 提取时间戳并设置 watermark
        DataStream<Event> withTimestampsAndWatermarks = events
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((event, recordTimestamp) -> event.timestamp));

        // 定义窗口逻辑
        DataStream<Tuple2<String, Integer>> result = withTimestampsAndWatermarks
                .keyBy(event -> event.user)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(5)))
                .process(new ProcessWindowFunction<Event, Tuple2<String, Integer>, String, TimeWindow>(){
                    @Override
                    public void process(String s, ProcessWindowFunction<Event, Tuple2<String, Integer>, String, TimeWindow>.Context context, Iterable<Event> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        int count = 0;
                        for (Event event : iterable) {
                            count++;
                        }
                        collector.collect(new Tuple2<>(s, count));
                    }
                });

        // 输出结果
        result.print();

        // 执行任务
        env.execute("Event Time Demo");
    }

    public static class Event {
        public String user;
        public String page;
        public long timestamp;

        public Event() {}

        public Event(String user, String page, long timestamp) {
            this.user = user;
            this.page = page;
            this.timestamp = timestamp;
        }
    }
}

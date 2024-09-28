package org.bigdatatechcir.learn_dinky;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class GlobalWindowDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //设置WebUI绑定的本地端口
        conf.setString(RestOptions.BIND_PORT,"8081");
        //使用配置

        //final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置时间语义为 Event Time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 使用 DataGeneratorSource 生成数据
        DataStream<String> text = env.addSource(new org.bigdatatechcir.learn_flink.part4_flink_window.GlobalWindowDemo.DataGeneratorSourceFunction());

        // 解析数据并提取时间戳
        DataStream<Tuple3<String, Integer, Long>> tuplesWithTimestamp = text
                .map(new MapFunction<String, Tuple3<String, Integer, Long>>() {
                    @Override
                    public Tuple3<String, Integer, Long> map(String value) {
                        String[] words = value.split(",");
                        return new Tuple3<>(words[0], Integer.parseInt(words[1]), Long.parseLong(words[2]));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.LONG));

        // 设置 Watermark 策略
        DataStream<Tuple3<String, Integer, Long>> withWatermarks = tuplesWithTimestamp
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Integer, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((element, recordTimestamp) -> element.f2));

        // 应用全局窗口
        DataStream<Tuple2<String, Integer>> globalWindowedStream = withWatermarks
                .keyBy(value -> value.f0)
                .windowAll(GlobalWindows.create())
                .trigger(CountTrigger.of(10)) // 当每个键接收到10个元素时，触发计算
                .process(new org.bigdatatechcir.learn_flink.part4_flink_window.GlobalWindowDemo.CountProcessFunction());

        // 输出结果
        globalWindowedStream.print();

        // 执行任务
        env.execute("Global Windowing Demo");
    }

    public static class CountProcessFunction extends ProcessAllWindowFunction<Tuple3<String, Integer, Long>, Tuple2<String, Integer>, GlobalWindow> {
        @Override
        public void process(ProcessAllWindowFunction<Tuple3<String, Integer, Long>, Tuple2<String, Integer>, GlobalWindow>.Context context, Iterable<Tuple3<String, Integer, Long>> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {
            Map<String, Integer> counts = new HashMap<>();

            for (Tuple3<String, Integer, Long> element : iterable) {
                String key = element.f0;
                counts.put(key, counts.getOrDefault(key, 0) + 1);
            }

            for (Map.Entry<String, Integer> entry : counts.entrySet()) {
                collector.collect(new Tuple2<>(entry.getKey(), entry.getValue()));
            }
        }
    }

    public static class DataGeneratorSourceFunction extends RichParallelSourceFunction<String> {
        private volatile boolean isRunning = true;
        private Random random = new Random();

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (isRunning) {
                int randomNum = random.nextInt(5) + 1;
                long timestamp = System.currentTimeMillis();
                ctx.collectWithTimestamp("key" + randomNum + "," + 1 + "," + timestamp, timestamp);
                System.out.println("Generated data: " + "key" + randomNum + "," + 1 + "," + timestamp + " at " + getCurrentFormattedDateTime(timestamp));
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

        private String getCurrentFormattedDateTime(long timestamp) {
            ZonedDateTime generateDataDateTime = Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault());
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
            return generateDataDateTime.format(formatter);
        }
    }
}

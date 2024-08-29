package org.bigdatatechcir.learn_flink.part5_flink_watermark;

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
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

public class WithIdLenessDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString(RestOptions.BIND_PORT, "8081");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> text = env.addSource(new RichParallelSourceFunction<String>() {
            private volatile boolean running = true;
            private volatile long count = 0; // 计数器用于跟踪已生成的数据条数
            private final Random random = new Random();

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (running) {
                    int randomNum = random.nextInt(5) + 1;
                    long timestamp = System.currentTimeMillis();
                    ctx.collectWithTimestamp("key" + randomNum + "," + 1 + "," + timestamp, timestamp);

                    if (++count % 200 == 0) { // 每200条数据发送一次Watermark
                        ctx.emitWatermark(new Watermark(timestamp));
                        System.out.println("Manual Watermark emitted: " + timestamp);
                    }

                    ZonedDateTime generateDataDateTime = Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault());
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
                    String formattedGenerateDataDateTime = generateDataDateTime.format(formatter);
                    System.out.println("Generated data: " + "key" + randomNum + "," + 1 + "," + timestamp + " at " + formattedGenerateDataDateTime);
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });

        DataStream<Tuple3<String, Integer, Long>> tuplesWithTimestamp = text.map(new MapFunction<String, Tuple3<String, Integer, Long>>() {
            @Override
            public Tuple3<String, Integer, Long> map(String value) {
                String[] words = value.split(",");
                return new Tuple3<>(words[0], Integer.parseInt(words[1]), Long.parseLong(words[2]));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT, Types.LONG));

        // 设置 Watermark 策略
        DataStream<Tuple3<String, Integer, Long>> withWatermarks = tuplesWithTimestamp.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, Integer, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        //处理空闲数据源
                        .withIdleness(Duration.ofSeconds(15))
                        .withTimestampAssigner((element, recordTimestamp) -> element.f2)
        );

        // 窗口逻辑
        DataStream<Tuple2<String, Integer>> keyedStream = withWatermarks
                .keyBy(value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Tuple3<String, Integer, Long>, Tuple2<String, Integer>, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple3<String, Integer, Long>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
                        int count = 0;
                        for (Tuple3<String, Integer, Long> element : elements) {
                            count++;
                        }

                        long start = context.window().getStart();
                        long end = context.window().getEnd();

                        ZonedDateTime startDateTime = Instant.ofEpochMilli(start).atZone(ZoneId.systemDefault());
                        ZonedDateTime endDateTime = Instant.ofEpochMilli(end).atZone(ZoneId.systemDefault());

                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
                        String formattedStart = startDateTime.format(formatter);
                        String formattedEnd = endDateTime.format(formatter);

                        System.out.println("Tumbling Window [start " + formattedStart + ", end " + formattedEnd + ") for key " + s);

                        // 输出窗口结束时的Watermark
                        long windowEndWatermark = context.currentWatermark();
                        ZonedDateTime windowEndDateTime = Instant.ofEpochMilli(windowEndWatermark).atZone(ZoneId.systemDefault());
                        String formattedWindowEndWatermark = windowEndDateTime.format(formatter);
                        System.out.println("Watermark at the end of window: " + formattedWindowEndWatermark);

                        out.collect(new Tuple2<>(s, count));
                    }
                });

        // 输出结果
        keyedStream.print();

        // 执行任务
        env.execute("With Id Leness Demo");
    }
}

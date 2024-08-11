package org.bigdatatechcir.learn_flink.part4_flink_window;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

public class SlidingWindowDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //设置WebUI绑定的本地端口
        conf.setString(RestOptions.BIND_PORT,"8081");
        //使用配置
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);

        // 设置时间语义为 Event Time
        env.setStreamTimeCharacteristic(org.apache.flink.streaming.api.TimeCharacteristic.EventTime);

        // 使用 DataGeneratorSource 生成数据
        DataStream<String> text = env.addSource(new RichParallelSourceFunction<String>() {
            private boolean running = true;

            private Random random = new Random();

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                int count = 1;
                while (running) {
                    int randomNum = random.nextInt(5) + 1; // 生成1到5之间的随机数
                    long timestamp = System.currentTimeMillis(); // 获取当前时间作为时间戳
                    ctx.collect("key" + randomNum + "," + 1 + "," + timestamp);
                    ZonedDateTime generateDataDateTime = Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault());
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
                    String formattedGenerateDataDateTime = generateDataDateTime.format(formatter);
                    System.out.println("Generated data: " + "key" + randomNum + "," + count + "," + timestamp + " at " + formattedGenerateDataDateTime);
                    Thread.sleep(1000); // 每秒生成一条数据
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });

        // 解析数据并提取时间戳
        DataStream<Tuple3<String, Integer, Long>> tuplesWithTimestamp = text.map(new MapFunction<String, Tuple3<String, Integer, Long>>() {
            @Override
            public Tuple3<String, Integer, Long> map(String value) {
                String[] words = value.split(",");
                return new Tuple3<>(words[0], Integer.parseInt(words[1]), Long.parseLong(words[2]));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT, Types.LONG));

        // 设置 Watermark 策略
        DataStream<Tuple3<String, Integer, Long>> withWatermarks = tuplesWithTimestamp.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Integer, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((element, recordTimestamp) -> element.f2));

        DataStream<Tuple2<String, Integer>> keyedStream = withWatermarks
                .keyBy(value -> value.f0)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2))) // 这里设置窗口大小为7秒，滑动间隔为2秒
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

                        System.out.println("Sliding Window [ start " + formattedStart + ", end " + formattedEnd + ", slide " + 2 + " s ] for key " + s);

                        out.collect(new Tuple2<>(s, count));
                    }
                });

        // 输出结果
        keyedStream.print();

        // 执行任务
        env.execute("Sliding Window Demo");
    }
}

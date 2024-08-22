package org.bigdatatechcir.learn_flink.part5_flink_watermark;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
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
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;
public class PunctuatedWatermarkDemo {
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

        // 设置 Punctuated Watermark 策略
        DataStream<Tuple3<String, Integer, Long>> withWatermarks = tuplesWithTimestamp.assignTimestampsAndWatermarks(new WatermarkStrategy<Tuple3<String, Integer, Long>>() {
            @Override
            public WatermarkGenerator<Tuple3<String, Integer, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new PunctuatedWatermarkGenerator();
            }

            @Override
            public TimestampAssigner<Tuple3<String, Integer, Long>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                return new PunctuatedWatermarkGenerator();
            }
        });

        // 窗口逻辑
        DataStream<Tuple2<String, Integer>> keyedStream = withWatermarks
                .keyBy(value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Tuple3<String, Integer, Long>, Tuple2<String, Integer>, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple3<String, Integer, Long>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
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

                        System.out.println("Tumbling Window [start " + formattedStart + ", end " + formattedEnd + ") for key " + key);

                        // 输出窗口结束时的Watermark
                        long windowEndWatermark = context.currentWatermark();
                        ZonedDateTime windowEndDateTime = Instant.ofEpochMilli(windowEndWatermark).atZone(ZoneId.systemDefault());
                        String formattedWindowEndWatermark = windowEndDateTime.format(formatter);
                        System.out.println("Watermark at the end of window: " + formattedWindowEndWatermark);

                        out.collect(new Tuple2<>(key, count));
                    }
                });

        // 输出结果
        keyedStream.print();

        // 执行任务
        env.execute("Punctuated Watermark Demo");
    }

    private static class PunctuatedWatermarkGenerator
            implements WatermarkGenerator<Tuple3<String, Integer, Long>>, TimestampAssigner<Tuple3<String, Integer, Long>> {
        private long maxTimestamp = Long.MIN_VALUE;

        @Override
        public long extractTimestamp(Tuple3<String, Integer, Long> element, long recordTimestamp) {
            // 提前事件时间要先判断时间戳字段是否为-1
            if (element.f2 != -1) {
                return element.f2;
            } else {
                // 如果为空，返回上一次的事件时间
                return recordTimestamp > 0 ? recordTimestamp : 0;
            }
        }

        @Override
        public void onEvent(Tuple3<String, Integer, Long> event, long eventTimestamp, WatermarkOutput output) {
            maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
            if (event.f0.equals("key2")) {
                System.out.println("Event: " + event.f0 + "," + event.f1 + "," + event.f2);
                ZonedDateTime watermarkDateTime = Instant.ofEpochMilli(maxTimestamp).atZone(ZoneId.systemDefault());
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
                String formattedWatermark = watermarkDateTime.format(formatter);
                System.out.println("Emitting Watermark: " + formattedWatermark);
                output.emitWatermark(new Watermark(event.f2));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // nothing
        }
    }
}

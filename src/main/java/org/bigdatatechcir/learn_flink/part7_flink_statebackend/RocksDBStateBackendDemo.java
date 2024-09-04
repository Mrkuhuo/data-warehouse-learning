package org.bigdatatechcir.learn_flink.part7_flink_statebackend;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
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

public class RocksDBStateBackendDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString(RestOptions.BIND_PORT, "8081");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        //env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage("file:///D:/flink-state"));
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("file:///D:/flink-state"));
        // 开启 checkpoint，并设置间隔 ms
        env.enableCheckpointing(1000);
        // 模式 Exactly-Once、At-Least-Once
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 两个 checkpoint 之间最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        // 超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 同时执行的 checkpoint 数量（比如上一个还没执行完，下一个已经触发开始了）
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 当用户取消了作业后，是否保留远程存储上的Checkpoint数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 设置自动生成Watermark的间隔时间
        // env.getConfig().setAutoWatermarkInterval(100000);
        env.setParallelism(1);

        //env.setRestartStrategy(RestartStrategies.noRestart());

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

                    // 如果生成的是 key2，则在一个新线程中处理延迟
                    if (randomNum == 2) {
                        new Thread(() -> {
                            try {
                                int delay = random.nextInt(10) + 1; // 随机数范围从1到10
                                Thread.sleep(delay * 1000); // 增加1到10秒的延迟
                                ctx.collectWithTimestamp("key" + randomNum + "," + 1 + "," + timestamp, timestamp);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }).start();
                    } else {
                        ctx.collectWithTimestamp("key" + randomNum + "," + 1 + "," + timestamp, timestamp);
                    }

                    if (++count % 200 == 0) {
                        ctx.emitWatermark(new Watermark(timestamp));
                        System.out.println("Manual Watermark emitted: " + timestamp);
                    }

                    ZonedDateTime generateDataDateTime = Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault());
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
                    String formattedGenerateDataDateTime = generateDataDateTime.format(formatter);
                    System.out.println("Generated data: " + "key" + randomNum + "," + 1 + "," + timestamp + " at " + formattedGenerateDataDateTime);

                    Thread.sleep(1000); // 每次循环后等待1秒
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
                WatermarkStrategy.<Tuple3<String, Integer, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((element, recordTimestamp) -> element.f2)
        );

        // 窗口逻辑
        DataStream<Tuple2<String, Integer>> keyedStream = withWatermarks
                .keyBy(value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Tuple3<String, Integer, Long>, Tuple2<String, Integer>, String, TimeWindow>() {
                    private ValueState<Integer> countState;

                    @Override
                    public void open(Configuration parameters) {
                        countState = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("count", Types.INT)
                        );
                    }

                    @Override
                    public void process(String s, Context context, Iterable<Tuple3<String, Integer, Long>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
                        int count = 0;
                        for (Tuple3<String, Integer, Long> element : elements) {
                            count++;
                        }

                        // 在更新状态之前获取当前状态值
                        Integer previousCount = countState.value();

                        // 更新状态
                        countState.update(count);

                        // 获取更新后的状态值
                        Integer updatedCount = countState.value();

                        // 打印状态更新前后的值
                        if (previousCount != null) {
                            System.out.println("Key: " + s + ", Previous Count: " + previousCount + ", Updated Count: " + updatedCount);
                        } else {
                            System.out.println("Key: " + s + ", Initial Count: " + updatedCount);
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

                        // 读取状态并输出
                        Integer currentCount = countState.value();
                        if (currentCount != null) {
                            out.collect(new Tuple2<>(s, currentCount));
                        }
                    }
                });

        // 输出结果
        keyedStream.print();

        // 执行任务
        env.execute("Periodic Watermark Demo");
    }
}
package org.bigdatatechcir.learn_flink.part6_flink_state;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.time.Duration;
import java.util.Random;

public class KeyedStateDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString(RestOptions.BIND_PORT, "8081");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        // 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/flink-state");

        // 开启 checkpoint，并设置间隔
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> text = env.addSource(new RichParallelSourceFunction<String>() {
            private volatile boolean running = true;
            private volatile long count = 0;
            private final Random random = new Random();

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (running) {
                    int randomNum = random.nextInt(5) + 1;
                    long timestamp = System.currentTimeMillis();

                    if (randomNum == 2) {
                        new Thread(() -> {
                            try {
                                int delay = random.nextInt(10) + 1;
                                Thread.sleep(delay * 1000);
                                ctx.collectWithTimestamp("key" + randomNum + "," + 1 + "," + timestamp, timestamp);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }).start();
                    } else {
                        ctx.collectWithTimestamp("key" + randomNum + "," + 1 + "," + timestamp, timestamp);
                    }

                    if (++count % 200 == 0) {
                        ctx.emitWatermark(new org.apache.flink.streaming.api.watermark.Watermark(timestamp));
                        System.out.println("Manual Watermark emitted: " + timestamp);
                    }

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

        DataStream<Tuple3<String, Integer, Long>> withWatermarks = tuplesWithTimestamp.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, Integer, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((element, recordTimestamp) -> element.f2)
        );

        DataStream<Tuple2<String, Integer>> keyedStream = withWatermarks
                .keyBy(value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Tuple3<String, Integer, Long>, Tuple2<String, Integer>, String, TimeWindow>() {
                    private ValueState<Integer> countState;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>(
                                "count",
                                Types.INT
                        );
                        countState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void process(String key,
                                        Context context,
                                        Iterable<Tuple3<String, Integer, Long>> elements,
                                        Collector<Tuple2<String, Integer>> out) throws IOException {
                        int count = 0;
                        for (Tuple3<String, Integer, Long> element : elements) {
                            count++;
                        }

                        Integer currentState = countState.value();
                        if (currentState == null) {
                            currentState = 0;
                        }

                        currentState += count;
                        countState.update(currentState);

                        out.collect(new Tuple2<>(key, currentState));

                        System.out.println("Key: " + key + ", Count in this window: " + count + ", Total Count so far: " + currentState);
                    }
                });

        keyedStream.print();

        env.execute("Periodic Watermark Demo");
    }
}

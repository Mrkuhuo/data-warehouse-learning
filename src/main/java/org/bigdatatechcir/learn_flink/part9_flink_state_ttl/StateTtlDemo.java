package org.bigdatatechcir.learn_flink.part9_flink_state_ttl;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.bigdatatechcir.learn_flink.part6_flink_state.OperatorStateDemo;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class StateTtlDemo {
    public static void main(String[] args) throws Exception {



        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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

        withWatermarks.addSink(new OperatorStateDemo.BufferingSink());

        env.execute("KeyedState");
    }

    static class BufferingSink implements SinkFunction<Tuple3<String, Integer, Long>>, CheckpointedFunction {

        private ListState<Tuple3<String, Integer, Long>> listState;
        private List<Tuple3<String, Integer, Long>> bufferedElements = new ArrayList<>();

        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(10))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                .cleanupIncrementally(10, true)
                .build();


        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {

            ListStateDescriptor<Tuple3<String, Integer, Long>> descriptor =
                    new ListStateDescriptor<Tuple3<String, Integer, Long>>("bufferedSinkState",
                            TypeInformation.of(new TypeHint<Tuple3<String, Integer, Long>>() {}));

            descriptor.enableTimeToLive(ttlConfig);

            listState = context.getOperatorStateStore().getListState(descriptor);

            if (context.isRestored()){
                for (Tuple3<String, Integer, Long> element : listState.get()){
                    bufferedElements.add(element);
                }
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            for (Tuple3<String, Integer, Long> element : bufferedElements){
                listState.add(element);
            }
        }


        @Override
        public void invoke(Tuple3<String, Integer, Long> value, Context context) throws Exception {

            bufferedElements.add(value);
            System.out.println("invoke>>> " + value);
            for (Tuple3<String, Integer, Long> element : bufferedElements){
                System.out.println(Thread.currentThread().getId() + " >> " + element.f0 + " : " + element.f1 + " : " + element.f2);
            }
            System.out.println("listSize  " + bufferedElements.size());
        }
    }
}

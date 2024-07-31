package org.bigdatatechcir.learn.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Random;

public class SlidingWindowingDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 使用DataGeneratorSource生成数据
        DataStream<String> text = env.addSource(new RichParallelSourceFunction<String>() {
            private boolean running = true;

            private Random random = new Random();

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                int count = 0;
                while (running) {
                    int randomNum = random.nextInt(5) + 1; // 生成1到5之间的随机数
                    ctx.collect("key" + randomNum + "," + count);
                    Thread.sleep(1000); // 每秒生成一条数据
                    count++;
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });

        DataStream<Tuple2<String, Integer>> pairs = text.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) {
                String[] words = value.split(",");
                return new Tuple2<>(words[0], Integer.parseInt(words[1]));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        DataStream<Tuple2<String, Integer>> keyedStream = pairs.keyBy(value -> value.f0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .reduce((a, b) -> new Tuple2<>(a.f0, a.f1 + b.f1));
        keyedStream.print();


        env.execute("Tumbling Window Demo");
    }
}

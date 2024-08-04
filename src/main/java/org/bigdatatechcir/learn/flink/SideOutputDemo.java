package org.bigdatatechcir.learn.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Random;

public class SideOutputDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> text = env.addSource(new RichParallelSourceFunction<String>() {
            private boolean running = true;
            private Random random = new Random();

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                int count = 0;
                while (running) {
                    int randomNum = random.nextInt(5) + 1;
                    ctx.collect("key" + randomNum + "," + count);
                    Thread.sleep(1000);
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

        OutputTag<Tuple2<String, Integer>> sideOutputTag = new OutputTag<Tuple2<String, Integer>>("side-output") {};

        SingleOutputStreamOperator<Tuple2<String, Integer>> processedPairs = pairs.process(new ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public void processElement(Tuple2<String, Integer> value,
                                       Context ctx,
                                       Collector<Tuple2<String, Integer>> out) throws Exception {
                if (Integer.parseInt(value.f0.substring(3)) % 2 == 0) {
                    ctx.output(sideOutputTag, value);
                } else {
                    out.collect(value);
                }
            }
        });

        DataStream<Tuple2<String, Integer>> mainOutput = processedPairs;
        DataStream<Tuple2<String, Integer>> sideOutput = processedPairs.getSideOutput(sideOutputTag);

        mainOutput.keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce((a, b) -> new Tuple2<>(a.f0, a.f1 + b.f1))
                .print("Main Output");

        sideOutput.print("Side Output");

        env.execute("Tumbling Window with Side Output Demo");
    }

}
